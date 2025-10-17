#!/usr/bin/env python3
"""Playtime.

Make your movie colletion resemble a visit to a Playtime video store in the 90ies.

Uses PTN to parse movie titles from directory names, cinemagoer to search IMDB, and
cinemagoerng to fetch data from IMDB.

More info at https://github.com/tykling/playtime
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import shutil
import sys
import time
from dataclasses import asdict, dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from urllib.request import Request, urlopen

import imdb
import PTN
from cinemagoerng import model as cgngmodel
from cinemagoerng import web
from enrich.logging import RichHandler
from rich_argparse import RichHelpFormatter
from typedload.exceptions import TypedloadValueError

logger = logging.getLogger("playtime")

# get version number from package metadata if possible
try:
    __version__ = version("playtime")
except PackageNotFoundError:  # pragma: no cover
    # package is not installed, get version from _version.py file if possible
    try:
        from _version import version as __version__
    except ImportError:
        # this must be a git checkout with no _version.py file, version unknown
        __version__: str = "0.0.0"  # type: ignore[no-redef]

MiB = 1024 * 1024
IMDB_ID_REGEX = "(tt[0-9]{7,10}+)"


@dataclass
class Cache:
    """Represents the movie cache."""

    def __init__(
        self,
        movies: dict[str, Movie],
        directories: dict[Path, str],
        cache_dir: Path,
        cache_filename: Path,
    ) -> None:
        """Get paths for cache file and covers."""
        logger.debug(":open_file_folder: Initialising Playtime cache...")
        if not movies:
            movies = {}
        if not directories:
            directories = {}
        self.movies = movies
        self.directories = directories
        self.cachedir = cache_dir.expanduser()
        if not self.cachedir.exists():
            self.cachedir.mkdir()
        self.cachefile = self.cachedir / cache_filename
        self.coverdir = self.cachedir / "covers"
        if not self.coverdir.exists():
            self.coverdir.mkdir()

    def get_basedirs(self) -> list[Path]:
        """Return a list of basedirs currently found in the cache."""
        return list({path.parent for path in self.directories})

    def count(self, imdb_id: str) -> int:
        """Count the number of times a movie is found in a directory the cache."""
        counter = 0
        for cache_id in self.directories.values():
            if cache_id == imdb_id:
                counter += 1
        return counter

    def read_cache_file(self) -> None:
        """Read the cache file and return a dict."""
        if not self.cachefile.exists():
            logger.warning(f"Cache file {self.cachefile} not found, starting with empty cache")
            return
        logger.debug(f":open_file_folder: Opening cache file {self.cachefile}...")
        with self.cachefile.open() as f:
            try:
                cache = json.loads(f.read())
            except json.decoder.JSONDecodeError:
                cache = {}
        self.directories = {Path(moviedir): imdb_id for moviedir, imdb_id in cache["directories"].items()}
        # turn movie dicts into Movie objects
        self.movies = {imdb_id: Movie(**values) for imdb_id, values in cache["movies"].items()}
        logger.debug(
            f":card_file_box:  Loaded cache file with {len(self.directories)} directories "
            f"with {len(self.movies)} movies from {self.cachefile}..."
        )

    def write_cache_file(self) -> None:
        """Write the cache to disk."""
        cachepath = self.cachefile.with_suffix(".newcache")
        target = cachepath.with_suffix(".cache")
        logger.debug(
            f"Writing cache file with {len(self.directories)} directories with {len(self.movies)} movies to {target}..."
        )
        with cachepath.open("w") as f:
            moviedict = {
                "directories": {str(moviedir): imdb_id for moviedir, imdb_id in self.directories.items()},
                "movies": {imdb_id: asdict(movie) for imdb_id, movie in self.movies.items()},
            }
            moviejson = json.dumps(moviedict)
            f.write(moviejson)
        # overwrite cache file with newcache
        cachepath = cachepath.replace(target)
        logger.debug(
            f"Cache file with {len(self.directories)} directories with "
            f"{len(self.movies)} movies written to {cachepath}..."
        )

    def get_category_values(self, category: str, runtime_interval: int = 15) -> set[str]:
        """Get a list of unique values for the given category."""
        things = set()
        for movie in self.movies.values():
            things.update(movie.get_category(category=category, runtime_interval=runtime_interval))
        return things


@dataclass
class Movie:
    """Represents a single movie."""

    # imdb movie ID (including "tt")
    imdb_id: str
    # primary imdb title
    title: str
    # url to cover image
    primary_image: str
    # movie production year as int
    year: int
    # list of language_codes, whatever they are?
    language_codes: list[str]
    # list of genres
    genres: list[str]
    # rating as a string
    rating: str
    # number of votes
    vote_count: int
    # duration
    runtime: int
    # optional, place on top250
    top_ranking: int | None
    # optional, place on bottom100
    bottom_ranking: int | None
    # epoch timestamp when the data was retrieved
    data_epoch: int
    # directors
    directors: list[str] | None = None
    # actors
    actors: list[str] | None = None

    @property
    def cover_filename(self) -> str:
        """Return the filename to use for the cover for this movie."""
        return f"{self.imdb_id}.jpg"

    @property
    def imdb_url(self) -> str:
        """Return the IMDB url of the movie."""
        return f"https://www.imdb.com/title/{self.imdb_id}/"

    @property
    def short(self) -> str:
        """A short string to describe the movie in log output."""
        return f"{self.title} ({self.year}) [{', '.join(self.genres)}]"

    @property
    def dirname(self) -> str:
        """Return the directory name to use for this movie. Replace os.sep to avoid pain."""
        return f"{self.title} ({self.year})".replace(os.sep, "_")

    @property
    def data_age_days(self) -> int:
        """Return the number of whole 24 hour periods since the data for this movie was fetched from IMDB."""
        now = int(time.time())
        age_secs = now - self.data_epoch
        return math.floor(age_secs / 86400)

    def get_category(self, category: str, runtime_interval: int = 15) -> list[str]:
        """Return data for the requested category as a list of strings."""
        if category == "runtime":
            lower = self.runtime // runtime_interval
            return [f"{runtime_interval * lower}-{runtime_interval * (lower + 1)} minutes"]
        value = getattr(self, category)
        if isinstance(value, list):
            # return as-is
            return value
        # return as a list with one string
        return [str(value)]

    @classmethod
    def create_from_imdb_id(cls: type[Movie], imdb_id: str) -> Movie | None:
        """Initialise a Movie object from IMDB id."""
        try:
            movie = web.get_title(f"{imdb_id}", accept_language="en")
        except TypedloadValueError:
            logger.warning(f":cross_mark: Unable to lookup IMDB id [cyan]{imdb_id}[/cyan] using cinemagoerng, skipping")
            return None
        if not isinstance(
            movie,
            cgngmodel.Movie | cgngmodel.VideoMovie | cgngmodel.TVMovie | cgngmodel.ShortMovie | cgngmodel.TVSpecial,
        ):
            logger.warning(
                f":cross_mark: imdb id [cyan]{imdb_id}[/cyan] returned a {type(movie)} "
                "which is currently unsupported by playtime"
            )
            return None
        return Movie.create_from_cinemagoerng(movie)

    @classmethod
    def create_from_cinemagoerng(cls: type[Movie], cgng_movie: cgngmodel.Movie) -> Movie:
        """Initialise a playtime Movie object from a cinemagoerng movie object."""
        return cls(
            imdb_id=cgng_movie.imdb_id,
            title=cgng_movie.title,
            primary_image=cgng_movie.primary_image,
            year=cgng_movie.year,
            language_codes=cgng_movie.language_codes,
            genres=cgng_movie.genres,
            rating=str(cgng_movie.rating),
            vote_count=cgng_movie.vote_count,
            runtime=cgng_movie.runtime,
            top_ranking=cgng_movie.top_ranking,
            bottom_ranking=cgng_movie.bottom_ranking,
            directors=[director.name for director in cgng_movie.directors],
            actors=[actor.name for actor in cgng_movie.cast[:10]],  # get the first 10 credited
            data_epoch=int(time.time()),
        )


class Playtime:
    """Main Playtime class."""

    def __init__(self, cache_directory: Path) -> None:
        """Initialise stuff."""
        cache_directory = cache_directory.expanduser()
        logger.debug(
            f":rocket: Initialising Playtime version [cyan]{__version__}[/cyan] with cachedir {cache_directory} ..."
        )

        # cinemagoer for imdb searching
        self.ia = imdb.Cinemagoer()

        # read and initialise movie info cache
        self.cache = Cache(cache_dir=cache_directory, cache_filename=Path("playtime.cache"), directories={}, movies={})
        self.cache.read_cache_file()

    def find_textfiles(self, moviedir: Path, extensions: list[str] | None = None) -> list[Path]:
        """Glob and return a list of textfiles in the directory."""
        if not extensions:
            # use default list of extensions
            extensions = ["txt", "nfo"]
        # use glob to find textfiles
        textfiles: list[Path] = []
        for ext in extensions:
            for textfile in moviedir.glob(f"*.{ext}"):
                # skip directories and other non-files
                if not textfile.is_file():
                    logger.warning(f"Skipping non-file [cyan]{moviedir}[/cyan]")
                    continue
                # skip big files
                if textfile.stat().st_size > MiB:
                    logger.warning(f"Skipping big textfile [cyan]{moviedir}[/cyan]")
                    continue
                logger.info(f"Checking file [cyan]{textfile}[/cyan] for IMDB id...")
                textfiles.append(textfile)
        return textfiles

    def find_imdb_id_in_textfiles(self, textfiles: list[Path]) -> str | None:
        """Search list of textfiles for an IMDB id, return the first found."""
        # loop over textfiles
        for textfile in textfiles:
            if not textfile.exists():
                continue
            # open files as bytes, their encoding is unknown
            with textfile.open("rb") as f:
                text = f.read()
            try:
                matches = re.findall(IMDB_ID_REGEX, text.decode("utf-8"))
            except UnicodeDecodeError:
                matches = re.findall(IMDB_ID_REGEX, text.decode("iso-8859-1"))
            if matches:
                logger.debug(f"Found IMDB id [cyan]{matches[0]}[/cyan] in file [cyan]{textfile}[/cyan]")
                return str(matches[0])
        # no luck :(
        logger.debug("No IMDB id found in any textfiles")
        return None

    def imdb_title_search(self, *, title: str) -> str | None:
        """Search IMDB for a movie title."""
        # search
        logger.info(f"Searching IMDB for title [cyan]{title}[/cyan] ...")
        try:
            search_results = self.ia.search_movie(title)
        except imdb._exceptions.IMDbDataAccessError:  # noqa: SLF001
            logger.warning(f"Unable to get search results for title [cyan]{title}[/cyan] :(")
            return None
        if not search_results:
            logger.warning(f"No search results for title [cyan]{title}[/cyan] :(")
            return None
        # return top result
        return f"tt{search_results[0].movieID}"

    def identify_movie(
        self,
        *,
        moviedir: Path,
        imdb_id: str = "",
        skip_imdb_search: bool,
        skip_textfiles: bool,
        search_confirmation: bool,
    ) -> Movie | None:
        """Identify movie from dirname and return a Movie object."""
        if not imdb_id:
            if not skip_textfiles:
                logger.debug(f"{moviedir} - trying to find IMDB id in textfile...")
                textfiles = self.find_textfiles(moviedir=moviedir)
                textfile_imdb_id = self.find_imdb_id_in_textfiles(textfiles=textfiles)
                if textfile_imdb_id:
                    logger.debug(f"{moviedir} - Found IMDB ID [cyan]{textfile_imdb_id}[/cyan] in textfile")
                    logger.info(f"{moviedir} - Looking up IMDB info for id [cyan]{textfile_imdb_id}[/cyan]...")
                    return Movie.create_from_imdb_id(textfile_imdb_id)
                logger.debug(f"{moviedir} - could not find IMDB id in textfile")

            if not skip_imdb_search:
                logger.debug(f"{moviedir} - trying to parse movie name from directory name...")
                parsed = PTN.parse(moviedir.name)
                title = parsed.get("title")
                if not title:
                    logger.warning(
                        f"{moviedir} - Failed to parse title for directory - please fix the directory name to be "
                        "more standard and retry, or just add the IMDB url of the movie in {moviedir / 'imdb.txt'}"
                    )
                    return None
                year = parsed.get("year")
                if year:
                    logger.debug(f"{moviedir} - Parsed dirname to title [cyan]{title}[/cyan] from {year}")
                    prompt = f"{title} {year}"
                else:
                    logger.debug(f"{moviedir} - Parsed dirname to title [cyan]{title}[/cyan] - unable to parse year")
                    prompt = title

                # confirm each search?
                if search_confirmation:
                    prompt = input("Search IMDB for '{prompt}' or enter new search:") or prompt
                search_imdb_id = self.imdb_title_search(title=prompt)
                if not search_imdb_id:
                    logger.debug(f"Search for '{prompt}' did not return anything useful")
                else:
                    imdb_id = search_imdb_id

            if not imdb_id:
                # no imdb id, give up
                return None

        # return Movie from cache if possible, or create new Movie object
        return self.cache.movies.get(imdb_id, Movie.create_from_imdb_id(imdb_id))

    def update_cache(  # noqa: PLR0913
        self,
        *,
        new_basedirs: list[Path],
        search_confirmation: bool = False,
        skip_textfiles: bool = False,
        skip_imdb_search: bool = False,
        skip_cache_clean: bool = False,
        update_age_days: int = 7,
    ) -> None:
        """Identify movies and update the cache of movie info."""
        # first check the filesystem and update the cache with any new movies
        self.update_cache_directories(
            new_basedirs=new_basedirs,
            search_confirmation=search_confirmation,
            skip_textfiles=skip_textfiles,
            skip_imdb_search=skip_imdb_search,
        )

        # update data
        self.update_cache_moviedata(update_age_days=update_age_days)

        # clean cache?
        if not skip_cache_clean:
            self.clean_cache()

    def update_cache_moviedata(self, update_age_days: int) -> None:
        """Get fresh data from IMDB when needed."""
        logger.debug(f"Checking data age of {len(self.cache.movies)} movies...")
        needs_updates = []
        for imdb_id, movie in self.cache.movies.items():
            if movie.data_age_days >= update_age_days:
                logger.debug(
                    f"Movie {movie.short} data age is {movie.data_age_days} days, the limit is {update_age_days} days."
                )
                needs_updates.append(imdb_id)
        if not needs_updates:
            return
        logger.info(f"Getting updated IMDB data for {len(needs_updates)} movies...")
        for imdb_id in needs_updates:
            logger.info(
                f"{needs_updates.index(imdb_id) + 1}/{len(needs_updates)} "
                f"Getting updated data for {self.cache.movies[imdb_id].short} ..."
            )
            updated_movie = Movie.create_from_imdb_id(imdb_id)
            if not updated_movie:
                logger.warning(f"Unable to get updated IMDB data for {imdb_id}, skipping")
                continue
            self.cache.movies[imdb_id] = updated_movie

    def update_cache_directories(
        self,
        *,
        new_basedirs: list[Path],
        search_confirmation: bool,
        skip_textfiles: bool,
        skip_imdb_search: bool,
    ) -> None:
        """Identify movies and update the cache of movie info."""
        # keep track of failed lookups
        fails = []
        # loop over configured basedirs
        basedirs = {moviedir.parent for moviedir in list(self.cache.directories.keys())}
        basedirs.update(new_basedirs)
        logger.debug(f"Looking for movies in basedirs: {basedirs} ...")
        for basedir in basedirs:
            # loop over directories (movies) in this basedir
            for moviedir in basedir.iterdir():
                if not moviedir.is_dir():
                    continue
                result = self.update_cache_directory(
                    moviedir=moviedir,
                    search_confirmation=search_confirmation,
                    skip_textfiles=skip_textfiles,
                    skip_imdb_search=skip_imdb_search,
                )
                if not result:
                    fails.append(moviedir)
        if fails:
            logger.warning(
                f"Done checking moviedirs. The following {len(fails)} directories could not be identified. "
                "Fix the directory names or add an IMDB url for the movie in an imdb.txt textfile inside the directory."
            )
            for fail in fails:
                logger.warning(fail)

    def update_cache_directory(
        self,
        *,
        moviedir: Path,
        search_confirmation: bool,
        skip_textfiles: bool,
        skip_imdb_search: bool,
    ) -> bool:
        """Identify movie in moviedir."""
        logger.debug(f"==== Processing directory {moviedir} ...")
        # have we seen this directory before?
        textfile_imdb_id: str | None = ""
        if moviedir in self.cache.directories:
            logger.debug(f"{moviedir} - directory is known in cache, checking imdb.txt file...")
            textfile_imdb_id = self.find_imdb_id_in_textfiles(textfiles=[moviedir / "imdb.txt"])
            if textfile_imdb_id:
                if textfile_imdb_id == self.cache.directories[moviedir]:
                    # this movie does not need reidentification, textfile matches cache
                    logger.debug(
                        f":white_check_mark: {moviedir} - Directory is known in the cache, "
                        "and imdb.txt contains the correct ID."
                    )
                    return True
                logger.info(
                    f"{moviedir} - textfile IMDB id {textfile_imdb_id} does not match "
                    f"cached IMDB id {self.cache.directories[moviedir]} - re-identifying movie..."
                )
            else:
                logger.debug(f":white_check_mark: {moviedir} Directory is known in the cache.")
                return True
        else:
            logger.info(f"{moviedir} - New movie directory found, identifying movie...")
        # pass imdb_id if one was found in a textfile
        movie = self.identify_movie(
            moviedir=moviedir,
            imdb_id=textfile_imdb_id or "",
            skip_imdb_search=skip_imdb_search,
            skip_textfiles=skip_textfiles,
            search_confirmation=search_confirmation,
        )
        if not movie:
            logger.warning(f":cross_mark: {moviedir} - Unable to get IMDB info, skipping")
            return False
        logger.info(f"{moviedir} - Got movie {movie.imdb_id}: {movie.short}")
        # update cache
        self.cache.directories[moviedir] = movie.imdb_id
        if movie.imdb_id not in self.cache.movies:
            self.cache.movies[movie.imdb_id] = movie
        # write back cache file before returning so the cache is not lost in case of a crash
        self.cache.write_cache_file()
        return True

    def clean_cache(self) -> None:
        """Clean cache."""
        logger.debug("Cleaning cache...")
        removed_directories = []
        removed_movies = []
        for directory in self.cache.directories:
            logger.debug(f"Checking if cache directory {directory} still exists on the filesystem...")
            if not directory.exists():
                logger.warning(f"Moviedir {directory} not found on the filesystem, removing from cache")
                removed_directories.append(directory)
        for imdb_id in self.cache.movies:
            logger.debug(f"Checking if movie {imdb_id} still exists on the filesystem...")
            if imdb_id not in self.cache.directories.values():
                removed_movies.append(imdb_id)
        if removed_directories:
            logger.warning(
                f"The following {len(removed_directories)} directories no longer "
                "exist on the filesystem and will now be removed from the cache:"
            )
            for removal in removed_directories:
                logger.warning(removal)
                del self.cache.directories[removal]
        if removed_movies:
            logger.warning(
                f"The following {len(removed_movies)} movies no longer exist anywhere "
                "on the filesystem and will now be removed from the cache:"
            )
            for imdb_id in removed_movies:
                logger.warning(self.cache.movies[imdb_id].short)
                del self.cache.movies[imdb_id]
        # save changes to the cache file
        self.cache.write_cache_file()

    def persist_imdb_urls(self, persist_filename: str = "imdb.txt") -> None:
        """Loop over movies in the cache and save the IMDB url for each to a textfile in the moviedir."""
        for moviedir, imdb_id in self.cache.directories.items():
            url = self.cache.movies[imdb_id].imdb_url
            txtpath = moviedir / persist_filename
            with txtpath.open("w") as f:
                f.write(f"{url}\n")
            logger.debug(f"Wrote IMDB url {url} to {txtpath}")
        logger.debug(f"Done. Wrote {len(self.cache.directories)} files with {len(self.cache.movies)} unique IMDB urls.")

    def list_moviedirs(self, *, identified_only: bool, unidentified_only: bool, duplicates_only: bool) -> None:
        """Loop over and output all dirs in the configured basedirs."""
        logger.debug(
            f"Listing movies, settings: identified_only: {identified_only}, "
            f"unidentified_only: {unidentified_only}, duplicates_only: {duplicates_only}"
        )
        # keep track of duplicates
        duplicates: dict[str, int] = {}
        # loop over basedirs
        for basedir in self.cache.get_basedirs():
            # loop over directories in this basedir
            for moviedir in basedir.iterdir():
                if not moviedir.is_dir():
                    continue
                # have we seen this directory before?
                imdb_id = self.cache.directories.get(moviedir)
                if not imdb_id:
                    if identified_only or duplicates_only:
                        # directory is not identified, can't be a duplicate when not identified, skip
                        continue
                    logger.info(f":cross_mark: UNIDENTIFIED {moviedir}")
                    continue

                # this moviedir is known in the cache
                # lazy, optimize this
                duplicates[imdb_id] = count = self.cache.count(imdb_id=imdb_id)

                # has data been downloaded for this movie?
                if imdb_id in self.cache.movies:
                    if unidentified_only:
                        # not unidentified, skip
                        continue
                    if duplicates_only and count == 1:
                        # this is not a duplicate
                        continue
                    logger.info(f":white_check_mark: OK ({count}) {moviedir}: {self.cache.movies[imdb_id].short}")
                    continue

                if not identified_only:
                    logger.info(f":grey_question: NODATA {moviedir}")

    def download_covers(self, *, save_covers_in_moviedirs: bool, force_download: bool) -> None:
        """Download covers for movies in the cache."""
        # loop over movies in cache and download missing covers to cache
        for movie in self.cache.movies.values():
            coverpath = self.cache.coverdir / movie.cover_filename
            if coverpath.exists() and not force_download:
                logger.debug(f":white_check_mark: Cover file {coverpath} already exists in cache - skipping download")
            else:
                if not movie.primary_image:
                    logger.warning(f"{movie.short} - cover URL not found, skipping")
                    continue
                logger.info(f"{movie.short} - Downloading cover to {coverpath}")
                self.download_file(movie.primary_image, coverpath)

        # all covers have been downloaded, copy or symlink them
        self.distribute_covers(save_covers_in_moviedirs=save_covers_in_moviedirs)

    def distribute_covers(self, *, save_covers_in_moviedirs: bool) -> None:
        """Loop over moviedi rs in the cache and copy or symlink cover to each."""
        for moviedir, imdb_id in self.cache.directories.items():
            movie = self.cache.movies[imdb_id]
            movie_cover = moviedir / "poster.jpg"
            cache_cover = self.cache.coverdir / movie.cover_filename
            if not cache_cover.exists():
                logger.warning(f"Cache cover file {cache_cover} not found - skipping {movie.short}")
                continue
            if movie_cover.exists():
                if movie_cover.is_symlink() and save_covers_in_moviedirs:
                    logger.debug(f":x: {movie_cover} is a symlink but it should be a real file")
                    movie_cover.unlink()
                elif movie_cover.is_symlink() and not save_covers_in_moviedirs:
                    logger.debug(
                        f":white_check_mark: The cover file {movie_cover} already exists as a symlink, skipping"
                    )
                    continue
                elif not movie_cover.is_symlink() and save_covers_in_moviedirs:
                    logger.debug(f":white_check_mark: The cover file {movie_cover} already exists as a file, skipping")
                    continue
                elif not movie_cover.is_symlink() and not save_covers_in_moviedirs:
                    logger.debug(f":x: {movie_cover} is a file but it should be a symlink")
                    movie_cover.unlink()

            # copy or symlink?
            if save_covers_in_moviedirs:
                # copy cover file
                shutil.copy(cache_cover, movie_cover)
                logger.debug(f":white_check_mark: Copied cover file {cache_cover} to {movie_cover}")
            else:
                # symlink cover
                movie_cover.symlink_to(cache_cover)
                logger.debug(f":white_check_mark: Symlinked cover file {cache_cover} to {movie_cover}")

    def download_file(self, url: str, path: Path) -> None:
        """Download a file."""
        request = Request(url)  # noqa: S310
        logger.debug(f"Downloading {url} to {path} ...")
        with urlopen(request) as response, path.open("wb") as f:  # noqa: S310
            f.write(response.read())

    def clean_category_dir(self, category_dir: Path) -> None:
        """Clean any old directories and symlinks from the categorydir."""
        # make sure the category dir exists
        category_dir.mkdir(exist_ok=True, parents=True)
        # delete any files in categorydir
        for root, dirs, files in category_dir.walk(top_down=False):
            for name in files:
                (root / name).unlink()
            for name in dirs:
                (root / name).rmdir()

    def create_symlink_dirs(
        self, *, symlink_dir: Path, categories: list[str], relative: bool = True, runtime_interval: int = 30
    ) -> None:
        """Create symlink dirs for the requested categories."""
        if not symlink_dir.exists():
            symlink_dir.mkdir()
        symlink_coverdir = symlink_dir / ".covers"
        symlink_coverdir.mkdir(exist_ok=True)
        for category in categories:
            categorydir = symlink_dir / category
            self.clean_category_dir(category_dir=categorydir)
            logger.debug(f"Creating symlinks by {category} in {categorydir}...")

            # find unique set of things in this category (years, genres, directors...)
            things = self.cache.get_category_values(category=category, runtime_interval=runtime_interval)

            # loop over things and create symlinks for each
            for thing in things:
                thingdir = self.get_thingdir(category=category, categorydir=categorydir, thing=thing)
                self.create_symlinks_for_thing(
                    thing=thing,
                    thingdir=thingdir,
                    category=category,
                    symlink_coverdir=symlink_coverdir,
                    relative=relative,
                    runtime_interval=runtime_interval,
                )
            if category in ["actors", "directors"]:
                # rename letter dirs
                for letterdir in categorydir.iterdir():
                    count = len(list(letterdir.iterdir()))
                    countdir = letterdir.parent / f"{letterdir.name} ({count} {category})"
                    logger.debug(f"Renaming {letterdir} to {countdir}")
                    letterdir.rename(countdir)

    def get_thingdir(self, *, category: str, categorydir: Path, thing: str) -> Path:
        """Return the dir for this thing (actor, director, genre...)."""
        if category in ["actors", "directors"]:
            # make a level with the first letter
            return categorydir / thing[0] / thing
        if category in ["runtime", "rankings"]:
            return categorydir / thing
        # year, genre
        return categorydir / thing

    def create_symlinks_for_thing(  # noqa: PLR0913
        self,
        *,
        thing: str,
        thingdir: Path,
        category: str,
        symlink_coverdir: Path,
        relative: bool,
        runtime_interval: int = 15,
    ) -> None:
        """Create symlinks for a thing (year, actor, genre, director, runtime)."""
        logger.debug(f"Creating dir {thingdir}")
        thingdir.mkdir(parents=True, exist_ok=True)
        # loop over moviedirs and create new symlinks for this thing
        for moviedir, imdb_id in self.cache.directories.items():
            logger.debug(f"Processing moviedir {moviedir} ...")
            movie = self.cache.movies[imdb_id]
            # does this movie need a symlink for this thing?
            if thing in self.cache.movies[imdb_id].get_category(category=category, runtime_interval=runtime_interval):
                # create directory for this movie if needed
                subdir = thingdir / movie.dirname
                subdir.mkdir(exist_ok=True)
                # handle cover for this movie
                cache_cover = self.cache.coverdir / movie.cover_filename
                symlink_cover = symlink_coverdir / movie.cover_filename
                movie_cover = subdir / "poster.jpg"
                if cache_cover.exists():
                    if not symlink_cover.exists():
                        logger.debug(f"Copying {cache_cover} to {symlink_cover}")
                        shutil.copy(cache_cover, symlink_cover)
                    if not movie_cover.exists():
                        logger.debug(f"Creating a relative symlink at {movie_cover} to {symlink_cover}")
                        movie_cover.symlink_to(symlink_cover.relative_to(subdir, walk_up=True))
                linkpath = subdir / moviedir.name
                if linkpath.is_symlink():
                    # The link to this movie already exists.
                    # This can happen when the same moviedir name
                    # exists in multiple basedirs, ignore and continue
                    continue
                if relative:
                    logger.debug(f"Creating relative symlink at {linkpath} to {moviedir}")
                    target = moviedir.relative_to(linkpath.parent, walk_up=True)
                else:
                    logger.debug(f"Creating absolute symlink at {linkpath} to {moviedir}")
                    target = moviedir
                linkpath.symlink_to(target, target_is_directory=True)
        # rename each thingdir to reflect the number of movies in it
        count = len(list(thingdir.iterdir()))
        countdir = thingdir.parent / f"{thingdir.name} ({count} movies)"
        logger.debug(f"Renaming {thingdir} to {countdir}")
        thingdir.rename(countdir)

    def create_runtime_symlinks(self, *, symlink_dir: Path, relative: bool = True) -> None:
        """Create symlink dirs for movie runtimes."""

    def cache_purge_ids(self, imdb_ids: list[str]) -> None:
        """Purge a list of IMDB ids from the cache."""
        for imdb_id in imdb_ids:
            if imdb_id in self.cache.movies:
                logger.info(f"Deleting IMDB id {imdb_id} from cache - movie {self.cache.movies[imdb_id]}")
                del self.cache.movies[imdb_id]
            else:
                logger.warning(f"IMDB id {imdb_id} not found in cache - cannot purge")
            moviedirs = []
            # find any cached directories with this ID and purge them too
            for directory, cache_id in self.cache.directories.items():
                if imdb_id == cache_id:
                    moviedirs.append(directory)
            if moviedirs:
                logger.info(f"Purging {len(moviedirs)} directories with cached IMDB id {imdb_id} from cache...")
                self.cache_purge_directories(moviedirs=moviedirs)
        self.cache.write_cache_file()

    def cache_purge_directories(self, moviedirs: list[Path]) -> None:
        """Purge one or more directories from the cache."""
        for moviedir in moviedirs:
            if moviedir not in self.cache.directories:
                logger.warning(f"Directory {moviedir} not found in cache, cannot purge.")
            else:
                logger.info(
                    f"Deleting directory {moviedir} with cached IMDB id {self.cache.directories[moviedir]} from cache"
                )
                del self.cache.directories[moviedir]
        self.cache.write_cache_file()


def get_parser() -> argparse.ArgumentParser:
    """Create an argparse monster."""
    parser = argparse.ArgumentParser(
        prog="Playtime",
        description=f"Playtime version {__version__}.",
        formatter_class=RichHelpFormatter,
    )

    ###########################################
    # global options
    parser.add_argument(
        "-c",
        "--cache-directory",
        type=Path,
        help="Cache directory path. Defaults to ~/.cache/playtime/",
        default=Path("~/.cache/playtime/"),
    )
    parser.add_argument(
        "-l",
        "--log-level",
        dest="log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level. One of DEBUG, INFO, WARNING, ERROR, CRITICAL. Defaults to INFO.",
        default="INFO",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_const",
        dest="log-level",
        const="WARNING",
        help="Quiet mode. No output at all if no errors are encountered. Equal to setting --log-level=WARNING.",
        default=argparse.SUPPRESS,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="log-level",
        const="DEBUG",
        help="Verbose/debug mode. Equal to setting --log-level=DEBUG.",
        default=argparse.SUPPRESS,
    )
    parser.add_argument(
        "-V",
        "--version",
        action="store_true",
        help="Show version and exit",
        default=argparse.SUPPRESS,
    )
    subparsers = parser.add_subparsers(dest="subparser_name", help="Playtime subcommand (required).")

    ###########################################
    # playtime update
    update_parser = subparsers.add_parser("update", help="Update Playtime movie cache with data from IMDB.")
    update_parser.add_argument(
        "basedirs",
        type=Path,
        nargs="*",
        help="Optional. Adds one or more new directories in which to look for movies. Each "
        "basedir is expected to contain a number of directories each containing exactly one movie.",
    )
    update_parser.add_argument(
        "-u",
        "--update-age-days",
        type=int,
        default=30,
        help="Number of days between updating cache data for a movie.",
    )
    update_parser.add_argument(
        "-f",
        "--force-update",
        action="store_true",
        help="Force updating the data for all movies (regardless of the age of any existing data in the cache).",
    )
    update_parser.add_argument(
        "-s",
        "--search-confirmation",
        action="store_true",
        help="Interactively confirm IMDB search terms before each search when identifying movies.",
    )
    update_parser.add_argument(
        "--skip-textfiles",
        action="store_true",
        help="Do not search textfiles in each moviedir for the IMDB ID when identifying movies",
    )
    update_parser.add_argument(
        "--skip-imdb-search",
        action="store_true",
        help="Do not search online on IMDB when identifying movies. See also --search-confirmation.",
    )
    update_parser.add_argument(
        "--skip-cache-clean",
        action="store_true",
        help="Do not clean directories and movies which no longer exist on the filesystem from the cache.",
    )

    ###########################################
    # playtime download
    download_parser = subparsers.add_parser("download", help="Download covers for movies in the cache.")
    download_parser.add_argument(
        "-f",
        "--force-download",
        action="store_true",
        help="Force download of cover even if the file already exists.",
    )
    download_parser.add_argument(
        "-s",
        "--save-covers-in-moviedirs",
        action="store_true",
        help="Copy the cover file to each moviedir instead of symlinking them from the cache cover dir.",
    )

    ###########################################
    # playtime symlink
    symlink_parser = subparsers.add_parser("symlink", help="Create directories hierachy with movie categories.")
    symlink_parser.add_argument(
        "symlinkdir",
        type=Path,
        help="Required. The directory in which to create the movie category symlinks.",
    )
    symlink_parser.add_argument(
        "-C",
        "--categories",
        nargs="+",
        default=["genres", "year", "directors", "actors", "runtime"],
        help="Movie categories to enable for 'playtime symlink'.",
    )

    ###########################################
    # playtime cache
    cache_parser = subparsers.add_parser("cache", help="Playtime cache operations.")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_subparser_name", help="Cache subcommand (required).")

    cache_purgedir_parser = cache_subparsers.add_parser(
        "purgedirs", help="Purge (remove) directory from the playtime cache"
    )
    cache_purgedir_parser.add_argument(
        "moviedir",
        type=Path,
        nargs="+",
        help="Moviedirs to purge (delete) from cache. This will force re-identification of the movies.",
    )

    cache_purgeid_parser = cache_subparsers.add_parser(
        "purgeids", help="Purge (remove) IMDB id from the playtime cache"
    )
    cache_purgeid_parser.add_argument(
        "imdb_id",
        nargs="+",
        help="IMDB IDs to purge data from cache. Include the 'tt' prefix!",
    )

    cache_persist_parser = cache_subparsers.add_parser(
        "persist", help="Persist movie identification by saving IMDB urls to textfile in each moviedir."
    )
    cache_persist_parser.add_argument(
        "-p",
        "--persist-filename",
        default="imdb.txt",
        help="Filename in which to persist IMDB urls in moviedirs.",
    )

    ###########################################
    # playtime ls
    ls_parser = subparsers.add_parser("ls", help="List all movies in configured directories.")
    ls_parser.add_argument(
        "-d",
        "--duplicates-only",
        action="store_true",
        help="Only list duplicate movies.",
    )
    ls_group = ls_parser.add_mutually_exclusive_group()
    ls_group.add_argument(
        "-i",
        "--identified-only",
        action="store_true",
        help="Only list identified movies.",
    )
    ls_group.add_argument(
        "-u",
        "--unidentified-only",
        action="store_true",
        help="Only list unidentified movies.",
    )

    return parser


def parse_args(
    mockargs: list[str] | None = None,
) -> tuple[argparse.ArgumentParser, argparse.Namespace]:
    """Create an argparse object and parse either mockargs (for testing) or sys.argv[1:] (for real)."""
    parser = get_parser()
    args = parser.parse_args(mockargs if mockargs else sys.argv[1:])
    return parser, args


def configure_logger(level: str) -> None:
    """Configure the logger."""
    # determine log format and level
    if level == "DEBUG":
        console_logformat = "%(asctime)s %(name)s.%(funcName)s():%(lineno)i:  %(message)s"
        datefmt = "%Y-%m-%d %H:%M:%S %z"
    else:
        console_logformat = "%(message)s"
        datefmt = "[%X]"
    # configure the logger
    logging.basicConfig(
        level=level,
        format=console_logformat,
        datefmt=datefmt,
        handlers=[RichHandler(markup=True)],
    )

    # also configure the root logger
    rootlogger = logging.getLogger("")
    rootlogger.setLevel(level)


def main(mockargs: list[str] | None = None) -> None:  # noqa: C901
    """Get command-line args, configure logging, and start Playtime."""
    # get argparser and parse args
    parser, args = parse_args(mockargs)

    # show version and exit?
    if "version" in args:
        print(f"Playtime version {__version__}.")  # noqa: T201
        sys.exit(0)

    # configure logger
    configure_logger(level=getattr(args, "log-level"))

    # initialise Playtime object
    pt = Playtime(cache_directory=args.cache_directory)

    if args.subparser_name == "update":
        pt.update_cache(
            new_basedirs=args.basedirs,
            search_confirmation=args.search_confirmation,
            skip_textfiles=args.skip_textfiles,
            skip_imdb_search=args.skip_imdb_search,
            skip_cache_clean=args.skip_cache_clean,
            update_age_days=args.update_age_days,
        )

    elif args.subparser_name == "persist":
        pt.persist_imdb_urls(persist_filename=args.persist_filename)

    elif args.subparser_name == "download":
        pt.download_covers(save_covers_in_moviedirs=args.save_covers_in_moviedirs, force_download=args.force_download)

    elif args.subparser_name == "symlink":
        pt.create_symlink_dirs(symlink_dir=args.symlinkdir, categories=args.categories)

    elif args.subparser_name == "cache":
        if args.cache_subparser_name == "purgedirs":
            pt.cache_purge_directories(moviedirs=args.moviedir)
        elif args.cache_subparser_name == "purgeids":
            pt.cache_purge_ids(imdb_ids=args.imdb_id)
        elif args.cache_subparser_name == "persist":
            pt.persist_imdb_urls(persist_filename=args.persist_filename)
        else:
            logger.error("Cache subcommand missing!")
            parser.print_help()

    elif args.subparser_name == "ls":
        pt.list_moviedirs(
            identified_only=args.identified_only,
            unidentified_only=args.unidentified_only,
            duplicates_only=args.duplicates_only,
        )
    else:
        logger.error("Playtime subcommand missing!")
        parser.print_help()

    logger.debug(":person_raising_hand: Playtime is over - bye!")


if __name__ == "__main__":
    main()
