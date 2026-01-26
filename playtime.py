#!/usr/bin/env python3
"""Playtime.

Make your movie colletion resemble a visit to a Playtime video store in the 90ies.

Uses PTN to parse movie titles from directory names, imdb-sqlite to fetch IMDB data,
and nanodjango for ORM goodies and webinterface.

Read more about IMDB datasets at https://developer.imdb.com/non-commercial-datasets/

More info at https://github.com/tykling/playtime
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

import django_stubs_ext
import PTN  # type: ignore[import-untyped]
from cinemagoerng import web as imdb
from cinemagoerng.model import (
    Movie,
    MusicVideo,
    ShortMovie,
    TVEpisode,
    TVMiniSeries,
    TVMovie,
    TVSeries,
    TVShortMovie,
    TVSpecial,
    VideoGame,
    VideoMovie,
)
from django.db import models
from django.utils import timezone
from enrich.logging import RichHandler
from nanodjango import Django, defer  # type: ignore[import-untyped]
from rich_argparse import RichHelpFormatter

with defer:
    from django_imdb.export_tsv import export_tsv_files
    from django_imdb.import_tsv import import_tsv_files
    from django_imdb.models import Title
    from django_imdb.pocketsearch import pocketsearch_normalise, title_search
    from django_imdb.utils import download_file

if TYPE_CHECKING:
    import datetime

logger = logging.getLogger("playtime")

django_stubs_ext.monkeypatch()

CgngObj: TypeAlias = (
    Movie
    | TVMovie
    | ShortMovie
    | TVShortMovie
    | VideoMovie
    | MusicVideo
    | VideoGame
    | TVSeries
    | TVMiniSeries
    | TVEpisode
    | TVSpecial
)

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

app = Django(
    SQLITE_DATABASE=Path("~/.cache/playtime/playtime.db").expanduser(),
    EXTRA_APPS=["django_imdb"],
)


@app.admin(list_display=["id", "title", "path"])
class Directory(models.Model):
    """Represents a directory containing a single movie or episode."""

    title: models.ForeignKey[Title, Title] = models.ForeignKey(
        "django_imdb.Title", on_delete=models.PROTECT, related_name="directories"
    )
    path: models.CharField[str, str] = models.CharField(unique=True, max_length=255, help_text="The directory")

    def __str__(self) -> str:
        """Text representation of a Directory."""
        return f"{self.path} - {self.title}"

    def write_imdb_url(self) -> None:
        """Write IMDB URL to imdb.txt file."""
        txtpath = Path(self.path, "imdb.txt")
        with txtpath.open("w") as f:
            f.write(f"{self.title.imdb_url}\n")
        logger.debug(f"Wrote IMDB url {self.title.imdb_url} to {txtpath}")


@app.admin(list_display=["id", "title", "data", "updated_at"])
class CGNGData(models.Model):
    """Movie metadata from CGNG. Contains cover+ranking and more which is not available from django-imdb."""

    title: models.OneToOneField[Title, Title] = models.OneToOneField(
        "django_imdb.Title", on_delete=models.PROTECT, related_name="cgngdata"
    )
    data: models.JSONField = models.JSONField(help_text="The movie data from CGNG")
    language: models.CharField[str, str] = models.CharField(
        default="en", help_text="The language code used when fetching this data."
    )
    updated_at: models.DateTimeField[datetime.datetime, datetime.datetime] = models.DateTimeField(
        auto_now=True, help_text="Date and time when this data was last updated."
    )

    def __str__(self) -> str:
        """Text representation."""
        return f"{self.title.pk} extra metadata, {len(self.data.keys())} keys"


class Playtime:
    """Main Playtime class."""

    def __init__(self, cache_directory: Path) -> None:
        """Initialise stuff."""
        self.cache_directory = cache_directory.expanduser()
        self.cover_directory = self.cache_directory / "covers"
        self.cover_directory.mkdir(exist_ok=True, parents=True)
        logger.debug(
            f":rocket: Initialising Playtime version [cyan]{__version__}[/cyan] with cachedir {cache_directory} ..."
        )

    #### IDENTIFICATION #####

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
                logger.debug(f"Checking file [cyan]{textfile}[/cyan] for IMDB id...")
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

    def local_title_search(self, title: str, year: int | None) -> Title | None:
        """Search the local datbase for the title and optionally restrict to year."""
        title = pocketsearch_normalise(title)
        logger.debug(f"Searching for title '{title}' and year {year}")
        results = title_search(title=title, year=year)
        if not results:
            logger.debug(f"No results found in local database searching for title {title} year {year}")
            return None
        return Title.objects.get(title_id=results[0])

    def identify_directories(
        self,
        *,
        directories: list[Path],
        persist: bool,
        force: bool,
        ignore_textfiles: bool,
        accept_languages: list[str],
    ) -> None:
        """Identify titles in directories."""
        for relpath in directories:
            # resolve relative dirs
            path = relpath.resolve()
            # identify directory
            title, source = self.identify_directory(directory=path, ignore_textfiles=ignore_textfiles, force=force)
            if not title:
                logger.warning(f":cross_mark: {path} - Unable to identify title.")
                continue
            logger.info(f"{path} - {title} ({source})")

            # persist directory identification in database
            directory, _ = Directory.objects.get_or_create(
                path=path,
                defaults={
                    "title": title,
                },
            )

            # persist identification in directory?
            if persist:
                directory.write_imdb_url()

            if not hasattr(title, "cgngdata"):
                # get extra metadata in english
                cgng = self.get_cgng_data(title=title, accept_languages=accept_languages)
                if cgng is None:
                    continue

            # download cover?
            coversrc = self.get_coverpath(title)
            url = title.cgngdata.data["primary_image"]  # type: ignore[attr-defined]
            if not self.get_coverpath(title).exists() and url is not None:
                logger.debug(f"Downloading cover for {title}")
                download_file(url=url, path=coversrc)

            # copy cover to directory?
            coverdst = path / "poster.jpg"
            if coversrc.exists() and not coverdst.exists():
                shutil.copy(coversrc, coverdst)

    def identify_directory(
        self,
        *,
        directory: Path,
        ignore_textfiles: bool = False,
        force: bool = False,
    ) -> tuple[Title, str] | tuple[None, None]:
        """Attempt to identify content in directory, create and return a Directory object."""
        if not force:
            # not forcing identification, try local database first
            try:
                dbdir = Directory.objects.get(path=str(directory))
                logger.debug(f"{directory} - known in local database, returning title {dbdir.title}")
            except Directory.DoesNotExist:
                logger.debug(f"{directory} - not known in local database, identifying...")
            else:
                return dbdir.title, "directory known in local database"

        if not ignore_textfiles:
            logger.debug(f"{directory} - trying to find IMDB id in textfile...")
            textfile_imdb_id = self.find_imdb_id_in_textfiles(textfiles=self.find_textfiles(moviedir=directory))
            if textfile_imdb_id:
                logger.debug(f"{directory} - Found IMDB ID [cyan]{textfile_imdb_id}[/cyan] in textfile")
                try:
                    return Title.objects.get(title_id=textfile_imdb_id), "directory identified from textfile"
                except Title.DoesNotExist:
                    logger.debug(
                        f"{directory} - IMDB ID from textfile '{textfile_imdb_id}' did not return a Title from database"
                    )
            logger.debug(f"{directory} - could not find IMDB id in textfile in directory")

        logger.debug(f"{directory} - trying to parse movie name from directory name...")
        parsed = PTN.parse(directory.name)

        # get title
        title = parsed.get("title")
        if not title:
            logger.warning(
                f"{directory} - Failed to parse title for directory - please fix the directory name to be "
                "more standard and retry, or add the IMDB url of the movie in {directory / 'imdb.txt'}"
            )
            return None, None

        # get year
        year = parsed.get("year")
        logger.debug(f"{directory} - Parsed dirname to title [cyan]{title}[/cyan] year {year}")

        # check local database for this title
        dbtitle = self.local_title_search(title=title, year=year)
        if not dbtitle and year:
            # try without year
            dbtitle = self.local_title_search(title=title, year=None)
        if dbtitle:
            Directory.objects.get_or_create(
                path=directory,
                defaults={
                    "title": dbtitle,
                },
            )
            return dbtitle, "title identified from directory name"
        logger.debug(f"Local database search for '{title}' from year '{year}' did not return anything useful")
        return None, None

    def list_directories(self) -> None:
        """Loop over and output all Directories."""
        for directory in Directory.objects.all().order_by("path"):
            logger.info(directory)

    def update_extra_metadata(self, accept_languages: list[str]) -> None:
        """Get or update metadata for all Directory titles."""
        for title in Title.objects.filter(directories__isnull=False).distinct():
            self.get_cgng_data(title=title, accept_languages=accept_languages)

    def get_cgng_data(
        self,
        title: Title,
        accept_languages: list[str],
        keys: tuple[str, ...] = ("bottom_ranking", "top_ranking", "country_codes", "language_codes", "primary_image"),
        max_age_seconds: int = 86400 * 14,
    ) -> CGNGData | None:
        """Create or update a CGNGData object from IMDB id."""
        # check for existing data
        try:
            existing = CGNGData.objects.get(title_id=title.title_id)
            if (
                existing.language in accept_languages
                and (timezone.now() - existing.updated_at).seconds < max_age_seconds
            ):
                # no need to get new data
                return existing
        except CGNGData.DoesNotExist:
            pass

        # do initial request in english
        language = "en"
        data = self.cgng_lookup(title=title, language=language)
        if data is None:
            return None

        # get non-english metadata?
        most_spoken = data.language_codes[0]
        if most_spoken != "en" and accept_languages and most_spoken in accept_languages:
            # the most-spoken language in this movie is not-english,
            # but among languages acceptable to the user, get metadata in this language
            i18ndata = self.cgng_lookup(title=title, language=most_spoken)
            if i18ndata is not None:
                data = i18ndata
                language = most_spoken

        # save in database
        modeldata = {k: v for k, v in data.__dict__.items() if k in keys}
        cgng, created = CGNGData.objects.update_or_create(
            title_id=title.title_id, defaults={"data": modeldata, language: language}
        )
        if created:
            logger.debug(f"Downloaded extra metadata for title {title} in language {language}")
        else:
            logger.debug(f"Updated extra metadata for title {title} in language {language}")
        return cgng

    def cgng_lookup(
        self,
        title: Title,
        language: str,
    ) -> CgngObj | None:
        """Do online IMDB lookup and return the data."""
        try:
            return imdb.get_title(title.title_id, accept_language=language)
        except Exception:
            logger.exception(f":cross_mark: Unable to lookup title {title} using cinemagoerng, skipping")
            return None

    #### SYMLINKS #####

    def clean_category_dir(self, category_dir: Path) -> None:
        """Clean any old directories and symlinks from the categorydir."""
        if category_dir.exists():
            # the clean-slate protocol, sir?
            shutil.rmtree(category_dir)
        # make sure the category dir exists
        category_dir.mkdir(parents=True)

    def get_title_categories(self, title: Title, category: str) -> list[str]:
        """Return symlink categories for a Title and category."""
        if category == "genres":
            things = title.genrelist
        elif category == "years":
            things = title.yearlist
        elif category in ["directors", "producers", "writers", "composers", "selfs"]:
            persons = title.crewmembers.filter(category=category[:-1]).values_list("person__name", flat=True)  # type: ignore[attr-defined]
            things = list(persons)
        elif category == "actors":
            things = list(
                title.crewmembers.filter(category__in=["actor", "actress"]).values_list("person__name", flat=True)  # type: ignore[attr-defined]
            )
        elif category == "runtime":
            things = [str(title.runtime_minutes // 30)] if title.runtime_minutes else []
        elif category == "rating":
            things = [title.rating.rating] if hasattr(title, "rating") else []
        elif category in ["top_ranking", "bottom_ranking"]:
            things = [title.cgngdata.data[category]] if hasattr(title, "cgngdata") else []
        elif category == "language":
            things = title.cgngdata.data["language_codes"] if hasattr(title, "cgngdata") else []
        elif category == "languages":
            things = [",".join(title.cgngdata.data["language_codes"])] if hasattr(title, "cgngdata") else []
        else:
            things = []
        return things

    def create_symlink_dirs(
        self, *, symlink_dir: Path, categories: list[str], accept_languages: list[str], relative: bool = True
    ) -> None:
        """Create symlink dirs for the requested categories."""
        symlink_dir = symlink_dir.resolve()
        if not symlink_dir.exists():
            symlink_dir.mkdir()
        coverdir = symlink_dir / ".covers"
        coverdir.mkdir(exist_ok=True)
        for category in categories:
            categorydir = symlink_dir / category
            self.clean_category_dir(category_dir=categorydir)
            logger.info(f"Creating symlinks by {category} in {categorydir} with languages {accept_languages}...")
            for title in Title.objects.filter(directories__isnull=False):
                logger.debug(f"Creating {category} symlinks for {title}")
                # loop over things in this category for this title
                for thing in self.get_title_categories(title=title, category=category):
                    if thing is None or thing == "":
                        continue
                    # thing is Western, Drama, actor name, year and such
                    subdir = self.get_category_subdir(
                        categorydir=categorydir,
                        category=category,
                        thing=thing,
                        title=title,
                        accept_languages=accept_languages,
                    )
                    logger.debug(f"Creating {category} symlinks for {thing} in subdir {subdir}")
                    subdir.mkdir(exist_ok=True, parents=True)
                    # loop over copies of this title
                    for directory in title.directories.all():  # type: ignore[attr-defined]
                        # the path of the symlink (not the link target)
                        linkpath = subdir / Path(directory.path).name
                        if linkpath.is_symlink():
                            # The link to this movie already exists.
                            # This can happen when the same titledir name
                            # exists in multiple places, ignore and continue
                            continue
                        if relative:
                            logger.debug(f"Creating relative symlink at {linkpath} to {directory.path}")
                            target = Path(os.path.relpath(directory.path, linkpath.parent))
                        else:
                            logger.debug(f"Creating absolute symlink at {linkpath} to {directory.path}")
                            target = Path(subdir)
                        # create symlink to this directory
                        linkpath.symlink_to(target, target_is_directory=True)
                        self.symlink_cover(cover_dest_dir=linkpath.parent, symlink_dir=symlink_dir, title=title)
        self.add_counts_to_dirnames(symlink_dir=symlink_dir, categories=categories)
        logger.debug("Done")

    def symlink_cover(self, cover_dest_dir: Path, symlink_dir: Path, title: Title) -> None:
        """Symlink the cover for this title."""
        logger.debug(f"Creating cover symlink for {title} in {cover_dest_dir} with symlink dir {symlink_dir}")
        coverdir = symlink_dir / ".covers"
        coverpath = coverdir / f"{title.title_id}.jpg"
        if not coverpath.exists():
            coversource = self.get_coverpath(title=title)
            if not coversource.exists():
                return
            # copy cover from the source
            shutil.copy(coversource, coverpath)
        coverdest = cover_dest_dir / "poster.jpg"
        # make symlink destination relative
        relcoverpath = os.path.relpath(coverpath, coverdest.parent)
        if not coverdest.exists():
            logger.debug(f"Creating cover symlink for {title} from {coverdest} to {relcoverpath}")
            coverdest.symlink_to(relcoverpath)

    def get_category_subdir(
        self, categorydir: Path, category: str, thing: str, title: Title, accept_languages: list[str]
    ) -> Path:
        """Return the subdir to use for this title in this category."""
        subdir: Path
        aka = self.get_title_aka(title=title, accept_languages=accept_languages)
        aka = aka.replace(os.sep, "_")
        dirname = f"{aka} ({title.premiered})"
        if category in ["actors", "directors", "producers", "composers", "writers"]:
            # make a level with the first letter
            subdir = categorydir / str(thing)[0] / str(thing) / dirname
        elif category == "runtime":
            step = 30
            runtime = int(thing) * step
            subdir = categorydir / f"{runtime}-{runtime + step}" / dirname
        elif category == "rating":
            subdir = categorydir / str(thing) / f"{dirname} ({title.rating.votes} votes)"  # type: ignore[attr-defined]
        elif category in ["top_ranking", "bottom_ranking"]:
            subdir = (
                categorydir / f"{thing:04}. {dirname} ({title.rating.rating} with {title.rating.votes} votes)"  # type: ignore[attr-defined]
            )
        else:
            subdir = categorydir / str(thing) / dirname
        return subdir

    def get_coverpath(self, title: Title) -> Path:
        """Get the cover path for this title. They are always jpg."""
        return self.cover_directory / f"{title.title_id}.jpg"

    def add_counts_to_dirnames(self, *, symlink_dir: Path, categories: list[str]) -> None:
        """Add counts to directory names on all levels."""
        for category in categories:
            if category in ["top_ranking", "bottom_ranking"]:
                continue
            unit = "people" if category in ["actors", "directors", "producers", "composers", "writers"] else "titles"
            categorydir = symlink_dir / category
            self.rename_dirs_with_counts(directories=list(categorydir.iterdir()), unit=unit)
            if category in ["actors", "directors", "producers", "composers", "writers"]:
                # go one level deeper for the people categories
                for thingdir in categorydir.iterdir():
                    self.rename_dirs_with_counts(directories=list(thingdir.iterdir()), unit="titles")

    def rename_dirs_with_counts(self, directories: list[Path], unit: str) -> None:
        """Rename the dirs with the number of things in them."""
        for directory in directories:
            count = len([d for d in directory.iterdir() if d.is_dir()])
            countdir = directory.parent / f"{directory.name} ({count} {unit})"
            logger.debug(f"Renaming {directory} to {countdir}")
            directory.rename(countdir)

    #### MISC #####

    def minsec(self, seconds: int) -> tuple[int, int]:
        """Return minutes and seconds."""
        spm = 60
        if seconds > spm:
            mins = seconds // spm
            secs = round(seconds % spm)
        else:
            mins = 0
            secs = seconds
        return mins, secs

    def get_title_aka(self, title: Title, accept_languages: list[str]) -> str:
        """Return the best aka for a title considering accept_languages."""
        # use original title if cgngdata["language_codes"] containes one of the acceptable_languages
        if title.cgngdata and "language_codes" in title.cgngdata.data and title.cgngdata.data["language_codes"]:  # type: ignore[attr-defined]
            for lang in accept_languages:
                if lang in title.cgngdata.data["language_codes"]:  # type: ignore[attr-defined]
                    return title.original_title
        return title.primary_title


############## BOILERPLATE #########################################################################


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
    # playtime identify
    identify_parser = subparsers.add_parser(
        "identify", help="Identify titles in directories, and update local database with the results."
    )
    identify_parser.add_argument(
        "titledirs",
        type=Path,
        nargs="+",
        help=(
            "Required. The directories containing titles to identify. "
            "Supports shell globs (like /movies/*). Supports multiple dirs (like /a/* /b/*)."
        ),
    )
    identify_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Attempt (re)identification even if the directory is already known in the local database.",
    )
    identify_parser.add_argument(
        "-t",
        "--ignore-textfiles",
        action="store_true",
        default=False,
        help="Ignore IMDB IDs in textfiles in each directory when identifying.",
    )
    identify_parser.add_argument(
        "-p",
        "--persist",
        action="store_true",
        default=False,
        help="Persist directory identification by writing the IMDB URL to imdb.txt.",
    )

    ###########################################
    # playtime extrameta
    extrameta_parser = subparsers.add_parser("extrameta", help="Get/update extra metadata (cover+ranking) from IMDB.")
    extrameta_parser.add_argument(
        "-l",
        "--languages",
        type=parse_comma_str_to_list,
        default=[],
        help="Comma-separated list of acceptable non-english languages.",
    )

    ###########################################
    # playtime symlink
    symlink_parser = subparsers.add_parser("symlink", help="Create directory hierachy with title categories.")
    symlink_parser.add_argument(
        "symlinkdir",
        type=Path,
        help="Required. The directory in which to create the title category symlinks.",
    )
    symlink_parser.add_argument(
        "-C",
        "--categories",
        nargs="+",
        default=[
            "genres",
            "years",
            "directors",
            "producers",
            "writers",
            "composers",
            "actors",
            "selfs",
            "runtime",
            "rating",
            "top_ranking",
            "bottom_ranking",
            "language",
            "languages",
        ],
        help="Movie categories to enable for 'playtime symlink'.",
    )
    symlink_parser.add_argument(
        "-l",
        "--languages",
        type=parse_comma_str_to_list,
        default=[],
        help="Comma-separated list of acceptable non-english languages.",
    )

    ###########################################
    # playtime ls
    subparsers.add_parser("ls", help="List all known directories.")

    ###########################################
    # playtime import
    import_parser = subparsers.add_parser("import", help="Downloading and importing IMDB data")
    import_parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path.home() / ".cache/django-imdb-tsv-data",
        help="Download directory for IMDB datadumps.",
    )
    import_parser.add_argument(
        "--download-host",
        type=str,
        default="datasets.imdbws.com",
        help="Hostname from which to download IMDB data dumps, defaults to datasets.imdbws.com",
    )
    import_parser.add_argument(
        "--skip-name-basics",
        action="store_true",
        default=False,
        help="Do not import name.basics.tsv.gz.",
    )
    import_parser.add_argument(
        "--skip-title-basics",
        action="store_true",
        default=False,
        help="Do not import title.basics.tsv.gz.",
    )
    import_parser.add_argument(
        "--skip-title-akas",
        action="store_true",
        default=False,
        help="Do not import title.akas.tsv.gz.",
    )
    import_parser.add_argument(
        "--skip-title-principals",
        action="store_true",
        default=False,
        help="Do not import title.principals.tsv.gz.",
    )
    import_parser.add_argument(
        "--skip-title-episodes",
        action="store_true",
        default=False,
        help="Do not import title.episodes.tsv.gz.",
    )
    import_parser.add_argument(
        "--skip-title-ratings",
        action="store_true",
        default=False,
        help="Do not import title.ratings.tsv.gz.",
    )
    import_parser.add_argument(
        "--max-tsv-age-seconds",
        default=86400 * 14,
        type=int,
        help=(
            "Delete existing TSV files and download new ones from IMDB if "
            "the existing files are older than this number of seconds."
        ),
    )
    import_parser.add_argument(
        "-l",
        "--languages",
        type=parse_comma_str_to_list,
        default=[],
        help="Comma-separated list of acceptable non-english languages.",
    )

    ###########################################
    # playtime export
    export_parser = subparsers.add_parser("export", help="Exporting IMDB TSV files from DB")
    export_parser.add_argument(
        "--export-dir",
        type=Path,
        default=Path.home() / "imdbexport",
        help="Directory for IMDB datadumps.",
    )
    export_parser.add_argument(
        "--skip-name-basics",
        action="store_true",
        default=False,
        help="Do not export name.basics.tsv.gz.",
    )
    export_parser.add_argument(
        "--skip-title-basics",
        action="store_true",
        default=False,
        help="Do not export title.basics.tsv.gz.",
    )
    export_parser.add_argument(
        "--skip-title-akas",
        action="store_true",
        default=False,
        help="Do not export title.akas.tsv.gz.",
    )
    export_parser.add_argument(
        "--skip-title-principals",
        action="store_true",
        default=False,
        help="Do not export title.principals.tsv.gz.",
    )
    export_parser.add_argument(
        "--skip-title-episodes",
        action="store_true",
        default=False,
        help="Do not export title.episodes.tsv.gz.",
    )
    export_parser.add_argument(
        "--skip-title-ratings",
        action="store_true",
        default=False,
        help="Do not export title.ratings.tsv.gz.",
    )
    return parser


def parse_comma_str_to_list(values: str) -> list[str]:
    """Parse comma-separated string to list."""
    return values.split(",")


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
        datefmt = "%Y-%m-%d %H:%M:%S %Z"
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


def main(mockargs: list[str] | None = None) -> None:
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

    if args.subparser_name == "import":
        import_tsv_files(
            download_dir=args.download_dir,
            download_host=args.download_host,
            skip_name_basics=args.skip_name_basics,
            skip_title_basics=args.skip_title_basics,
            skip_title_akas=args.skip_title_akas,
            skip_title_principals=args.skip_title_principals,
            skip_title_episodes=args.skip_title_episodes,
            skip_title_ratings=args.skip_title_ratings,
            max_tsv_age_seconds=args.max_tsv_age_seconds,
        )

    elif args.subparser_name == "export":
        export_tsv_files(
            export_dir=args.export_dir,
            skip_name_basics=args.skip_name_basics,
            skip_title_basics=args.skip_title_basics,
            skip_title_akas=args.skip_title_akas,
            skip_title_principals=args.skip_title_principals,
            skip_title_episodes=args.skip_title_episodes,
            skip_title_ratings=args.skip_title_ratings,
        )

    elif args.subparser_name == "extrameta":
        pt.update_extra_metadata(accept_languages=args.languages)

    elif args.subparser_name == "identify":
        pt.identify_directories(
            directories=args.titledirs,
            force=args.force,
            ignore_textfiles=args.ignore_textfiles,
            persist=args.persist,
            accept_languages=args.languages,
        )

    elif args.subparser_name == "symlink":
        pt.create_symlink_dirs(symlink_dir=args.symlinkdir, categories=args.categories, accept_languages=args.languages)

    elif args.subparser_name == "ls":
        pt.list_directories()

    else:
        logger.error("Playtime subcommand missing!")
        parser.print_help()

    logger.debug(":person_raising_hand: Playtime is over - bye!")


if __name__ == "__main__":
    main()
