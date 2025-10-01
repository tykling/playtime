# type: ignore  # noqa: PGH003
"""Fixtures for pytest."""

from pathlib import Path

import pytest

from playtime import Playtime


@pytest.fixture(scope="session")
def playtime(tmpdir_factory, movies):
    """Fixture to return an updated Playtime instance and Path for moviedirs."""
    cachedir = Path(tmpdir_factory.mktemp("cache"))
    pt = Playtime(cache_directory=cachedir)
    pt.update_cache(
        new_basedirs=[movies / "movies1", movies / "movies2"],
    )
    return pt, movies


def create_moviedirs(basedir, movies):
    """Recursive thing to create directories and files from a dict."""
    for pathname, contents in movies.items():
        path = Path(basedir, pathname)
        if isinstance(contents, dict):
            path.mkdir()
            # call recursively to create directory with contents
            create_moviedirs(basedir=path, movies=contents)
        elif isinstance(contents, str):
            # write a file
            with path.open("w") as f:
                f.write(contents)


@pytest.fixture(scope="session")
def movies(tmpdir_factory):
    """Return moviedir test data."""
    moviedir = Path(tmpdir_factory.mktemp("movies"))
    movies = {
        "movies1": {
            "commando": {},
        },
        "movies2": {
            "commando.1985": {},
            "jackass4": {
                "imdb.txt": "https://www.imdb.com/title/tt11466222/",
            },
        },
    }
    create_moviedirs(moviedir, movies)
    return moviedir
