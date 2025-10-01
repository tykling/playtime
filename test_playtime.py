# type: ignore  # noqa: PGH003
"""Basic tests for playtime. Requires internet and imdb to be working."""

from pathlib import Path

from playtime import Playtime


def test_update_cache(playtime: tuple[Playtime, Path]) -> None:
    """Test basic 'playtime update' functionality."""
    pt, moviedir = playtime
    assert "tt0088944" in pt.cache.movies
    assert "tt11466222" in pt.cache.movies

    assert moviedir / "movies1/commando" in pt.cache.directories
    assert moviedir / "movies2/commando.1985" in pt.cache.directories
    assert moviedir / "movies2/jackass4" in pt.cache.directories


def test_cover_download(playtime) -> None:
    """Test 'playtime download'."""
    pt, moviedir = playtime
    pt.download_covers(save_covers_in_moviedirs=False, force_download=False)
    pt.download_covers(save_covers_in_moviedirs=False, force_download=False)
    assert (pt.cache.cachedir / "covers/tt0088944.jpg").exists()
    assert (pt.cache.cachedir / "covers/tt11466222.jpg").exists()
    for moviedir in pt.cache.directories:
        assert (moviedir / "poster.jpg").is_symlink()

    pt.download_covers(save_covers_in_moviedirs=True, force_download=False)
    pt.download_covers(save_covers_in_moviedirs=True, force_download=False)
    for moviedir in pt.cache.directories:
        assert (moviedir / "poster.jpg").exists()
        assert not (moviedir / "poster.jpg").is_symlink()


def test_symlink_dirs(tmpdir_factory, playtime) -> None:
    """Test 'playtime symlink'."""
    pt, moviedir = playtime
    symlink_dir = Path(tmpdir_factory.mktemp("symlinks"))
    pt.create_symlink_dirs(
        symlink_dir=symlink_dir, categories=["genres", "year", "directors", "actors"], relative=False
    )
    commandolink = symlink_dir / "genres/Action (2 movies)/Commando (1985)/commando.1985"
    jackasslink = symlink_dir / "year/2022 (1 movies)/Jackass Forever (2022)/jackass4"

    assert commandolink.is_symlink()
    assert commandolink.readlink() == str(moviedir / "movies2/commando.1985")
    assert jackasslink.is_symlink()
    assert jackasslink.readlink() == str(moviedir / "movies2/jackass4")

    pt.create_symlink_dirs(symlink_dir=symlink_dir, categories=["genres", "year", "directors", "actors"], relative=True)
    assert commandolink.is_symlink()
    assert commandolink.readlink() == "../../../../movies0/movies2/commando.1985"
    assert jackasslink.is_symlink()
    assert jackasslink.readlink() == "../../../../movies0/movies2/jackass4"
