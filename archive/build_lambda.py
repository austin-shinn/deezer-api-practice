"""Build a Lambda deployment zip for one of the pull functions.

Installs requests for the Lambda Linux x86_64 / Python 3.13 target (not the local Windows
interpreter), copies the handler modules, and zips with forward-slash paths.

Usage: python build_lambda.py deezer|lastfm|xref
"""
import argparse
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).parent
SOURCES = {
    "deezer": ["deezer_handler.py", "deezer_pull.py", "lake.py"],
    "lastfm": ["lastfm_handler.py", "lastfm_pull.py", "lake.py"],
    "xref": ["xref_handler.py", "build_artist_xref.py", "lastfm_pull.py", "lake.py"],
}


def install_dependencies(build_dir: Path) -> None:
    subprocess.run(
        [
            sys.executable, "-m", "pip", "install", "requests",
            "--target", str(build_dir),
            "--platform", "manylinux2014_x86_64",
            "--implementation", "cp",
            "--python-version", "3.13",
            "--only-binary=:all:",
            "--quiet",
        ],
        check=True,
    )


def write_zip(build_dir: Path, zip_path: Path) -> None:
    zip_path.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(build_dir.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                zf.write(path, path.relative_to(build_dir).as_posix())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=sorted(SOURCES))
    args = parser.parse_args()

    build_dir = HERE / "build" / args.source
    zip_path = HERE / "dist" / f"{args.source}_pull.zip"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)
    install_dependencies(build_dir)
    for module in SOURCES[args.source]:
        shutil.copy(HERE / module, build_dir / module)
    write_zip(build_dir, zip_path)
    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"Built {zip_path} ({size_mb:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
