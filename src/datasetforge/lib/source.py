import os
import sys
from collections.abc import Generator
from pathlib import Path

in_type: list[str] = [
    "*.jpg",
    "*.jpeg",
    "*.png"
]
experimental_type: list[str] = [
    "*.webp"
]

type_video: list[str] = [
    "mp4",
    "mkv"
]

in_type.extend(experimental_type)

sourceGen_Cache_exclude = {}

def _if_exclude(file: Path, exclude_dirs: list[str]) -> bool:
    #global sourceGen_Cache_exclude
    for exclude in exclude_dirs:
        if file.absolute().is_relative_to(sourceGen_Cache_exclude[exclude]):
            return True
    return False

def sourceGen(path: list[str] | str, recursive: bool, exclude_dirs: list[str] | str | None, types: list[str] | None = None) -> Generator[Path, None, None]:
    if type(path) is str:
        path = [path]
    
    if type(exclude_dirs) is str:
        exclude_dirs = [exclude_dirs]
    
    if exclude_dirs is None:
        exclude_dirs = []
    
    if types is None:
        types = in_type
    
    if recursive:
        func = "rglob"
    else:
        func = "glob"
    
    for exclude in exclude_dirs:
        if not exclude in sourceGen_Cache_exclude:
            sourceGen_Cache_exclude[exclude] = Path(exclude).absolute()
    
    for folder in path:
        for _ext in types:
            for _file in getattr(Path(folder), func)(_ext):
                _file: Path
                if os.path.isfile(_file):  # noqa: SIM102
                    # pyrefly: ignore [bad-argument-type]
                    if not _if_exclude(_file, exclude_dirs):
                        yield _file

if __name__ == "__main__":
    import time
    start_time = time.monotonic()
    i = 0
    for _i in sourceGen(path=sys.argv[1], recursive=True, exclude_dirs=sys.argv[2]):
        i += 1
        if ".back" in str(_i):
            print(str(_i))
    print(time.monotonic() - start_time)
    print(i)