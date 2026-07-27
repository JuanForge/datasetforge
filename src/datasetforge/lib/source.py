from pathlib import Path
from typing import Generator

in_type: list[str] = [
    "*.jpg",
    "*.jpeg",
    "*.png"
]

def sourceGen(path: list[str], recursive: bool) -> Generator[Path, None, None]:
    if recursive:
        func = "rglob"
    else:
        func = "glob"
    
    for folder in path:
        for _ext in in_type:
            for _file in getattr(Path(folder), func)(_ext):
                yield _file