import os
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

in_type.extend(experimental_type)

def sourceGen(path: list[str], recursive: bool) -> Generator[Path, None, None]:
    if recursive:
        func = "rglob"
    else:
        func = "glob"
    
    for folder in path:
        for _ext in in_type:
            for _file in getattr(Path(folder), func)(_ext):
                if os.path.isfile(_file):
                    yield _file