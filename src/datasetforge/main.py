import argparse
import hashlib
import heapq
import itertools
import json
import math
import os
import sys
import tempfile
import time
import warnings
from pathlib import Path
from typing import Generator

import imagehash
import orjson
from imagehash import ImageHash
from tqdm import tqdm

from datasetforge import __version__
from datasetforge.lib.build_dataset import main as lib_build_dataset_main
from datasetforge.lib.inject_console import PrefixWriter
from datasetforge.lib.phash import get as phash_value

in_type = "*.jpg"

def export_command(args: argparse.Namespace) -> None:
    # args.input: list[str]
    # args.output: str
    # args.pass2png: bool -> int
    # args.threads: int
    # args.verbose: bool
    print("="*5 + "build_dataset start" + "="*5)
    original_stdout = sys.stdout
    sys.stdout = PrefixWriter(sys.stdout, "[lib:build_dataset:main] ")
    lib_build_dataset_main(
        mode=int(not args.pass2png),
        out=str(args.output),
        verbose=bool(args.verbose),
        threads=int(args.threads),
        folders=list(args.input)
        )
    sys.stdout = original_stdout
    print("="*5 + "build_dataset end" + "="*5)
    return

def _index_command(input: list[str], output: str, threads: int = 0, verbose: bool = True, phash_bits: int = 64) -> None:
    hash_size: int = math.isqrt(phash_bits)
    print(f"hash size : {hash_size}")
    
    if hash_size * hash_size != phash_bits:
        raise ValueError(
            "--phash-bits must be a perfect square (64, 256, 1024, ...)"
    )
    
    base = os.path.join(output, __version__, "__unit__")
    os.makedirs(base, exist_ok=True)
    
    with open(os.path.join(output, __version__, "META.json"), "w", encoding="utf-8") as meta:
        meta.write(json.dumps(
            {
                "phash": {
                    "bits": phash_bits
                }
            }
        ))
    try:
        numOffile = 0
        print(f"syncro in {', '.join(input)}...")
        for folder in input:
            for _ in Path(folder).rglob(in_type):
                if _.is_file():
                    numOffile += 1
        
        for file in tqdm(
            (file for folder in input for file in Path(folder).rglob(in_type)),
            total=numOffile,
            desc="index",
            dynamic_ncols=True,
            smoothing=0.05,
            mininterval=0.5,
            miniters=1
            ):
            outJson = f"{os.path.join(base, file.stem)}.json"
            outJsonTemp = f"{outJson}.temp"
            if not os.path.isfile(outJson):
                with open(file, "rb") as infile:
                    if verbose:
                        tqdm.write(f"[ in  ] : path : '{file}', output : '{outJson}'")
                    
                    data: bytes = infile.read()
                    
                    open(outJsonTemp, "wb").write(orjson.dumps(
                        {
                            "phash": str(phash_value(data, hash_size=hash_size)),
                            "sha256": hashlib.sha256(data).hexdigest(),
                            "size": len(data),
                            "path": str(Path(file).resolve())
                        }
                    ))
                    os.replace(outJsonTemp, outJson)
    except KeyboardInterrupt:
        return

def index_command(args: argparse.Namespace) -> None:
    # args.input: list[str]
    # args.output: str
    # args.threads: int - nos used
    # args.verbose: bool
    # args.phash_bits: int
    return _index_command(
        input=args.input,
        output=args.output,
        threads=args.threads,
        verbose=args.verbos,
        phash_bits=args.phash_bits
    )

def jsonloadcache(x: bytes) -> dict[str, str | int]:
    data = orjson.loads(x)
    return {
        "path":    data["path"],
        "phash":   data["phash"],
        "sha256":  data["sha256"],
        "size":    data["size"]
    }

def phash_live(phash_max_percent: float, phash_min_percent: float, percent: float|int) -> list[bool | float | int]:
    if percent > 100 or percent < 0:
        RuntimeError(f"panic, percent is {percent}")
    
    if percent <= phash_max_percent and percent >= phash_min_percent:
        return [True, percent]
    
    return [False, 0]

def duplicates_command(args: argparse.Namespace) -> None:
    # args.top_k: int
    # args.phash: bool
    # args.input: list[str]
    # args.input_cache: str
    # args.output: str
    # args.verbose: bool
    # args.steam: bool
    # args.phash_live: bool
    # args.phash_max_percent: float - default == 0.0
    # args.phash_min_percent: float - default == 0.0
    # args.phash_bits: int
    tmp = None
    try:
        top_k = int(args.top_k)
        #if top_k <= 0:
        #    raise RuntimeError("invalide top-k value")
        
        if args.input and args.input_cache:
            raise RuntimeError("You have specified both --input and --input-cache, which cannot work together.")
        
        if args.phash_max_percent == 0.0 and args.phash_live:
            raise RuntimeError("--phash-live defined without --phash-max-percent")
        
        if (not args.phash_live) and args.phash_max_percent != 0.0:
            raise RuntimeError("--phash-max-percent defined without --phash-live")
        
        
        if args.input:
            print("Build the index files...")
            tmp = tempfile.TemporaryDirectory()
            _index_command(input=args.input, output=tmp.name, phash_bits=args.phash_bits)
            _input = os.path.join(tmp.name, __version__)
        else:
            _input = args.input
        
        
        with open(os.path.join(_input, "META.json"), "r", encoding="utf-8") as meta:
            phash_bits = json.loads(meta.read())["phash"]["bits"]
        
        _input = os.path.join(_input, "__unit__")
        
        numOffile = 0
        input: list[str] = [_input]
        print(f"syncro in {', '.join(input)}...")
        for folder in input:
            for _ in Path(folder).rglob('*.json'):
                if _.is_file():
                    numOffile += 1
        
        comparisons = []
        counter = itertools.count()
        
        if not args.stream:
            
            hashes: dict[str, ImageHash] = {}
            
            for file in tqdm(
                (file for folder in input for file in Path(folder).rglob('*.json')),
                total=numOffile,
                desc="duplicates",
                dynamic_ncols=True,
                smoothing=0.05,
                mininterval=0.5,
                miniters=1
                ):
                data = jsonloadcache(open(file, "rb").read())
                
                
                path = str(data["path"])
                phash: ImageHash = imagehash.hex_to_hash(data["phash"])
                
                for old_path, old_phash in hashes.items():
                    distance = phash - old_phash
                    
                    if args.phash_live:
                        temp = phash_live(float(args.phash_max_percent), float(args.phash_min_percent), percent = distance/phash_bits * 100)
                        if temp[0]:
                            tqdm.write(f"phash-live : {path} - {old_path} : {temp[1]}% diff")
                    
                    item = {
                        "path1": old_path,
                        "path2": path,
                        "distance": distance,
                    }
                    
                    entry = (-distance, next(counter), item)
                    
                    if top_k > 0:
                        if len(comparisons) < top_k:
                            heapq.heappush(comparisons, entry)
                        
                        elif distance < -comparisons[0][0]:
                            heapq.heapreplace(comparisons, entry)
                hashes[path] = phash
        else:
            
            pbar = tqdm(
                total=numOffile ** 2,
                desc="duplicates",
                dynamic_ncols=True,
                smoothing=0.05,
                mininterval=0.5,
                miniters=1
            )
            
            def file_stream() -> Generator[str, None, None]:
                for folder in input:
                    for file in Path(folder).rglob("*.json"):
                        yield str(file)
            
            for file in file_stream():
                data = jsonloadcache(open(file, "rb").read())
                
                path = str(data["path"])
                phash = imagehash.hex_to_hash(data["phash"])
                
                for old_file in file_stream():
                    pbar.update(1)
                    if old_file == file:
                        continue
                    
                    old_data = jsonloadcache(open(old_file, "rb").read())
                    
                    old_path = str(old_data["path"])
                    old_phash = imagehash.hex_to_hash(old_data["phash"])
                    
                    distance = phash - old_phash
                    
                    if args.phash_live:
                        temp = phash_live(float(args.phash_max_percent), float(args.phash_min_percent), percent = distance/phash_bits * 100)
                        if temp[0]:
                            tqdm.write(f"phash-live : {path} - {old_path} : {temp[1]}% diff")
                    
                    item = {
                        "path1": path,
                        "path2": old_path,
                        "distance": distance,
                    }
                    
                    entry = (-distance, next(counter), item)
                    
                    if top_k > 0:
                        if len(comparisons) < top_k:
                            heapq.heappush(comparisons, entry)
                        
                        elif distance < -comparisons[0][0]:
                            heapq.heapreplace(comparisons, entry)
        
        comparisons.sort(key=lambda x: x[2]["distance"])
        
        for _, _, item in comparisons:
            print(
                f"k-top : {item['path1']} - {item['path2']} : {item['distance']/phash_bits * 100}% diff"
            )
    except KeyboardInterrupt:
        pass
    finally:
        del tmp



def main() -> None:
    if os.name == "nt":
        warnings.warn(
            "The current kernel is not officially supported. "
            "Windows NT kernels are not supported by this application. "
            "Some features may not work correctly.",
            RuntimeWarning
        )
    
    print("""\033[33mWARNING:
        This version is intended for developers.
        Some features may still experience instability, including potential Out-Of-Memory (OOM) issues.
        The 'duplicates' subcommand, in particular, can consume a significant amount of memory and may trigger OOM errors depending on the dataset size.
        
        Please monitor your system resources carefully while using this feature. \033[0m
        """
    )
    
    _help_phash_bits = "Set the pHash bit size. Higher values increase precision and reduce collisions, which is useful for large datasets."
    
    parser = argparse.ArgumentParser(
        prog="datasetforge",
        description="DatasetForge is a toolkit for optimizing and preparing image datasets for AI training.",
        allow_abbrev=False
    )
    
    parser.add_argument(
        "--allow-unsupported-kernel",
        help="Allow execution on unsupported kernels.",
        action="store_true"
    )
    
    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )
    
    # == export ==
    export_parser = subparsers.add_parser(
        "export",
        help="Exporter un dataset",
        allow_abbrev=False
    )
    export_parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="Folders."
    )
    export_parser.add_argument(
        "--output",
        help="Foler ouput.",
        required=True
    )
    export_parser.add_argument(
        "--pass2png",
        help="convert input format to png",
        action="store_true"
    )
    export_parser.add_argument(
        "--verbose",
        help="",
        action="store_true"
    )
    export_parser.add_argument(
        "--threads",
        help="Num of threads.",
        type=int,
        default=0
    )
    
    export_parser.set_defaults(
        func=export_command
    )
    
    # == index ==
    index_parser = subparsers.add_parser(
        "index",
        help="Make the index file for image.",
        allow_abbrev=False
    )
    index_parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="Folders."
    )
    index_parser.add_argument(
        "--output",
        help="Foler ouput for cache json file.",
        required=True
    )
    index_parser.add_argument(
        "--verbose",
        help="",
        action="store_true"
    )
    index_parser.add_argument(
        "--threads",
        help="Num of threads.",
        type=int,
        default=0
    )
    index_parser.add_argument(
        "--phash-bits",
        help=_help_phash_bits,
        type=int,
        default=64
    )
    
    index_parser.set_defaults(
        func=index_command
    )
    
    # == duplicates ==
    duplicates_parser = subparsers.add_parser(
        "duplicates",
        help="",
        allow_abbrev=False
    )
    duplicates_parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="Folders of the datasets."
    )
    duplicates_parser.add_argument(
        "--input-cache",
        help="Folder of the cached json.",
        type=str
    )
    duplicates_parser.add_argument(
        "--phash",
        action="store_true"
    )
    duplicates_parser.add_argument(
        "--phash-bits",
        help="Requires `--input` to operate; `--input-cache` uses the same one specified during its creation. " + _help_phash_bits,
        type=int,
        default=64
    )
    duplicates_parser.add_argument(
        "--phash-live",
        action="store_true",
        help=(
            "Display all pHash matches matching --phash-max-percent instead of "
            "limiting results to top-k. Requires --phash-max-percent."
        )
    )
    duplicates_parser.add_argument(
        "--phash-max-percent",
        type=float,
        default=0.0,
        help=(
            "Maximum pHash difference percentage to display. Must be used together "
            "with --phash-live. Displays every file pair with a pHash difference "
            "less than or equal to the specified percentage."
        )
    )
    duplicates_parser.add_argument(
        "--phash-min-percent",
        type=float,
        default=0.0,
        help=(
            "Minimum pHash difference percentage to display. Must be used together "
            "with --phash-live. Displays every file pair with a pHash difference "
            "greater than or equal to the specified percentage."
        )
    )
    duplicates_parser.add_argument(
        "--top-k",
        type=int,
        default=0
    )
    duplicates_parser.add_argument(
        "--stream",
        action="store_true"
    )
    #duplicates_parser.add_argument(
    #    "--auto-index",
    #    action="store_true"
    #)
    duplicates_parser.set_defaults(
        func=duplicates_command
    )
    # ======
    
    args = parser.parse_args()
    
    if (not args.allow_unsupported_kernel ) and os.name != "posix":
        print("""\033[33mWARNING:
        This platform is not officially supported and the application cannot continue.
        If you want to run anyway, use:
            --allow-unsupported-kernel
        
        Warning: Running on an unsupported kernel may cause unexpected behavior.\033[0m
        """
        )
        sys.exit(295)
    
    args.func(args)


if __name__ == "__main__":
    main()