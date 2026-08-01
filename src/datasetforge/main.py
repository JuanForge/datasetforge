import argparse
import heapq
import itertools
import json
import math
import os
import sys
import tempfile
import threading
import time
import warnings
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import imagehash
import orjson
import psutil
from imagehash import ImageHash
from tqdm import tqdm

from datasetforge import __version__
from datasetforge.lib.benchmark import benchmark
from datasetforge.lib.build_dataset import RenameMode
from datasetforge.lib.build_dataset import main as lib_build_dataset_main
from datasetforge.lib.hash import hash_algo, hash_default
from datasetforge.lib.inject_console import PrefixWriter
from datasetforge.lib.multicore import Multicore
from datasetforge.lib.phash import get as phash_value
from datasetforge.lib.source import sourceGen


class errors:
    class invalidMemoryUnit(Exception):
        pass

process = psutil.Process(os.getpid())

_getMemoryAlloc_time: float = 0.0
_getMemoryAlloc_log: str = ""
def _getMemoryAlloc(interval: float = 0) -> str:
    global _getMemoryAlloc_time, _getMemoryAlloc_log
    if time.monotonic() - _getMemoryAlloc_time > interval:
        _getMemoryAlloc_log = f"{process.memory_info().rss / (1024 * 1024):.4f} MiB"
        _getMemoryAlloc_time = time.monotonic()
    
    return _getMemoryAlloc_log

def _ThreadSecureMemory(x: int) -> None:
    sleep = 5
    while True:
        time.sleep(sleep)
        size: int = process.memory_info().rss
        if size > x:
            for _ in range(5): print(f"[ SecureMemory ] : memory : {size} : EXIT !")
            os._exit(45)

UNITS: dict[str, int] = {
    "K": 1024,
    "M": 1024**2,
    "G": 1024**3,
    "T": 1024**4,
}

def parse_size(value: str) -> int:
    if value[-1] in UNITS:
        value = value.upper().replace(",", ".").strip()
        
        for unit, multiplier in UNITS.items():
            if value.endswith(unit):
                number = float(value[:-1])
                return int(number * multiplier)
        
        return int(float(value))
    else:
        raise errors.invalidMemoryUnit()

def benchmark_command(args: argparse.Namespace) -> None:
    benchmark()



def export_command(args: argparse.Namespace) -> None:
    # args.input: list[str]
    # args.output: str
    # args.threads: int
    # args.verbose: bool
    # args.no_recursive
    # args.rename: bool
    # args.rename_sha256: bool
    # args.output_format: str | None
    
    if args.rename and args.rename_sha256:
        raise RuntimeError("Cannot use --rename together with --rename-sha256.")
    
    if args.rename:
        rename = RenameMode.COUNT
    elif args.rename_sha256:
        rename = RenameMode.SHA256
    else:
        rename = RenameMode.DEFAULT
    
    print("="*5 + "build_dataset start" + "="*5)
    original_stdout = sys.stdout
    sys.stdout = PrefixWriter(sys.stdout, "[lib:build_dataset:main] ")
    lib_build_dataset_main(
        out=str(args.output),
        verbose=bool(args.verbose),
        recursive=not args.no_recursive,
        rename=rename,
        format=args.output_format,
        threads=int(args.threads),
        folders=list(args.input)
    )
    sys.stdout = original_stdout
    print("="*5 + "build_dataset end" + "="*5)
    print("!! You may leave. !!")


# pyrefly: ignore [explicit-any]
def _index(input: list[str], output: str, recursive: bool, hash_func: Callable[[bytes], Any], hash_name: str, threads: int = 0, verbose: bool = True, phash_bits: int = 64) -> None:
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
                },
                "hash": hash_name
            }
        ))
    
    numOffile = 0
    
    print(f"syncro in {', '.join(input)}...")
    for _ in sourceGen(input, recursive=recursive):
            if _.is_file():
                numOffile += 1
    
    counterFiles = itertools.count()
    
    for file in tqdm(
        sourceGen(input, recursive=recursive),
        total=numOffile,
        desc="index",
        dynamic_ncols=True,
        smoothing=0.05,
        mininterval=0.5,
        miniters=1
        ):
        outJson = f"{os.path.join(base, str(next(counterFiles)))}.json"
        outJsonTemp = f"{outJson}.temp"
        if not os.path.isfile(outJson):
            with open(file, "rb") as infile:
                if verbose:
                    tqdm.write(f"[ in  ] : path : '{file}', output : '{outJson}'")
                
                data: bytes = infile.read()
                
                with open(outJsonTemp, "wb") as f:
                    f.write(orjson.dumps(
                        {
                            "phash": str(phash_value(data, hash_size=hash_size)),
                            "hash": hash_func(data).hexdigest(),
                            "size": len(data),
                            "path": str(Path(file).resolve())
                        }
                        )
                    )
                os.replace(outJsonTemp, outJson)

def index_command(args: argparse.Namespace) -> None:
    # args.input: list[str]
    # args.output: str
    # args.threads: int - nos used
    # args.verbose: bool
    # args.phash_bits: int
    # args.no_recursive: bool
    try:
        return _index(
            input=args.input,
            output=args.output,
            recursive=not args.no_recursive,
            threads=args.threads,
            verbose=args.verbos,
            phash_bits=args.phash_bits,
            hash_func=hash_algo[args.hash],
            hash_name=args.hash
        )
    except KeyboardInterrupt:
        return

def jsonloadcache(x: bytes) -> dict[str, str | int]:
    data = orjson.loads(x)
    return {
        "path":    data["path"],
        "phash":   data["phash"],
        "sha256":  data["sha256"],
        "size":    data["size"]
    }

def _phash_live(phash_max_percent: float, phash_min_percent: float, percent: float) -> list[bool | float | int]:
    if percent > 100 or percent < 0:
        raise RuntimeError(f"panic, percent is {percent}")
    
    if percent <= phash_max_percent and percent >= phash_min_percent:
        return [True, percent]
    
    return [False, 0]


    # args.top_k: int
    # args.phash: bool
    # args.input: list[str] | None
    # args.input_cache: str | None
    # args.output: str
    # args.verbose: bool
    # args.steam: bool
    # args.phash_live: bool
    # args.phash_max_percent: float - default == 0.0
    # args.phash_min_percent: float - default == 0.0
    # args.phash_bits: int
    # args.no_recursive: bool


def _duplicates_command_worker() -> None: pass
def _duplicates_command(
    top_k: int,
    input: list[str] | None,
    input_cache: str | None,
    output: str,
    verbose: bool,
    stream: bool,
    phash_live: bool,
    phash_max_percent: float,
    phash_min_percent: float,
    phash_bits: int,
    no_recursive: bool
) -> None:
    tmp = None
    try:
        if bool(input) == bool(input_cache):
            raise RuntimeError("You have specified both --input and --input-cache, which cannot work together.")
        
        if phash_max_percent == 0.0 and phash_live:
            raise RuntimeError("--phash-live defined without --phash-max-percent")
        
        if (not phash_live) and phash_max_percent != 0.0:
            raise RuntimeError("--phash-max-percent defined without --phash-live")
        
        if no_recursive and (not input):
            raise RuntimeError("--no--recursive requires --input")
        
        
        if input:
            print("Build the index files...")
            tmp = tempfile.TemporaryDirectory()
            _index(
                input=input,
                output=tmp.name,
                recursive=not no_recursive,
                phash_bits=phash_bits,
                hash_name=hash_default,
                hash_func=hash_algo[hash_default]
            )
            _input = os.path.join(tmp.name, __version__)
        else:
            _input = str(input_cache)
        
        start_time = time.monotonic()
        
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
        
        if not stream:
            
            hashes: dict[str, ImageHash] = {}
            
            pbar = tqdm(
                (file for folder in input for file in Path(folder).rglob('*.json')),
                total=numOffile,
                desc="duplicates",
                dynamic_ncols=True,
                smoothing=0.05,
                mininterval=0.5,
                miniters=1
            )
            
            for file in pbar:
                with open(file, "rb") as _f:
                    _data = _f.read()
                data = jsonloadcache(_data)
                
                
                path = str(data["path"])
                phash: ImageHash = imagehash.hex_to_hash(data["phash"])
                
                for old_path, old_phash in hashes.items():
                    
                    _counter: int = next(counter)
                    pbar.set_postfix(memory_rss=_getMemoryAlloc(interval=2))
                    
                    distance = phash - old_phash
                    
                    if phash_live:
                        temp = _phash_live(phash_max_percent, phash_min_percent, percent = distance/phash_bits * 100)
                        if temp[0]:
                            tqdm.write(f"phash-live : {path} - {old_path} : {temp[1]}% diff")
                    
                    item = {
                        "path1": old_path,
                        "path2": path,
                        "distance": distance,
                    }
                    
                    entry = (-distance, _counter, item)
                    
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
                with open(file, "rb") as _f:
                    _data = _f.read()
                data = jsonloadcache(_data)
                
                path = str(data["path"])
                phash = imagehash.hex_to_hash(data["phash"])
                
                for old_file in file_stream():
                    pbar.update(1)
                    if old_file == file:
                        continue
                    
                    with open(old_file, "rb") as f:
                        old_data_bin = f.read()
                    old_data = jsonloadcache(old_data_bin)
                    
                    old_path = str(old_data["path"])
                    old_phash = imagehash.hex_to_hash(old_data["phash"])
                    
                    distance = phash - old_phash
                    
                    if phash_live:
                        temp = _phash_live(phash_max_percent, phash_min_percent, percent = distance/phash_bits * 100)
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
        pbar.close()
        total_time = time.monotonic() - start_time
        comparisons.sort(key=lambda x: x[2]["distance"])
        Num0 = 0
        Num25 = 0
        Num50 = 0
        Num75 = 0
        for _, _, item in comparisons:
            percent = item['distance']/phash_bits * 100
            if percent <= 0:
                Num0 += 1
            elif percent <= 25:
                Num25 += 1
            elif percent <= 50:
                Num50 += 1
            elif percent <= 75:
                Num75 += 1
            print(
                f"k-top : {item['path1']} - {item['path2']} : {percent}% diff"
            )
        print("="*5 + " Summary " + "="*5)
        print(f"  0% : {Num0}")
        print(f"  25% : {Num25}")
        print(f"  50% : {Num50}")
        print(f"  75% : {Num75}")
        print(f"total time (no index) : {total_time}")
        print("="*19)
    except KeyboardInterrupt:
        pass
    finally:
        del tmp

def duplicates_command(args: argparse.Namespace) -> None:
    # args.top_k: int
    # args.input: list[str] | None
    # args.input_cache: str | None
    # args.output: str
    # args.verbose: bool
    # args.stream: bool
    # args.phash_live: bool
    # args.phash_max_percent: float - default == 0.0
    # args.phash_min_percent: float - default == 0.0
    # args.phash_bits: int
    # args.no_recursive: bool
    _duplicates_command(
        top_k=args.top_k,
        input=args.input,
        input_cache=args.input_cache,
        output=args.output,
        verbose=args.verbose,
        stream=args.stream,
        phash_live=args.phash_live,
        phash_max_percent=args.phash_max_percent,
        phash_min_percent=args.phash_min_percent,
        phash_bits=args.phash_bits,
        no_recursive=args.no_recursive
    )



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
    _help_no_recursive = "Do not scan subdirectories recursively"
    
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
    parser.add_argument(
        "--verbose",
        help="",
        action="store_true"
    )
    parser.add_argument(
        "--max-memory",
        help="K, M, G, T",
        type=str,
        default=None
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
        "--threads",
        help="Num of threads.",
        type=int,
        default=0
    )
    export_parser.add_argument(
        "--no-recursive",
        action="store_true",
        help=_help_no_recursive
    )
    export_parser.add_argument(
        "--rename",
        action="store_true",
        help="Rename exported files using unique filenames instead of preserving the original source filenames."
    )
    export_parser.add_argument(
        "--rename-sha256",
        action="store_true",
        help="Rename exported files using their SHA-256 hash instead of preserving the original source filenames."
    )
    export_parser.add_argument(
        "--output-format",
        help="Specify the output format (jpg, jpeg, png...)."
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
        "--no-recursive",
        action="store_true",
        help=_help_no_recursive
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
    index_parser.add_argument(
        "--hash",
        choices=list(hash_algo.keys()),
        default=hash_default
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
        help="Folders of the datasets.",
        default=None
    )
    duplicates_parser.add_argument(
        "--input-cache",
        help="Folder of the cached json.",
        type=str,
        default=None
    )
    #duplicates_parser.add_argument(
    #    "--phash",
    #    action="store_true"
    #)
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
        action="store_true",
        help="Allows for O(k) memory usage at the cost of n(n - 1) comparisons; used on massive datasets."
    )
    duplicates_parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Requires `--input` to operate; " + _help_no_recursive
    )
    duplicates_parser.set_defaults(
        func=duplicates_command
    )
    # == benchmark ==
    benchmark_parser = subparsers.add_parser(
        "benchmark",
        help="",
        allow_abbrev=False
    )
    benchmark_parser.set_defaults(
        func=benchmark_command
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
    
    if not args.max_memory is None:
        threading.Thread(
            target=_ThreadSecureMemory,
            args=(parse_size(args.max_memory),),
            daemon=True
        ).start()
    
    args.func(args)


if __name__ == "__main__":
    main()