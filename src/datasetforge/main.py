import argparse
import hashlib
import heapq
import itertools
import json
import math
import os
import sys
import warnings
from pathlib import Path

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

def index_command(args: argparse.Namespace) -> None:
    # args.input: list[str]
    # args.output: str
    # args.threads: int - nos used
    # args.verbose: bool
    # agrs.phash_bits: int
    hash_size: int = math.isqrt(args.phash_bits)
    print(f"hash size : {hash_size}")
    
    if hash_size * hash_size != args.phash_bits:
        raise ValueError(
            "--phash-bits must be a perfect square (64, 256, 1024, ...)"
    )
    
    base = os.path.join(args.output, __version__, "__unit__")
    os.makedirs(base, exist_ok=True)
    
    with open(os.path.join(args.output, __version__, "META.json"), "w", encoding="utf-8") as meta:
        meta.write(json.dumps(
            {
                "phash": {
                    "bits": args.phash_bits
                }
            }
        ))
    try:
        numOffile = 0
        print(f"syncro in {', '.join(args.input)}...")
        for folder in args.input:
            for _ in Path(folder).rglob(in_type):
                if _.is_file():
                    numOffile += 1
        
        for file in tqdm(
            (file for folder in args.input for file in Path(folder).rglob(in_type)),
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
                    if args.verbose:
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

def jsonloadcache(x: bytes) -> dict[str, str | int]:
    data = orjson.loads(x)
    return {
        "path":    data["path"],
        "phash":   data["phash"],
        "sha256":  data["sha256"],
        "size":    data["size"]
    }

#def top_k_algo(entry: list[ImageHash], top: int) -> list[ImageHash]:
#    pass

def duplicates_command(args: argparse.Namespace) -> None:
    # args.top_k: int
    # args.phash: bool
    # args.input: str
    # args.output: str
    # args.verbose: bool
    try:
        top_k = int(args.top_k)
        if top_k <= 0:
            raise RuntimeError("invalide top-k value")
        
        with open(os.path.join(args.input, "META.json"), "r", encoding="utf-8") as meta:
            phash_bits = json.loads(meta.read())["phash"]["bits"]
        
        args.input = os.path.join(args.input, "__unit__")
        
        numOffile = 0
        input: list[str] = [args.input]
        print(f"syncro in {', '.join(input)}...")
        for folder in input:
            for _ in Path(folder).rglob('*.json'):
                if _.is_file():
                    numOffile += 1
        
        comparisons = []
    
        hashes: dict[str, ImageHash] = {}
        counter = itertools.count()
        
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
        
                item = {
                    "path1": old_path,
                    "path2": path,
                    "distance": distance,
                }
        
                entry = (-distance, next(counter), item)
        
                if len(comparisons) < top_k:
                    heapq.heappush(comparisons, entry)
        
                elif distance < -comparisons[0][0]:
                    heapq.heapreplace(comparisons, entry)
        
            hashes[path] = phash
        
        
        
        comparisons.sort(key=lambda x: x[2]["distance"])
        
        for _, _, item in comparisons:
            print(
                f"{item['path1']} - {item['path2']} : {item['distance']/phash_bits * 100}% diff"
            )
    except KeyboardInterrupt:
        pass


        
        #for key, value in top_k_list.copy().items():
        #    top_k_list[str(data["path"])] = {
        #        "phash": imagehash.hex_to_hash(data["phash"])
        #    }
            #if len(top_k_list) > top_k:
            #    last = 0
            #    for key, value in top_k_list.copy().items():



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
        help="Set the pHash bit size. Higher values increase precision and reduce collisions, which is useful for large datasets.",
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
        required=True,
        help="Folder of the cached json.",
        type=str
    )
    duplicates_parser.add_argument(
        "--phash",
        action="store_true"
    )
    duplicates_parser.add_argument(
        "--top-k",
        type=int,
        default=5
    )
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