import argparse
import hashlib
import heapq
import itertools
import json
import math
import multiprocessing
import os
import sys
import threading
import time
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import humanize
import imagehash
import numpy
import orjson
import psutil
from imagehash import ImageHash
from pympler import asizeof
from send2trash import send2trash
from tqdm import tqdm

from datasetforge import __version__  # noqa: F401
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
def _index(input: list[str], exclude_dir: list[str] | None, output: str, recursive: bool, hash_func: Callable[[bytes], Any], hash_name: str, threads: int = 0, verbose: bool = True, phash_bits: int = 64) -> list[str]:
    pbar: None | tqdm[Any] = None
    try:
        results: list[str] = []
        hash_size: int = math.isqrt(phash_bits)
        print(f"hash size : {hash_size}")
        
        if hash_size * hash_size != phash_bits:
            raise RuntimeError(
                "--phash-bits must be a perfect square (64, 256, 1024, ...)"
        )
        
        base = os.path.join(output, "__unit__")
        os.makedirs(base, exist_ok=True)
        
        with open(os.path.join(output, "META.json"), "w", encoding="utf-8") as meta:
            meta.write(json.dumps(
                {
                    "phash": {
                        "bits": phash_bits
                    },
                    "hash": hash_name
                }
            ))
        
        numOffile = 0
        
        start_time = time.monotonic()
        print(f"syncro in {', '.join(input)}...", end='', flush=True)
        for _ in sourceGen(input, recursive=recursive, exclude_dirs=exclude_dir):
                numOffile += 1
        print(f"done | {time.monotonic() - start_time}")
        
        
        total: int = 0
        # TheCache = f"{os.path.join(base, "__cache__")}.jsonl"
        
        pbar = tqdm(
                sourceGen(input, recursive=recursive, exclude_dirs=exclude_dir),
                total=numOffile,
                desc="index",
                dynamic_ncols=True,
                smoothing=0.05,
                mininterval=0.5,
                miniters=1
        )
        it = iter(pbar)
        
        with Multicore(
            func=_index_worker,
            worker_kwargs={"verbose": verbose, "hash_size": hash_size, "hash_func": hash_func, "output": base},
            core=threads or os.cpu_count() or 1,
            timeout=2,
            _dev=True
        ) as core:
            while total < numOffile:
                try:
                    core.put(
                        file=str(next(it))
                    )
                except StopIteration: pass
                
                for _ in core.get():
                    results.append(_)
                    total += 1
        return results
    except KeyboardInterrupt:
        if pbar:
            pbar.close()
        raise

# pyrefly: ignore [explicit-any]
def _index_worker(file: str, verbose: bool, hash_size: int, hash_func: Callable[[bytes], Any], output: str) -> str:
    st_mtime_ns = Path(file).stat().st_mtime_ns
    out_file = os.path.join(output, file.lstrip("/"))
    out_file_json = f"{out_file}-{st_mtime_ns}.json"
    
    if not os.path.isfile(out_file_json):
        with open(file, "rb") as f:
            data: bytes = f.read()
            if verbose:
                tqdm.write(f"[ in  ] : path : '{file}'")
        
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        
        with open(f"{out_file_json}.tmp", "wb") as f:
            f.write(orjson.dumps(
                    {
                        "phash": str(phash_value(data, hash_size=hash_size)),
                        "hash": hash_func(data).hexdigest(),
                        "size": len(data),
                        "path": str(Path(file).resolve())
                    }
                ))
        os.replace(f"{out_file_json}.tmp", out_file_json)
    return out_file_json

"""
def index_command(args: argparse.Namespace) -> None:
    # args.input: list[str]
    # args.output: str
    # args.threads: int - nos used
    # args.verbose: bool
    # args.phash_bits: int
    # args.no_recursive: bool
    try:
        _index(
            input=args.input,
            output=args.output,
            recursive=not args.no_recursive,
            threads=args.threads,
            verbose=args.verbose,
            phash_bits=args.phash_bits,
            hash_func=hash_algo[args.hash],
            hash_name=args.hash
        )
    except KeyboardInterrupt:
        return
"""

"""
def jsonloadcache(x: bytes) -> dict[str, str | int]:
    data = orjson.loads(x)
    return {
        "path":    data["path"],
        "phash":   data["phash"],
        "hash":    data["hash"],
        "size":    data["size"]
    }
"""
"""
def _phash_live(phash_max_percent: float, phash_min_percent: float, percent: float) -> list[bool | float | int]:
    if percent > 100 or percent < 0:
        raise RuntimeError(f"panic, percent is {percent}")
    
    if percent <= phash_max_percent and percent >= phash_min_percent:
        return [True, percent]
    
    return [False, 0]
"""

def _duplicates_command(
    input: list[str],
    exclude_dir: list[str] | None,
    top_k: int | None,
    hash_value: str,
    no_system_cache: bool,
    phash_optimizer: Literal["default", "numpy", "xor"],
    verbose: bool,
    phash_live: bool,
    phash_low_log: bool,
    phash_max_percent: float | None,
    phash_min_percent: float | None,
    phash_bits: int,
    no_recursive: bool,
    allow_rm: bool,
    rm_allowed_dirs: list[str] | list[Path] | None,
    threads: int = os.cpu_count() or 1,
) -> None:
    _dev_permit_rm: bool = False
    start_time_task = None
    
    if not rm_allowed_dirs is None:
        _rm_allowed_dirs: list[Path] = []
        for _ in rm_allowed_dirs:
            if isinstance(_, Path):
                _rm_allowed_dirs.append(_)
            else:
                _rm_allowed_dirs.append(Path(_))
        rm_allowed_dirs = _rm_allowed_dirs
    
    phash_local_xor = phash_local_numpy = None
    
    if phash_optimizer == "default":
        pass
    elif phash_optimizer == "numpy":
        phash_local_numpy = True
    elif phash_optimizer == "xor":
        phash_local_xor = True
    else:
        raise RuntimeError("invalid value for the optimizer")
    
    hash_func = hash_algo[hash_value]
    
    try:
        if phash_live and ( phash_min_percent is None or phash_max_percent is None ):
            raise RuntimeError("--phash-live without defining --phash-min-percent and/or --phash-max-percent.")
        
        if (not phash_max_percent ) is None and phash_live:
            raise RuntimeError("--phash-live defined without --phash-max-percent")
        
        if phash_max_percent and (not phash_live):
            raise RuntimeError("--phash-max-percent defined without --phash-live")
        
        if no_recursive and (not input):
            raise RuntimeError("--no-recursive requires --input")
        
        if rm_allowed_dirs and len(rm_allowed_dirs) >= 1 and (not allow_rm):
            raise RuntimeError("--rm-allowed-dirs requires --allow-rm.")
        
        if not no_system_cache:
            _env = os.environ.get("XDG_CACHE_HOME")
            if type(_env) is str:
                name_base = _env
            else:
                name_base = os.path.expanduser("~/.cache")
        else:
            name_base = "/tmp/"
        
        name = os.path.join(name_base, "datasetforge", hashlib.sha256(f"{phash_bits}{hash_value}".encode()).hexdigest())
        del name_base
        tqdm.write(f"cache used : {name}")
        
        liste: list[str] = _index(
            input=input,
            exclude_dir=exclude_dir,
            output=name,
            recursive=not no_recursive,
            phash_bits=phash_bits,
            hash_name=hash_value,
            hash_func=hash_func,
            verbose=verbose,
            threads=threads
        )
        _input = name
        
        META: dict[str, str | int] = {}
        with open(os.path.join(_input, "META.json"), "r", encoding="utf-8") as meta:
            META["phash_bits"] = json.loads(meta.read())["phash"]["bits"]
        
        # _start_time = time.monotonic()
        # print("syncro...", end='', flush=True)
        # TheCache = []
        # with open(os.path.join(_input, "__unit__", "__cache__.jsonl"), "rb") as f:
        #     for line in f:
        #         TheCache.append(orjson.loads(line))
        #liste: list[Path] = list(Path(os.path.join(_input, "__unit__")).rglob('*.json'))
        #  print(f"done | {time.monotonic() - _start_time}")
        
        hashes: list[dict[str, str | ImageHash | int]] = []
        
        _start_time = time.monotonic()
        print("loading cache file...", end='', flush=True)
        for entry in liste:
            with open(entry, "rb") as f:
                entry = orjson.loads(f.read())
            if phash_local_xor:
                _phash = int(entry["phash"], 16)
            else:
                _phash = imagehash.hex_to_hash(entry["phash"])
            
            hashes.append(
                {
                    "path": str(entry["path"]),
                    "phash": _phash
                }
            )
        print(f"done | {time.monotonic() - _start_time}")
        print(f"total : {len(hashes)}")
        
        def _duplicates_command_worker(
            i: list[int],
            phash_low_log: bool,
            phash_local_numpy: bool,
            phash_local_xor: bool,
            top_k: int| None,
            phash_bits: int,
            phash_live: bool,
            phash_min_percent: float,
            phash_max_percent: float,
            allow_rm: bool,
            rm_allowed_dirs: list[Path] | None,
            _hashes: list[dict[str, str | ImageHash | int]] | None = None
        ) -> dict[str, int | str | list[Any]]:
            
            phash_live_trace: list[str] = []
            phash_live_return: list[dict[str, int | str | list[str]]] = []
            if _hashes is None:
                hashes_local = hashes
            else:
                hashes_local = _hashes
            
            _start_time = time.monotonic()
            top_comparisons = []
            counter = itertools.count()
            
            for _i in i:
                current_hash = hashes_local[_i]
                
                current_path = str(current_hash["path"])
                current_phash = current_hash["phash"]
                
                # current_path, current_phash_hex = hashes_local[i]
                # 
                # current_phash = imagehash.hex_to_hash(current_phash_hex)
                # 
                # for j in range(i + 1, len(hashes_local)):
                #     old_path, old_phash_hex = hashes_local[j]
                # 
                #     old_phash = imagehash.hex_to_hash(old_phash_hex)
                # 
                #     distance = current_phash - old_phash
                
                for old_hash in hashes_local[_i + 1:]:
                    # pyrefly: ignore [bad-assignment]
                    old_path: str = old_hash["path"]
                    # old_phash = imagehash.hex_to_hash(old_hash["phash"]) # dev
                    # pyrefly: ignore [bad-assignment]
                    old_phash: ImageHash = old_hash["phash"]
                    
                    # distance = (current_phash ^ old_phash).bit_count()
                    if phash_local_numpy:
                        # assert current_phash.hash.shape == old_phash.hash.shape
                        # pyrefly: ignore [missing-attribute]
                        distance = numpy.count_nonzero(current_phash.hash != old_phash.hash)
                    elif phash_local_xor:
                        # pyrefly: ignore [unsupported-operation]
                        distance = (current_phash ^ old_phash).bit_count()
                    else:
                        # pyrefly: ignore [unsupported-operation]
                        distance = current_phash - old_phash
                    
                    if phash_live:
                        percent = distance / phash_bits * 100
                        if percent <= phash_max_percent and percent >= phash_min_percent:
                            if allow_rm and rm_allowed_dirs and len(rm_allowed_dirs) >= 1:
                                for permit_folder in rm_allowed_dirs:
                                    
                                    old_path_obj = Path(old_path)
                                    current_path_obj = Path(current_path)
                                    
                                    if old_path_obj.resolve().is_relative_to(permit_folder.resolve()):
                                        rm_file = old_path
                                    elif current_path_obj.resolve().is_relative_to(permit_folder.resolve()):
                                        rm_file = current_path
                                    else:
                                        rm_file = None
                                    if rm_file:
                                        rm_file = Path(rm_file)
                                        if not phash_low_log:
                                            phash_live_trace.append(f"\033[32mfound : {old_path} = {current_path}\033[0m")
                                            phash_live_trace.append(f"\033[32mvalid rm : {rm_file}\033[0m")
                                        
                                        if _dev_permit_rm:
                                            try:
                                                send2trash(rm_file)
                                            except FileNotFoundError:
                                                if not phash_low_log:
                                                    phash_live_trace.append(f"FileNotFoundError : {rm_file}")
                                        break
                            if not phash_low_log:
                                phash_live_return.append(
                                    {"write": f"phash-live : {old_path} - {current_path} : {percent}", "trace": phash_live_trace}
                                )
                            #return {"type": 0, "write": f"phash-live : {old_path} - {current_path} : {percent}", "trace": trace} # ne pas faire ruturn car casse dès i > 1, faire une liste de result puis resuilt final type list[]; exploité phash_live_list
                    comparison = {
                        "path1": current_path,
                        "path2": old_path,
                        "distance": distance,
                    }
                    
                    heap_entry = (
                        -distance,
                        next(counter),
                        comparison,
                    )
                    
                    if top_k:
                        if len(top_comparisons) < top_k:
                            heapq.heappush(top_comparisons, heap_entry)
                        
                        elif distance < -top_comparisons[0][0]:
                            heapq.heapreplace(top_comparisons, heap_entry)
            if phash_live:
                return {"type": 0, "results": phash_live_return}
            results = []
            
            for _, _, comparison in top_comparisons:
                results.append(comparison)
            
            return {"type": 1, "results": results}
        
        #multiprocessing.set_start_method("spawn")
        start_time_task = time.monotonic()
        with Multicore(
            func=_duplicates_command_worker,
            core=threads,
            timeout=2,
            _dev=True,
            worker_kwargs={
                "phash_local_xor": phash_local_xor,
                "phash_local_numpy": phash_local_numpy,
                "phash_low_log": phash_low_log,
                "_hashes": None if multiprocessing.get_start_method() == "fork" else hashes,
                "top_k": top_k,
                "phash_bits": META["phash_bits"],
                "phash_live": phash_live,
                "phash_min_percent": phash_min_percent,
                "phash_max_percent": phash_max_percent,
                "allow_rm": allow_rm, 
                "rm_allowed_dirs": rm_allowed_dirs
            }
        ) as core:
            # tqdm.write(str(core.workers[0].pid))
            # time.sleep(10)
            
            final = []
            counter = itertools.count()
            
            task = iter(range(len(hashes)))
            
            numWorker = 1
            
            pbar = tqdm(
                total=len(hashes),
                desc="duplicates",
                dynamic_ncols=True,
                smoothing=0.05,
                mininterval=0.5,
                miniters=1
            )
            stats = tqdm(
                total=0,
                position=1,
                bar_format="{desc}",
                dynamic_ncols=True
            )
            stats_time = 0.0
            
            worker_bars = [
                tqdm(
                    total=0,
                    position=i + numWorker + 1,
                    bar_format="{desc}"
                )
                for i in range(threads)
            ]
            
            finished = 0
            timeout_get: float | None = None
            try:
                estimate_chunk = 1
                process_main = psutil.Process(os.getpid())
                process_main.cpu_percent()
                # cpu_percent_timer = time.monotonic()
                total = len(hashes)
                locked_total = total
                _debug_int = 0
                while finished < total: # num != core_count
                    
                    # remaining = len(hashes) - finished
                    # estimate_chunk = max(
                    #     1,
                    #     int(
                    #         remaining
                    #         / (threads * 4)
                    #         * math.sqrt(max(top_k, 1) / 100)
                    #     )
                    # )
                    
                    # if (time.monotonic() - cpu_percent_timer ) > 2:
                    #     cpu_percent = process_main.cpu_percent()
                    #     if cpu_percent > 10:
                    #         estimate_chunk += min(max(1, int(cpu_percent / 10)), 5)
                    #     else:
                    #         estimate_chunk -= 1
                    #     
                    #     estimate_chunk = max(1, estimate_chunk)
                    #     cpu_percent_timer = time.monotonic()
                    force = 7
                    if top_k:
                        estimate_chunk = max(
                            1,
                            int((top_k / 50) / (max(1, finished / total * 100) / force)))
                    elif phash_live:
                        estimate_chunk = int(max(1, (100 - (finished / total) * 100) )) # 100 % - 0%
                        estimate_chunk = int(max(1, estimate_chunk / (locked_total / 2000)))
                    
                    if (time.monotonic() - stats_time) >= 2:
                        stats.set_description_str(
                            f'USS workers : {core.workers_memory_usage_uss() / (1024 * 1024):.4f} MiB | '
                            f'USS main : {psutil.Process(os.getpid()).memory_full_info().uss / (1024 * 1024):.4f} MiB | '
                            f'RSS main : {psutil.Process(os.getpid()).memory_full_info().rss / (1024 * 1024):.4f} MiB | '
                            f'PSS+main : {core.workers_memory_usage_pss(include_main=True) / (1024 * 1024):.4f} MiB   | '
                            f"Queue_input : {core._input_Queue.qsize()} | "
                            f"Queue_output : {core._output_Queue.qsize()} | "
                            f"chunk : {estimate_chunk} | "
                            f"CPU : {process_main.cpu_percent()} % | "
                            f"remaining tasks : {'no' if bool(timeout_get) else 'yes'}"
                        )
                        for index, process in enumerate(core.get_workers()):
                            memory = psutil.Process(process.pid).memory_full_info()
                            hu = humanize.naturalsize
                            worker_bars[index].set_description_str(f"[{index}] : USS {hu(memory.uss, binary=True)} | RSS {hu(memory.rss, binary=True)}")
                        stats_time = time.monotonic()
                    
                    pbar.set_postfix(memory_rss=_getMemoryAlloc(interval=2), refresh=False)
                    
                    
                    chunk: list[int] = []
                    try:
                        for _ in range(estimate_chunk):
                            chunk.append(next(task))
                    except StopIteration:
                        timeout_get = 0.5
                    
                    if len(chunk) >= 1:
                        core.put(i=chunk)
                        if len(chunk) > 1:
                            total -= (len(chunk) - 1)
                    pbar.total = total
                    
                    for result in core.get(timeout=timeout_get):
                        pbar.update()
                        finished += 1
                        if result["type"] == 1 and type(top_k) is int:
                            for item in result["results"]:
                                distance = item["distance"]
                                entry = (
                                    -distance,
                                    next(counter),
                                    item
                                )
                                
                                if len(final) < top_k:
                                    heapq.heappush(final, entry)
                                
                                elif distance < -final[0][0]:
                                    heapq.heapreplace(final, entry)
                        elif result["type"] == 0:
                            for _result in result["results"]:
                                if _result.get("write"):
                                    tqdm.write(_result["write"])
                                if _result.get("trace"):
                                    for _trace in _result["trace"]:
                                        tqdm.write(_trace)
                                _debug_int += 1
                            #if result["trace"]:
                            #    for _trace in result["trace"]:
                            #        tqdm.write(str(_trace) + "\n")
                        else:
                            raise RuntimeError()
                pbar.close()
                tqdm.write("exit")
            except (KeyboardInterrupt, StopIteration): pass
            pbar.close()
            stats.close()
            for _ in worker_bars:
                _.close()
            
            # pyrefly: ignore [implicit-any-lambda]
            final.sort(key=lambda x: x[2]["distance"], reverse=True)
            
            cat: dict[int, int]  = {}
            for _ in range(0, 100, 10):
                cat[_] = 0
            
            for _, _, item in final:
                percent = item['distance']/META["phash_bits"] * 100
                for cat_percent in cat.copy():
                    if percent <= cat_percent:
                        cat[cat_percent] += 1
                        break
                
                print(
                    f"k-top : '{item['path1']}' - '{item['path2']}' : {percent}% diff"
                )
            
            print("="*5 + " Summary " + "="*5)
            for percent, number in cat.items():
                print(f"   {percent}% : {number}")
            print("="*19)
    except KeyboardInterrupt:
        print("CTRL+C")
    except Exception:
        raise
    finally:
        """
        with open(cache_distance_file, "wb") as f:
            f.write(orjson.dumps(cache_distance))
        """
        if start_time_task:
            print(time.monotonic() - start_time_task)

def duplicates_command(args: argparse.Namespace) -> None:
    _duplicates_command(
        input=args.input,
        exclude_dir=args.exclude_dir,
        top_k=args.top_k,
        hash_value=args.hash,
        phash_optimizer=args.phash_optimizer,
        no_system_cache=args.no_system_cache,
        verbose=args.verbose,
        phash_live=args.phash_live,
        phash_low_log=args.phash_low_log,
        phash_max_percent=args.phash_max_percent,
        phash_min_percent=args.phash_min_percent,
        phash_bits=args.phash_bits,
        no_recursive=args.no_recursive,
        threads=args.threads,
        allow_rm=args.allow_rm,
        rm_allowed_dirs=args.rm_allowed_dirs
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
        help="Maximum memory available to DatasetForge (e.g. 1024K, 1024M, 16G, 1T).",
        type=str,
        default=None
    )
    parser.add_argument(
        "--threads",
        help="Number of worker threads used for dataset processing.",
        type=int,
        default=os.cpu_count() or 1
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
        "--exclude-dir",
        nargs="+",
        default=None,
        help="""
        Exclude directories located inside --input directories.
        Useful for excluding subdirectories of an input directory."
        """
    )
    #duplicates_parser.add_argument(
    #    "--phash",
    #    action="store_true"
    #)
    duplicates_parser.add_argument(
        "--hash",
        choices=list(hash_algo.keys()),
        default=hash_default
    )
    duplicates_parser.add_argument(
        "--phash-bits",
        help='Set the pHash bit size. Higher values increase precision and reduce collisions, which is useful for large datasets.',
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
        default=None,
        help=(
            "Maximum pHash difference percentage to display. Must be used together "
            "with --phash-live. Displays every file pair with a pHash difference "
            "less than or equal to the specified percentage."
        )
    )
    duplicates_parser.add_argument(
        "--phash-min-percent",
        type=float,
        default=None,
        help=(
            "Minimum pHash difference percentage to display. Must be used together "
            "with --phash-live. Displays every file pair with a pHash difference "
            "greater than or equal to the specified percentage."
        )
    )
    duplicates_parser.add_argument(
        "--top-k",
        type=int,
        default=None,
        help="Return the K results with the lowest pHash distance."
    )
    duplicates_parser.add_argument(
        "--no-recursive",
        action="store_true",
        help=_help_no_recursive
    )
    duplicates_parser.add_argument(
        "--allow-rm",
        action="store_true",
        help="Authorizes deletion actions"
    )
    duplicates_parser.add_argument(
        "--rm-allowed-dirs",
        nargs="+",
        help="""
        Folder(s) where recursive file deletion is allowed.
        Files matching the pHash difference range specified by `--phash-min-percent` and `--phash-max-percent` can be automatically deleted.
        """
    )
    duplicates_parser.add_argument(
        "--phash-low-log",
        action="store_true",
        help=""
    )
    duplicates_parser.add_argument(
        "--no-system-cache",
        action="store_true",
        help="" # "Use the system cache (.cache) instead of /tmp for persistent caching."
    )
    duplicates_parser.add_argument(
        "--phash-optimizer",
        choices=("default", "numpy", "xor"),
        default="default",
        help=(
            "pHash optimization method: "
            "'default' uses the standard Python implementation of ImageHash. "
            "'numpy' Uses a locally patched NumPy implementation for pHash distance calculations without additional safety checks, for up to 3.5x faster. "
            "'xor' Use XOR-based pHash distance calculation for up to 23.5x faster performance on 64-bit processors."
        )
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