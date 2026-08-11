import hashlib
import itertools
import os
import signal
import time
import traceback
from enum import StrEnum
from multiprocessing import Process, Queue
from multiprocessing.queues import Queue as QueueType
from queue import Empty as queue_Empty
from typing import Any

from setproctitle import setproctitle
from tqdm import tqdm

from datasetforge.lib import format as formatLib
from datasetforge.lib.source import sourceGen


# pyrefly: ignore [explicit-any]
def unit(queue_in: QueueType[dict[str, bytes]], queue_out: QueueType[dict[str, bytes | Any]], format: str|None) -> None:
    """
    pass2png_watermark = 0,
    copy = 1
    """
    entry = {}
    try:
        def default(x: bytes, format: str) -> bytes: return x
        
        if format is None:
            algo = default
        else:
            algo = formatLib.encode
        
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        setproctitle("build_datasets-worker")
        while True:
            entry: dict[str, bytes]|None = queue_in.get()
            
            if entry is None:
                #queue_out.cancel_join_thread()
                return
            queue_out.put(
                {
                    "path": entry["path"],
                    "bytes": algo(entry["bytes"], format=str(format))
                }
            )
    except Exception as e:  # noqa: BLE001
        if type(entry) is dict:
            path = entry.get("path")
        else:
            path = None
        
        queue_out.put({
            "error_bool": True,
            "path": path,
            "error_type": type(e).__name__,
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        return


# pyrefly: ignore [explicit-any]
def write_queue(queue_out: QueueType[dict[str, bytes | Any]], printlog: bool) -> None:
    while True:
        try:
            entry: dict[str, bytes] = queue_out.get(block=False)
            if not entry.get("error_bool"):
                if printlog:
                    tqdm.write(f"[ out ] : path : '{entry["path"]}'")
                
                with open(entry["path"], "wb") as fileout:
                    fileout.write(entry["bytes"])
            else:
                tqdm.write("="*5 + "ERROR IN WORKERS" + "="*5)
                tqdm.write("="*20)
                tqdm.write(str(entry["traceback"]))
                tqdm.write("="*20)
                raise RuntimeError(
                    f"{entry["path"]} > {entry['error_type']} : {entry['error']}"
                )
        except queue_Empty:
            break


class RenameMode(StrEnum):
    DEFAULT = "default"
    COUNT = "count"
    SHA256 = "sha256"

def main(
        out: str,
        verbose: bool,
        folders: list[str],
        recursive: bool,
        rename: RenameMode,
        format: str|None = None,
        threads: int = 0,
    ) -> None:
    
    if threads == 0:
        threads = os.cpu_count() or 1
    
    GenName = itertools.count()
    
    setproctitle("build_datasets-main")
    
    workers: list[Process] = []
    print(f"[ init ] : num_workers = '{threads}'")
    
    queue_in: QueueType[dict[str, str | bytes] | None] = Queue(maxsize=threads)
    queue_out: QueueType[dict[str, str | bytes | bool]] = Queue()
    
    print(f"out : {out}")
    os.makedirs(out, exist_ok=True)
    
    numOffile = 0
    print(f"syncro in {', '.join(folders)}...")
    
    for _ in sourceGen(folders, recursive=recursive, exclude_dirs=None):
        if _.is_file():
            numOffile += 1
    
    for _ in range(threads):
        p = Process(
            target=unit,
            args=(queue_in, queue_out, format)
        )
        p.start()
        workers.append(p)
    
    try:
        for file in tqdm(
            sourceGen(folders, recursive=recursive),
            total=numOffile,
            desc="build",
            dynamic_ncols=True,
            smoothing=0.05,
            mininterval=0.5,
            miniters=1
        ):
            _cachedFile: bytes|bool = False
            
            if rename is RenameMode.COUNT:
                outfileName: str = str(next(GenName))
            
            elif rename is RenameMode.DEFAULT:
                outfileName: str = file.stem
            
            elif rename is RenameMode.SHA256:
                with open(file, "rb") as f:
                    _cachedFile = f.read()
                outfileName = hashlib.sha256(_cachedFile).hexdigest()
            
            else:
                raise RuntimeError(f"invalide rename : {rename}")
            
            if format is None:
                _format: str = file.suffix.replace(".", "")
            else:
                _format = format
            
            outfile: str = f"{os.path.join(out, outfileName)}.{_format}"
            
            if not os.path.isfile(f"{outfile}"):
                if type(_cachedFile) is not bytes:
                    with open(file, "rb") as infile:
                        _cachedFile = infile.read()
                
                if verbose:
                    tqdm.write(f"[ in  ] : path : '{file}', output : '{outfile}'")
                queue_in.put({"path": f"{outfile}", "bytes": _cachedFile})
            else:
                tqdm.write(f"double surname : '{file}'")
            
            write_queue(queue_out, verbose)
    
    except KeyboardInterrupt:
        pass
    finally:
        print("exit...")
        
        for _ in workers:
            queue_in.put(None)
        
        while any(process.is_alive() for process in workers):
            write_queue(queue_out, verbose)
            time.sleep(0.1)
        
        for process in workers:
            process.join()
        
        write_queue(queue_out, verbose)