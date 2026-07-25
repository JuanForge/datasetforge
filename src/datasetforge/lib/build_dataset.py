import os
import signal
import time
import traceback
from multiprocessing import Process, Queue
from multiprocessing.queues import Queue as QueueType
from pathlib import Path
from queue import Empty as queue_Empty
from typing import Any

from setproctitle import setproctitle


from tqdm import tqdm

from datasetforge.lib import png
from datasetforge.lib.ext import ext


def unit(queue_in: QueueType[dict[str, bytes]], queue_out: QueueType[dict[str, bytes | Any]], mode: int) -> None:
    """
    pass2png_watermark = 0,
    copy = 1
    """
    entry = {}
    try:
        def default(x: bytes) -> bytes: return x
        
        if mode == 0:
            algo = png.encode
        elif mode == 1:
            algo = default
        else:
            raise RuntimeError("invalide mode")
        
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
                    "bytes": algo(entry["bytes"])
                }
            )
    except Exception as e:
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




def main(
        mode: int,
        out: str,
        verbose: bool,
        folders: list[str],
        in_type: str = "*.jpg",
        threads: int = os.cpu_count() or 1
    ) -> None:
    
    setproctitle("build_datasets-main")
    
    workers: list[Process] = []
    print(f"[ init ] : num_workers = '{threads}'")
    
    queue_in: QueueType[dict[str, str | bytes] | None] = Queue(maxsize=threads)
    queue_out: QueueType[dict[str, str | bytes | bool]] = Queue()
    
    print(f"out : {out}")
    os.makedirs(out, exist_ok=True)
    
    numOffile = 0
    print(f"syncro in {', '.join(folders)}...")
    for folder in folders:
        for _ in Path(folder).rglob(in_type):
            if _.is_file():
                numOffile += 1
    
    
    for _ in range(threads):
        p = Process(
            target=unit,
            args=(queue_in, queue_out, mode)
        )
        p.start()
        workers.append(p)
    
    try:
        for file in tqdm(
            (file for folder in folders for file in Path(folder).rglob(in_type)),
            total=numOffile,
            desc="build",
            dynamic_ncols=True,
            smoothing=0.05,
            mininterval=0.5,
            miniters=1
        ):
            if not os.path.isfile(f"{os.path.join(out, file.stem)}.{ext(mode, file)}"):
                with open(file, "rb") as infile:
                    if verbose:
                        tqdm.write(f"[ in  ] : path : '{file}', output : '{os.path.join(out, file.stem)}.{ext(mode, file)}'")
                    queue_in.put({"path": f"{os.path.join(out, file.stem)}.{ext(mode, file)}", "bytes": infile.read()})
            
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