import os
import platform
import signal
import time
import traceback
from collections.abc import Callable, Generator
from multiprocessing import Process, Queue
from multiprocessing.queues import Queue as QueueType
from queue import Empty as queue_Empty
from queue import Full as queue_Full
from typing import Any, Self

import psutil
from setproctitle import setproctitle


class errors:
    class workerRunError(Exception):
        pass
    class UnexpectedWorkerExitError(Exception):
        pass


def _worker(
    # pyrefly: ignore [explicit-any]
    func: Callable[..., Any],
    
    # pyrefly: ignore [explicit-any]
    worker_kwargs: dict[str, Any],
    
    # pyrefly: ignore [explicit-any]
    input_Queue: QueueType[dict[Any, Any] | None],
    
    # pyrefly: ignore [explicit-any]
    output_Queue: QueueType[dict[Any, Any]],
    
    dev: bool = False
) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    if platform.system() == "Linux":
        setproctitle(f"worker-{os.getpid()}")
    
    while True:
        try:
            tache = input_Queue.get(block=True)
            if tache == None:
                if dev: print("stop")
                return
            else:
                output_Queue.put(
                    {
                        "error": False,
                        "return": func(
                            *tache["args"],
                            **tache["kwargs"],
                            **worker_kwargs)
                    }
                )
        except BaseException as e:  # noqa: BLE001
            output_Queue.put({"error": True, "raise": e})
            print(traceback.format_exc())


# pyrefly: ignore [explicit-any]
def _clear_queue(queue: QueueType[Any]) -> None:
    while True:
        try:
            queue.get_nowait()
        except queue_Empty:
            break

class Multicore:
    """
    for dev
    - If you plan to use this code, make sure to design your operations as atomic transactions, since the workers may need to be terminated forcefully.
    """
    def __init__(
        self,
        # pyrefly: ignore [explicit-any]
        func: Callable[..., Any],
        # pyrefly: ignore [explicit-any]
        worker_kwargs: None | dict[str, Any] = None,
        core: int = 0,
        input_Queue: int | None = None,
        closeOnError: bool = True,
        timeout: float | None = None,
        _dev: bool = False
    ) -> None:
        core = core or os.cpu_count() or 1
        # pyrefly: ignore [explicit-any]
        self._input_Queue: QueueType[dict[Any, Any] | None] = Queue(maxsize=input_Queue or (int(core * 1.5)))
        # pyrefly: ignore [explicit-any]
        self._output_Queue: QueueType[dict[Any, Any]] = Queue()
        self.func = func
        
        if worker_kwargs is None:
            worker_kwargs = {}
        
        self.workers: list[Process] = []
        
        _timeloadworkers = time.monotonic()
        for _ in range(core):
            p = Process(
                target=_worker,
                args=(
                    func,
                    worker_kwargs,
                    self._input_Queue,
                    self._output_Queue,
                    _dev
                )
            )
            p.start()
            self.workers.append(p)
        self._timeloadworkers = time.monotonic() - _timeloadworkers
        
        self.closed: bool = False
        self.closeOnError = closeOnError
        self.timeout = timeout
        self._sleepStop: float = 0.01
        self._dev = _dev
        self._workers_health_check_interval: float = 0.75
        self._workers_health_check_interval_timer = time.monotonic()
    
    def __enter__(self) -> Self:
        return self
    
    def __exit__(self, *_) -> None:
        if self._dev: print("worker close...")
        self.close()
    
    # pyrefly: ignore [explicit-any]
    def function(self, func: Callable[..., Any]) -> None:
        """update the initial function"""
        self.func = func
    
    def put(
        self, 
        # pyrefly: ignore [explicit-any]
        *args: Any,
        # pyrefly: ignore [explicit-any]
        **kwargs: Any
    ) -> None:
        self.status()
        self._input_Queue.put({
            "args": args,
            "kwargs": kwargs,
        })
    
    # pyrefly: ignore [explicit-any]
    def get(self, block: bool = False, timeout: float|None = None, _status: bool = True) -> list[Any]:
        # pyrefly: ignore [explicit-any]
        out: list[Any] = []
        if _status: self.status()
        if type(timeout) in [float, int]:
            block = True
        while True:
            try:
                data = self._output_Queue.get(block=block, timeout=timeout)
            except queue_Empty:
                break
            
            if data["error"]:
                print("="*5 + "ERROR IN WORKERS" + "="*5)
                if self.closeOnError:
                    self.close()
                raise errors.workerRunError(str(data["raise"]))
            else:
                out.append(data["return"])
        return out
    
    def close(self) -> None:
        if not self.closed:
            start_time = time.monotonic()
            for _ in self.workers:
                try:
                    self._input_Queue.put(None, block=False)
                except queue_Full: pass
            
            #for _ in [self._input_Queue, self._output_Queue]:
            #    _clear_queue(_)
            
            while any(
                worker.is_alive() for worker in self.workers
                ) and (self.timeout is None or (time.monotonic() - start_time) < self.timeout):
                time.sleep(self._sleepStop)
                self.get(_status=False)
            
            for worker in self.workers:
                if worker.is_alive():
                    if self._dev: print("kill !")
                    worker.kill()
                worker.join()
            
            self._close_Queue([self._input_Queue, self._output_Queue])
            if self._dev: print(f"[Multicore] : time to make the workers : {self._timeloadworkers:.8f}")
    
    # pyrefly: ignore [explicit-any]
    def _close_Queue(self, i: list[QueueType[Any]]) -> None:
        for _ in i:
            _clear_queue(_)
            _.close()
            _.join_thread()
    
    def _status(self) -> bool:
        if (time.monotonic() - self._workers_health_check_interval_timer) > self._workers_health_check_interval:
            for _ in self.workers:
                if not _.is_alive():
                    return False
            self._workers_health_check_interval_timer = time.monotonic()
        return True
    
    def status(self) -> None:
        """trigger an error if one of the workers stops working."""
        if not self._status():
            raise errors.UnexpectedWorkerExitError()
    
    def _memory_usage(self, type: str, include_main: bool = False) -> int:
        total = 0
        pids: list[int | None] = []
        
        for _ in self.workers:
            pids.append(_.pid)
        
        if include_main:
            pids.append(os.getpid())
        
        for _ in pids:
            if not _ is None:
                try:
                    if type in ["rss"]:
                        total += getattr(psutil.Process(_).memory_info(), type)
                    else:
                        total += getattr(psutil.Process(_).memory_full_info(), type)
                except psutil.NoSuchProcess:
                    pass
        return total
    
    def get_workers(self) -> list[Process]:
        return self.workers
    
    def workers_memory_usage_rss(self) -> int:
        return self._memory_usage("rss")
    
    def workers_memory_usage_uss(self) -> int:
        return self._memory_usage("uss")

    def workers_memory_usage_pss(self, include_main: bool = False) -> int:
        return self._memory_usage("uss", include_main=include_main)



if __name__ == "__main__":
    import itertools
    setproctitle("main dev worker")
    i_gen = itertools.count()
    _i = 0
    def _GenTest() -> Generator[int, None, None]:
        global _i
        while True:
            _i += 1
            yield _i
    
    def _test(x: str) -> str:
        coef = 1
        for i in range(100_000_000 * coef):
            i - 800
        return x
    
    #test = Multicore(
    #    func=_test,
    #    core=(os.cpu_count() or 1) * 2,
    #    timeout=None
    #)
    try:
        # for i in _GenTest():
        #     test.put(x=i)
        # test.close()
        
        with Multicore(core=0, func=_test, timeout=0) as s:
            while True:
                num = next(i_gen)
                s.put(x=num)
                for i in s.get():
                    print(i)
        print("163 : exit")
    except KeyboardInterrupt:
        pass
    finally:
        print("close...")