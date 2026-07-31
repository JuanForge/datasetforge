import os
import time
from collections.abc import Callable, Generator
from multiprocessing import Process, Queue
from multiprocessing.queues import Queue as QueueType
from queue import Empty as queue_Empty
from typing import Any, Self

from setproctitle import setproctitle


class errors:
    class workerRunError(Exception):
        pass


def _worker(
    # pyrefly: ignore [explicit-any]
    func: Callable[..., Any],
    # pyrefly: ignore [explicit-any]
    input_Queue: QueueType[dict[Any, Any] | None],
    # pyrefly: ignore [explicit-any]
    output_Queue: QueueType[dict[Any, Any]]
    ) -> None:
    setproctitle(f"worker-{os.getpid()}")
    while True:
        try:
            tache = input_Queue.get(block=True)
            if tache == None:
                return
            else:
                output_Queue.put(
                    {
                        "error": False,
                        "return": func(*tache["args"], **tache["kwargs"])
                    }
                )
        except BaseException as e:  # noqa: BLE001
            output_Queue.put({"error": True, "raise": e})

class Multicore:
    """
    for dev
    - If you plan to use this code, make sure to design your operations as atomic transactions, since the workers may need to be terminated forcefully.
    """
    def __init__(
        self,
        # pyrefly: ignore [explicit-any]
        func: Callable[..., Any],
        core: int = 0,
        input_Queue: int | None = None,
        closeOnError: bool = True,
        timeout: float | None = None
    ) -> None:
        core = core or os.cpu_count() or 1
        # pyrefly: ignore [explicit-any]
        self._input_Queue: QueueType[dict[Any, Any] | None] = Queue(maxsize=input_Queue or core)
        # pyrefly: ignore [explicit-any]
        self._output_Queue: QueueType[dict[Any, Any]] = Queue()
        self.func = func
        self.workers: list[Process] = []
        for _ in range(core):
            p = Process(
                target=_worker,
                args=(func, self._input_Queue, self._output_Queue)
            )
            p.start()
            self.workers.append(p)
        self.closed: bool = False
        self.closeOnError = closeOnError
        self.timeout = timeout
        self._sleepStop: float = 0.01
    
    def __enter__(self) -> Self:
        return self
    
    def __exit__(self, *_) -> None:
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
        self._input_Queue.put({
            "args": args,
            "kwargs": kwargs,
        })
    
    # pyrefly: ignore [explicit-any]
    def get(self) -> list[Any]:
        """no lock"""
        # pyrefly: ignore [explicit-any]
        out: list[Any] = []
        while True:
            try:
                data = self._output_Queue.get(block=False)
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
                self._input_Queue.put(None)
            
            while any(
                worker.is_alive() for worker in self.workers
                ) and (self.timeout is None or (time.monotonic() - start_time) < self.timeout):
                time.sleep(self._sleepStop)
            
            for worker in self.workers:
                if worker.is_alive():
                    worker.kill()
                worker.join()

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