from typing import Generator
import os
from collections.abc import Callable, Iterable
from multiprocessing import Process, Queue
from multiprocessing.queues import Queue as QueueType
from typing import Any
from queue import Empty as queue_Empty


def _worker(
    # pyrefly: ignore [explicit-any]
    func: Callable[..., Any],
    # pyrefly: ignore [explicit-any]
    input_Queue: QueueType[dict[Any, Any] | None],
    # pyrefly: ignore [explicit-any]
    output_Queue: QueueType[dict[Any, Any]]
    ) -> None:
    try:
        while True:
            tache = input_Queue.get(block=True)
            if tache == None:
                return
            else:
                output_Queue.put(
                    {
                        "error": False,
                        "return": func(tache["args"])
                    }
                )
    except BaseException as e:  # noqa: BLE001
        output_Queue.put({"error": True, "raise": e})

class multicore:
    # pyrefly: ignore [explicit-any]
    def __init__(self, func: Callable[..., Any], core: int = 0,) -> None:
        core = core or os.cpu_count() or 1
        # pyrefly: ignore [explicit-any]
        self._input_Queue: QueueType[dict[Any, Any] | None] = Queue(maxsize=core)
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
    
    # pyrefly: ignore [explicit-any]
    def put(self, args: Iterable[Any], lock: bool = True) -> None:
        self._input_Queue.put({"args": args})
    
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
                raise data["raise"]
            else:
                out.append(data["return"])
        return out
    
    def close(self) -> None:
        for _ in self.workers:
            self._input_Queue.put(None)
        
        for worker in self.workers:
            worker.join()

if __name__ == "__main__":
    _i = 0
    def _GenTest() -> Generator[int, None, None]:
        global _i
        while True:
            _i += 1
            yield _i
    
    def _test(x: str) -> str:
        print(f"_test : {x}")
        return x
    
    test = multicore(func=_test, core=2)
    
    for i in _GenTest():
        test.put({"num": i})
        print(str(95), test._input_Queue.qsize())
        print(f"81 : {test.get()}")
        print(str(97), test._input_Queue.qsize())
    print("close...")
    test.close()