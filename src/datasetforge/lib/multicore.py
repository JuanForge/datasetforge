import os
from collections.abc import Callable, Iterable
from multiprocessing import Process, Queue
from multiprocessing.queues import Queue as QueueType
from typing import Any


def _worker(
    # pyrefly: ignore [explicit-any]
    func: Callable[..., Any],
    # pyrefly: ignore [explicit-any]
    input_Queue: QueueType[dict[Any, Any] | None],
    # pyrefly: ignore [explicit-any]
    output_Queue: QueueType[dict[Any, Any]]
    ) -> None:
    try:
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
    def put_and_get(self, args: Iterable[Any], lock: bool = True) -> list[Any]:
        self._input_Queue.put({"args": args})
        data = self._output_Queue.get()
        
        if data["error"]:
            print("="*5 + "ERROR IN WORKERS" + "="*5)
            raise data["raise"]
        else:
            return data["return"]
    
    def close(self) -> None:
        for _ in self.workers:
            self._input_Queue.put(None)
        
        for worker in self.workers:
            worker.join()

if __name__ == "__main__":
    def _test(x: str) -> str:
        print(f"_test : {x}")
        return x
    test = multicore(func=_test, core=2)
    print(f"56 : {test.put_and_get({"path": "/test"})}")
    print("close...")
    test.close()