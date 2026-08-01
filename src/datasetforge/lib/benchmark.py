import time

import humanize

from datasetforge.lib.hash import hash_algo


def benchmark() -> None:
    for _size_memory in [1024, 1024 * 1024, 1024 ** 3, (1024 ** 3) * 10]:
        print(f"lock memory : {humanize.naturalsize(_size_memory, binary=True)}...", end='', flush=True)
        data = bytearray(_size_memory)
        print("locked !")
        results: list[tuple[str, float]] = []
        for algo, func in hash_algo.items():
            start_time = time.perf_counter()
            getattr(func(data), "hexdigest")  # noqa: B009
            _total = time.perf_counter() - start_time
            #print(f"[{algo}] : time : {_total}")
            results.append((algo, _total))
        
        results.sort(key=lambda x: x[1])
        for index, (algo, ttime) in enumerate(results):
            print(f"[#{index}] : [{algo}] : {ttime:.16f}s | {(_size_memory / ttime / (1024**3)):.2f} GiB/s")

if __name__ == "__main__":
    benchmark()