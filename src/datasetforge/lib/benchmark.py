import hashlib
import time

import blake3
import humanize
import xxhash

_algo = {
    "sha256": hashlib.sha256,
    "XXH3-64": xxhash.xxh3_64,
    "XXH3-128": xxhash.xxh3_128,
    "BLAKE3": blake3.blake3
}

def benchmark() -> None:
    for _size_memory in [1024, 1024 * 1024, 1024 ** 3, (1024 ** 3) * 10]:
        data = bytearray(_size_memory)
        print(f"lock memory : {humanize.naturalsize(_size_memory, binary=True)}")
        for algo, func in _algo.items():
            start_time = time.monotonic()
            getattr(func(data), "hexdigest")  # noqa: B009
            print(f"[{algo}] : time : {time.monotonic() - start_time}")

if __name__ == "__main__":
    benchmark()