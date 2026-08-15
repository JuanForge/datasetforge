import time

import imagehash
import numpy

size_for = 10_000_000

a64 = "9c4c6136a70b6f99"
b64 = "9c4c6136a70b6f99"


a256 = "9c4c6136a70b6f999c4c6136a70b6f999c4c6136a70b6f999c4c6136a70b6f99"
b256 = "9c4c6136a70b6f999c4c6136a70b6f999c4c6136a70b6f999c4c6136a70b6f99"

for unit in [(a64, b64), (a256, b256)]:
    a: str = unit[0]
    b: str = unit[1]
    
    a_int = int(a, 16)
    b_int = int(b, 16)
    
    a_imagehash = imagehash.hex_to_hash(a).hash
    b_imagehash = imagehash.hex_to_hash(b).hash
    
    a_imagehash_obj = imagehash.hex_to_hash(a)
    b_imagehash_obj = imagehash.hex_to_hash(b)
    
    print(f"===={len(a) * 4} bits====")
    
    start_time = time.perf_counter()
    for _ in range(size_for):
        distance = a_imagehash_obj - b_imagehash_obj
    print(f"base : {time.perf_counter() - start_time}")
    
    start_time = time.perf_counter()
    for _ in range(size_for):
        distance = numpy.count_nonzero(a != b)
        distance = int(distance)
    print(f"numpy local: {time.perf_counter() - start_time}")
    
    start_time = time.perf_counter()
    for _ in range(size_for):
        distance = (a_int ^ b_int).bit_count()
    print(f"XOR : {time.perf_counter() - start_time}")