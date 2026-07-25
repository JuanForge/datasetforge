import io
import sys
import time

import imagehash
from imagehash import ImageHash
from PIL import Image


def get(data: bytes, hash_size: int = 8) -> ImageHash:
    return imagehash.phash(Image.open(io.BytesIO(data)), hash_size=hash_size)

if __name__ == "__main__":
    start_time = time.monotonic()
    print(
        get( 
                open( sys.argv[1], "rb").read() ) - get( open(sys.argv[2], "rb").read()
            )
        )
    print(time.monotonic() - start_time)