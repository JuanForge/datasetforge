import hashlib

import blake3
import xxhash

hash_algo = {
    "sha256": hashlib.sha256,
    "xxh3-64": xxhash.xxh3_64,
    "xxh3-128": xxhash.xxh3_128,
    "blake3": blake3.blake3
}
hash_default = "sha256"