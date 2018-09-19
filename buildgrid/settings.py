import hashlib


# The hash function that CAS uses
HASH = hashlib.sha256
HASH_LENGTH = HASH().digest_size * 2
