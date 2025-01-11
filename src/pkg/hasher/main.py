import hashlib


class Hasher:
    def __init__(self):
        return

    @staticmethod
    def get_hash(data: str):
        sha256_hash = hashlib.sha256()
        sha256_hash.update(data.encode('utf-8'))
        return sha256_hash.hexdigest()
