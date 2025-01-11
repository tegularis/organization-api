from src.pkg.database.models import ApiKey
from src.pkg.hasher.main import Hasher


class Middleware:
    def __init__(self, hasher: Hasher):
        self.hasher = hasher

    async def authenticate(self, headers) -> bool:
        if not "X-API-KEY" in headers:
            return False
        if not await ApiKey.get(hashed_key=self.hasher.get_hash(data=str(headers["X-API-KEY"]))):
            return False
        return True
