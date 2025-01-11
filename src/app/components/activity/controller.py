from typing import Union
from src.app.components.activity.repository import ActivityRepository
from src.pkg.logger.main import Logger


class ActivityController:
    def __init__(self, cfg: dict, logger: Logger, repository: ActivityRepository):
        self.cfg = cfg
        self.logger = logger
        self.repository = repository

    async def get_by_uuid(self, uuid: str):
        activity = await self.repository.get_by_uuid(uuid=uuid)
        if not activity:
            return 404, {
                'message': "activity not found"
            }
        return 200, {
            'message': "success",
            'content': {
                "activity": activity
            }
        }

    async def get_all(self, limit: Union[int, None], offset: Union[int, None]):
        if not limit:
            limit = 1000000
        if not offset:
            offset = 0
        activities = await self.repository.get_all(limit=limit, offset=offset)
        return 200, {
            'message': "success",
            'content': {
                "activities": activities
            }
        }
