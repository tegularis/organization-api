from typing import Union
from src.app.components.building.repository import BuildingRepository
from src.pkg.logger.main import Logger


class BuildingController:
    def __init__(self, cfg: dict, logger: Logger, repository: BuildingRepository):
        self.cfg = cfg
        self.logger = logger
        self.repository = repository

    async def get_by_uuid(self, uuid: str):
        building = await self.repository.get_by_uuid(uuid=uuid)
        if not building:
            return 404, {
                'message': "building not found"
            }
        return 200, {
            'message': "success",
            'content': {
                "building": building
            }
        }

    async def get_in_radius(self, latitude: float, longitude: float, radius: float, limit: Union[int, None],
                            offset: Union[int, None]):
        if not limit:
            limit = 1000000
        if not offset:
            offset = 0
        buildings = await self.repository.get_in_radius(latitude=latitude, longitude=longitude, radius=radius,
                                                        limit=limit, offset=offset)
        return 200, {
            'message': "success",
            'content': {
                "buildings": buildings
            }
        }
