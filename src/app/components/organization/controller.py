from typing import Union
from src.app.components.organization.repository import OrganizationRepository
from src.pkg.logger.main import Logger


class OrganizationController:
    def __init__(self, cfg: dict, logger: Logger, repository: OrganizationRepository):
        self.cfg = cfg
        self.logger = logger
        self.repository = repository

    async def get_by_uuid(self, uuid: str):
        organization = await self.repository.get_by_uuid(uuid=uuid)
        if not organization:
            return 404, {
                'message': "organization not found"
            }
        return 200, {
            'message': "success",
            'content': {
                "organization": organization
            }
        }

    async def get_by_name(self, name: str):
        organization = await self.repository.get_by_name(name=name)
        if not organization:
            return 404, {
                'message': "organization not found"
            }
        return 200, {
            'message': "success",
            'content': {
                "organization": organization
            }
        }

    async def get_in_radius(self, latitude: float, longitude: float, radius: float, limit: Union[int, None],
                            offset: Union[int, None]):
        if not limit:
            limit = 1000000
        if not offset:
            offset = 0
        organizations = await self.repository.get_in_radius(latitude=latitude, longitude=longitude, radius=radius,
                                                            limit=limit, offset=offset)
        return 200, {
            'message': "success",
            'content': {
                "organizations": organizations
            }
        }

    async def get_by_activity(self, activity_uuid: str, limit: Union[int, None], offset: Union[int, None]):
        if not limit:
            limit = 1000000
        if not offset:
            offset = 0
        organizations = await self.repository.get_by_activity(activity_uuid=activity_uuid, limit=limit, offset=offset)
        return 200, {
            'message': "success",
            'content': {
                "organizations": organizations
            }
        }
