from typing import Optional
from fastapi import APIRouter, Request, Response

from src.app.components.middleware.main import Middleware
from src.app.components.organization.controller import OrganizationController
from src.pkg.logger.main import Logger


class OrganizationRouter:
    def __init__(self, controller: OrganizationController, cfg, logger: Logger, middleware: Middleware):
        self.controller = controller
        self.cfg = cfg
        self.router = APIRouter()
        self.logger = logger
        self.middleware = middleware

        @self.router.get('/by_uuid')
        async def get_by_uuid(request: Request, response: Response, uuid: str):
            if not await self.middleware.authenticate(request.headers):
                response.status_code = 401
                return {
                    'message': "authentication failed"
                }

            status_code, data = await self.controller.get_by_uuid(uuid=uuid)
            response.status_code = status_code
            return data

        @self.router.get('/by_name')
        async def get_by_name(request: Request, response: Response, name: str):
            if not await self.middleware.authenticate(request.headers):
                response.status_code = 401
                return {
                    'message': "authentication failed"
                }

            status_code, data = await self.controller.get_by_name(name=name)
            response.status_code = status_code
            return data

        @self.router.get('/in_radius')
        async def get_in_radius(request: Request, response: Response, latitude: float, longitude: float, radius: float,
                                limit: Optional[int] = None, offset: Optional[int] = None):
            if not await self.middleware.authenticate(request.headers):
                response.status_code = 401
                return {
                    'message': "authentication failed"
                }

            status_code, data = await self.controller.get_in_radius(
                latitude=latitude, longitude=longitude, radius=radius, limit=limit, offset=offset)
            response.status_code = status_code
            return data

        @self.router.get('/by_activity')
        async def get_by_activity(request: Request, response: Response, activity: str,
                                  limit: Optional[int] = None, offset: Optional[int] = None):
            if not await self.middleware.authenticate(request.headers):
                response.status_code = 401
                return {
                    'message': "authentication failed"
                }

            status_code, data = await self.controller.get_by_activity(activity=activity, limit=limit, offset=offset)
            response.status_code = status_code
            return data
