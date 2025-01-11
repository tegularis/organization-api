import uvicorn
from fastapi import FastAPI
from src.app.components.activity.controller import ActivityController
from src.app.components.activity.repository import ActivityRepository
from src.app.components.activity.router import ActivityRouter
from src.app.components.building.controller import BuildingController
from src.app.components.building.repository import BuildingRepository
from src.app.components.building.router import BuildingRouter
from src.app.components.middleware.main import Middleware
from src.app.components.organization.controller import OrganizationController
from src.app.components.organization.repository import OrganizationRepository
from src.app.components.organization.router import OrganizationRouter
from src.pkg.database.models import async_session, create_models
from src.pkg.hasher.main import Hasher
from src.pkg.logger.main import Logger


class App:
    def __init__(self, cfg: dict, logger: Logger):
        self.cfg = cfg

        hasher = Hasher()
        middleware = Middleware(hasher=hasher)

        activity_repository = ActivityRepository(async_session=async_session)
        building_repository = BuildingRepository(async_session=async_session)
        organization_repository = OrganizationRepository(async_session=async_session)

        activity_controller = ActivityController(cfg=self.cfg, logger=logger, repository=activity_repository)
        building_controller = BuildingController(cfg=self.cfg, logger=logger, repository=building_repository)
        organization_controller = OrganizationController(cfg=self.cfg, logger=logger,
                                                         repository=organization_repository)

        self.app = FastAPI()
        self.app.include_router(
            ActivityRouter(
                cfg=cfg, controller=activity_controller, logger=logger, middleware=middleware).router,
                prefix="/activity"
        )
        self.app.include_router(
            BuildingRouter(
                cfg=cfg, controller=building_controller, logger=logger, middleware=middleware).router,
                prefix="/building"
        )
        self.app.include_router(
            OrganizationRouter(
                cfg=cfg, controller=organization_controller, logger=logger, middleware=middleware).router,
                prefix="/organization"
        )

    async def run(self):
        await create_models(insert_test_data=True) # Set True to insert test rows into tables
        config = uvicorn.Config(self.app, host=self.cfg["app"]["host"], port=self.cfg["app"]["port"])
        server = uvicorn.Server(config)
        await server.serve()
