import asyncio
from logging import basicConfig, INFO
from src.pkg.logger.main import Logger
from config.main import Config
from src.main import App


async def run_app():
    basicConfig(level=INFO)
    cfg = Config("config/config.yml").load()
    logger = Logger(filename='organization-app.log', name='ORG-APP', cfg=cfg)
    app = App(cfg, logger)
    await app.run()


if __name__ == '__main__':
    asyncio.run(run_app())
