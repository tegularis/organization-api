import logging
import sys


class Logger:
    def __init__(self, name, filename, cfg, console_output=False):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(filename)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

        if console_output:
            stream_handler = logging.StreamHandler(sys.stdout)
            logger.addHandler(stream_handler)

        self.name = name
        self.logger = logger
        self.cfg = cfg

    def success(self, msg):
        msg = f"{self.name} | SUCCESS: {msg}"
        self.logger.info(msg)

    def info(self, msg):
        msg = f"{self.name} | {msg}"
        self.logger.info(msg)

    def error(self, msg):
        msg = f"{self.name} | {msg}"
        self.logger.error(msg)

    def warning(self, msg):
        msg = f"{self.name} | {msg}"
        self.logger.warning(msg)

