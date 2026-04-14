import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


class EthereumLogger:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(EthereumLogger, cls).__new__(cls)
        return cls._instance

    def __init__(self, name="EthereumApp", log_file="app.log", level=logging.INFO):
        # Prevent re-initialization if the singleton already exists
        if hasattr(self, "_initialized"):
            return

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self._initialized = True

        # 1. Define Formats
        # Production tip: Include %(threadName)s for multi-threaded apps
        log_format = logging.Formatter(
            '[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 2. Console Handler (Standard Output)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_format)
        self.logger.addHandler(console_handler)

        # 3. Rotating File Handler (Prevents disk exhaustion)
        if log_file:
            log_path = Path("logs")
            log_path.mkdir(exist_ok=True)  # Create logs directory if missing

            file_handler = RotatingFileHandler(
                log_path / log_file,
                maxBytes=10 ** 7,  # 10MB per file
                backupCount=5  # Keep last 5 files
            )
            file_handler.setFormatter(log_format)
            self.logger.addHandler(file_handler)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg, exc_info=True)  # exc_info captures stack traces

    def warning(self, msg):
        self.logger.warning(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def critical(self, msg):
        self.logger.critical(msg)


# Global utility instance
eth_logger = EthereumLogger()