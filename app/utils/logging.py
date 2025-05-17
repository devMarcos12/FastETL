import logging
import os
from datetime import datetime

def setup_logger(name):
    """Configure and return a logger with file and console handlers."""
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        timestamp = datetime.now().strftime("%Y%m%d")
        file_handler = logging.FileHandler(f"logs/etl_{timestamp}.log")
        file_handler.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger