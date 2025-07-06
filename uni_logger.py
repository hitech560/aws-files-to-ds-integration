# uni_logger.py
import sys
import os
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from io import TextIOWrapper

LOGGING_LEVELS = set (
    [
        "NOTSET",   # NOTSET (0):       This level is rarely used directly for logging messages 
                    #                   but serves as a default or for specific configurations.
        "DEBUG",    # DEBUG (10):       Used for detailed diagnostic information, typically of 
                    #                   interest only during development and troubleshooting.
        "INFO",     # INFO (20):        Used to confirm that things are working as expected or  
                    #                   to provide general information about the application's 
                    #                   operational flow. 
        "WARNING",  # WARNING (30):     Indicates that something unexpected has occurred, or a 
                    #                   potential problem in the near future (e.g., 'disk space 
                    #                   low'). The software is still working as expected. This 
                    #                   is the default logging level.
        "ERROR",    # ERROR (40):       Indicates a more serious problem where the software has 
                    #                   not been able to perform some function.
        "CRITICAL"  # CRITICAL (50):    Indicates a serious error, suggesting that the program 
                    #                   itself may be unable to continue running. 
    ]
)

class UTF8StreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        if stream is None:
            stream = TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
        super().__init__(stream)

def setup_logger(name: str = None) -> logging.Logger:
    script_name = name or Path(sys.argv[0]).stem
    environment = os.getenv("ENVIRONMENT", "DEV").upper()
    log_level = os.getenv("LOG_LEVEL", "WARNING").upper()
    log_level = log_level if log_level in LOGGING_LEVELS else "WARNING"
    logging_level = getattr(logging, log_level, logging.WARNING)

    logger = logging.getLogger(f"{script_name} - {__name__}")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")

    today_str = datetime.now().strftime("%Y-%m-%d")
    # log_dir = Path("Logs") / environment / today_str
    log_dir = Path("Logs") 
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = log_dir / f"{script_name}.log"

    if not logger.handlers:
        # Console: UTF-8 safe
        console_handler = UTF8StreamHandler()
        console_handler.setLevel(
            logging_level if logging_level <= logging.WARNING else logging.WARNING
            )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File: UTF-8 safe with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file_path,
            maxBytes=1_000_000,
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setLevel(
            logging_level if logging_level <= logging.INFO else logging.INFO
            )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
