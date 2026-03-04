"""
utils/logger.py
---------------
Centralized, colorized logging setup for the ETL test framework.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

try:
    import colorlog
    HAS_COLORLOG = True
except ImportError:
    HAS_COLORLOG = False


LOG_DIR = Path(__file__).parent.parent / "reports" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger(level: str = "INFO"):
    """Initialize the root logger with console (colored) and file handlers."""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid duplicate handlers on re-import
    if root_logger.handlers:
        return

    fmt = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    # Console handler
    if HAS_COLORLOG:
        color_formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s [%(levelname)-8s]%(reset)s %(name)s: %(message)s",
            datefmt=date_fmt,
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(color_formatter)
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(fmt, date_fmt))

    # File handler — rotated per run
    log_file = LOG_DIR / f"etl_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt, date_fmt))

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
