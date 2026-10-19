"""Logging: console + daily-rotated file under data_dir/logs."""
from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

_FMT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: str, data_dir: Path) -> None:
    log_dir = data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(_FMT, _DATEFMT)
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_dir / "agent.log", when="midnight", backupCount=14, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    for noisy in ("httpx", "httpcore", "telegram", "apscheduler", "websockets", "aiosqlite"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
