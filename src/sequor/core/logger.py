from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any


def create_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger(str(log_path))
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_path)
    fh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)
    return logger


def log_json(logger: logging.Logger, payload: dict[str, Any]) -> None:
    logger.info(json.dumps(payload, ensure_ascii=True, sort_keys=True))
