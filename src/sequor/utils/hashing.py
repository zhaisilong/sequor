from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, ensure_ascii=True, default=str, separators=(",", ":"))


def sha256_obj(data: Any) -> str:
    return sha256_text(canonical_json(data))


def file_sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    p = Path(path)
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
