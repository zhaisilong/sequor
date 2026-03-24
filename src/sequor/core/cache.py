from __future__ import annotations

import json
from pathlib import Path
import threading
from typing import Any


class CacheStore:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.objects_dir = cache_dir / "objects"
        self.index_path = cache_dir / "index.jsonl"
        self._lock = threading.Lock()
        self.objects_dir.mkdir(parents=True, exist_ok=True)

    def _obj_dir(self, key: str) -> Path:
        return self.objects_dir / key

    def exists(self, key: str) -> bool:
        return (self._obj_dir(key) / "result.json").exists()

    def get(self, key: str) -> dict[str, Any] | None:
        path = self._obj_dir(key) / "result.json"
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def set(self, key: str, payload: dict[str, Any], index_entry: dict[str, Any] | None = None) -> None:
        obj_dir = self._obj_dir(key)
        with self._lock:
            obj_dir.mkdir(parents=True, exist_ok=True)
            (obj_dir / "result.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
            if index_entry:
                with self.index_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(index_entry, sort_keys=True) + "\n")
