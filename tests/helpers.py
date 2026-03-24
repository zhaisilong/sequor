from __future__ import annotations

import time
from typing import Any


def sleep_python_fn(arg: dict[str, Any], spec=None, ctx=None) -> dict[str, Any]:
    seconds = float(arg.get("sleep", 0))
    time.sleep(seconds)
    return {"item_id": arg.get("item_id"), "slept": seconds}


def script_emit(arg: dict[str, Any], spec=None, ctx=None) -> dict[str, Any]:
    return {
        "item_id": str(arg.get("item_id", "script")),
        "value": str(arg.get("value", "ok")),
    }
