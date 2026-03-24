from __future__ import annotations

import importlib


def load_object(path: str):
    module_name, _, attr = path.rpartition(".")
    if not module_name:
        raise ValueError(f"Invalid import path: {path}")
    mod = importlib.import_module(module_name)
    try:
        return getattr(mod, attr)
    except AttributeError as exc:
        raise AttributeError(f"Cannot resolve object {path}") from exc
