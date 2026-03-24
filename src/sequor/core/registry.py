from __future__ import annotations

from collections.abc import Callable
from typing import Any

BUILDERS: dict[str, Any] = {}
PROCESSORS: dict[str, Any] = {}
EXECUTORS: dict[str, Any] = {}


def _register(registry: dict[str, Any], name: str, obj: Any) -> Any:
    if name in registry:
        raise ValueError(f"Duplicate registration: {name}")
    registry[name] = obj
    return obj


def register_builder(name: str) -> Callable[[Any], Any]:
    def deco(obj: Any) -> Any:
        return _register(BUILDERS, name, obj)

    return deco


def register_processor(name: str) -> Callable[[Any], Any]:
    def deco(obj: Any) -> Any:
        return _register(PROCESSORS, name, obj)

    return deco


def register_executor(name: str) -> Callable[[Any], Any]:
    def deco(obj: Any) -> Any:
        return _register(EXECUTORS, name, obj)

    return deco


def get_builder(name: str) -> Any:
    if name not in BUILDERS:
        available = ", ".join(sorted(BUILDERS)) or "<empty>"
        raise KeyError(f"Builder not found: {name}. Available: {available}")
    return BUILDERS[name]


def get_processor(name: str) -> Any:
    if name not in PROCESSORS:
        available = ", ".join(sorted(PROCESSORS)) or "<empty>"
        raise KeyError(f"Processor not found: {name}. Available: {available}")
    return PROCESSORS[name]


def get_executor(name: str) -> Any:
    if name not in EXECUTORS:
        available = ", ".join(sorted(EXECUTORS)) or "<empty>"
        raise KeyError(f"Executor not found: {name}. Available: {available}")
    return EXECUTORS[name]
