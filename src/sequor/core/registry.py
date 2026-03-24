from __future__ import annotations

from collections.abc import Callable
from typing import Any

PLANNERS: dict[str, Any] = {}
RUNNERS: dict[str, Any] = {}
EXECUTORS: dict[str, Any] = {}


def _register(registry: dict[str, Any], name: str, obj: Any) -> Any:
    if name in registry:
        raise ValueError(f"Duplicate registration: {name}")
    registry[name] = obj
    return obj


def register_planner(name: str) -> Callable[[Any], Any]:
    def deco(obj: Any) -> Any:
        return _register(PLANNERS, name, obj)

    return deco


def register_runner(name: str) -> Callable[[Any], Any]:
    def deco(obj: Any) -> Any:
        return _register(RUNNERS, name, obj)

    return deco


def register_executor(name: str) -> Callable[[Any], Any]:
    def deco(obj: Any) -> Any:
        return _register(EXECUTORS, name, obj)

    return deco


def get_planner(name: str) -> Any:
    if name not in PLANNERS:
        available = ", ".join(sorted(PLANNERS)) or "<empty>"
        raise KeyError(f"Planner not found: {name}. Available: {available}")
    return PLANNERS[name]


def get_runner(name: str) -> Any:
    if name not in RUNNERS:
        available = ", ".join(sorted(RUNNERS)) or "<empty>"
        raise KeyError(f"Runner not found: {name}. Available: {available}")
    return RUNNERS[name]


def get_executor(name: str) -> Any:
    if name not in EXECUTORS:
        available = ", ".join(sorted(EXECUTORS)) or "<empty>"
        raise KeyError(f"Executor not found: {name}. Available: {available}")
    return EXECUTORS[name]
