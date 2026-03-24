from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sequor.core.states import FlowState, ItemState, TaskState


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class TaskSpec:
    name: str
    builder: str
    processor: str
    executor: str | None = None
    depends_on: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    retry: int = 0
    timeout: int | None = None
    cache: bool = True
    enabled: bool = True


@dataclass(slots=True)
class ItemResult:
    item_id: str
    state: ItemState
    success: bool
    cached: bool = False
    output: Any = None
    artifacts: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


@dataclass(slots=True)
class TaskResult:
    task_name: str
    state: TaskState
    item_results: list[ItemResult] = field(default_factory=list)
    started_at: str = field(default_factory=utc_now_iso)
    finished_at: str | None = None
    stats: dict[str, int] = field(default_factory=dict)
    error: str | None = None


@dataclass(slots=True)
class FlowResult:
    flow_name: str
    state: FlowState
    run_id: str
    task_results: dict[str, TaskResult] = field(default_factory=dict)
    started_at: str = field(default_factory=utc_now_iso)
    finished_at: str | None = None
    work_dir: Path | None = None


@dataclass(slots=True)
class FlowConfig:
    name: str
    fail_fast: bool = True
    work_dir: str | None = None
