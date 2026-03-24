from __future__ import annotations

from typing import Protocol

from sequor.core.context import RunContext
from sequor.core.models import TaskSpec


class TaskBuilder(Protocol):
    def build(self, spec: TaskSpec, ctx: RunContext) -> list[dict]:
        ...
