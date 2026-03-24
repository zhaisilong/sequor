from __future__ import annotations

from typing import Protocol

from sequor.core.context import RunContext
from sequor.core.models import ItemResult, TaskSpec


class Processor(Protocol):
    def process(self, arg: dict, spec: TaskSpec, ctx: RunContext) -> ItemResult:
        ...
