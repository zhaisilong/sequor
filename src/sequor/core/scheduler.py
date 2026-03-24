from __future__ import annotations

from collections import deque

from sequor.core.models import TaskResult, TaskSpec
from sequor.core.states import TaskState

FAILED_LIKE = {TaskState.FAILED, TaskState.UPSTREAM_FAILED}


def validate_task_names(tasks: list[TaskSpec]) -> None:
    seen: set[str] = set()
    dup: set[str] = set()
    for task in tasks:
        if task.name in seen:
            dup.add(task.name)
        seen.add(task.name)
    if dup:
        raise ValueError(f"Duplicate task names: {sorted(dup)}")


def validate_dag(tasks: list[TaskSpec]) -> None:
    validate_task_names(tasks)
    name_set = {t.name for t in tasks}
    for t in tasks:
        missing = [d for d in t.depends_on if d not in name_set]
        if missing:
            raise ValueError(f"Task {t.name} depends on missing tasks: {missing}")

    indeg = {t.name: len(t.depends_on) for t in tasks}
    downstream: dict[str, list[str]] = {t.name: [] for t in tasks}
    for t in tasks:
        for d in t.depends_on:
            downstream[d].append(t.name)
    q = deque([name for name, d in indeg.items() if d == 0])
    seen = 0
    while q:
        cur = q.popleft()
        seen += 1
        for nxt in downstream[cur]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                q.append(nxt)
    if seen != len(tasks):
        raise ValueError("Cycle detected in DAG")


class Scheduler:
    def __init__(self, tasks: list[TaskSpec], fail_fast: bool = True) -> None:
        self.tasks = {t.name: t for t in tasks}
        self.fail_fast = fail_fast
        self.downstream: dict[str, list[str]] = {t.name: [] for t in tasks}
        self.unmet = {t.name: len(t.depends_on) for t in tasks}
        self.depends_on = {t.name: list(t.depends_on) for t in tasks}
        for t in tasks:
            for dep in t.depends_on:
                self.downstream[dep].append(t.name)

    def run(self, run_task_fn) -> dict[str, TaskResult]:
        results: dict[str, TaskResult] = {}
        ready = deque([name for name, c in self.unmet.items() if c == 0])

        def mark_upstream_failed(start_name: str) -> None:
            q = deque([start_name])
            while q:
                name = q.popleft()
                if name in results:
                    continue
                results[name] = TaskResult(task_name=name, state=TaskState.UPSTREAM_FAILED, error="Upstream failed")
                for nxt in self.downstream[name]:
                    self.unmet[nxt] -= 1
                    if self.unmet[nxt] == 0:
                        q.append(nxt)

        while ready:
            name = ready.popleft()
            if name in results:
                continue
            deps = self.depends_on[name]
            if any(results[d].state in FAILED_LIKE for d in deps):
                mark_upstream_failed(name)
                continue

            res = run_task_fn(self.tasks[name])
            results[name] = res
            if self.fail_fast and res.state == TaskState.FAILED:
                for nxt in self.downstream[name]:
                    mark_upstream_failed(nxt)
                continue

            for nxt in self.downstream[name]:
                self.unmet[nxt] -= 1
                if self.unmet[nxt] == 0:
                    ready.append(nxt)

        for name in self.tasks:
            if name not in results:
                results[name] = TaskResult(task_name=name, state=TaskState.UPSTREAM_FAILED, error="Not scheduled")
        return results
