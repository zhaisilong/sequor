from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from sequor.core.context import RunContext
from sequor.core.models import FlowResult, TaskSpec
from sequor.core.runner import Runner
from sequor.core.scheduler import Scheduler, validate_dag
from sequor.core.states import FlowState, TaskState
from sequor.tasks import task_runtime  # noqa: F401
from sequor.core import executor as _executor_registry  # noqa: F401


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Flow:
    def __init__(
        self,
        name: str,
        tasks: list[TaskSpec],
        fail_fast: bool = True,
        work_dir: str | None = None,
        output_dir: str | None = None,
        pipeline_dir: str | None = None,
        parallelism: int = 4,
    ) -> None:
        self.name = name
        self.tasks = tasks
        self.fail_fast = fail_fast
        self.work_dir = work_dir
        self.output_dir = output_dir
        self.pipeline_dir = pipeline_dir
        self.parallelism = max(1, int(parallelism))
        validate_dag(tasks)

    def run(self) -> FlowResult:
        ctx = RunContext.create(
            self.name,
            base_work_dir=self.work_dir,
            public_output_dir=self.output_dir,
            launch_cwd=str(Path.cwd()),
            pipeline_dir=self.pipeline_dir or str(Path.cwd()),
        )
        ctx.state["flow_parallelism"] = self.parallelism
        runner = Runner(ctx)
        scheduler = Scheduler(self.tasks, fail_fast=self.fail_fast)
        flow_result = FlowResult(
            flow_name=self.name,
            state=FlowState.RUNNING,
            run_id=ctx.run_id,
            work_dir=Path(ctx.work_dir),
        )
        flow_result.started_at = _utc_now()

        task_results = scheduler.run(runner.run_task)
        flow_result.task_results = task_results
        flow_result.finished_at = _utc_now()

        states = [tr.state for tr in task_results.values()]
        if any(s == TaskState.FAILED for s in states):
            flow_result.state = FlowState.FAILED
        elif any(s == TaskState.UPSTREAM_FAILED for s in states):
            flow_result.state = FlowState.PARTIAL
        elif any(s == TaskState.SKIPPED for s in states):
            flow_result.state = FlowState.PARTIAL
        else:
            flow_result.state = FlowState.SUCCESS

        return flow_result
