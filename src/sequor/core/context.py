from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4


@dataclass(slots=True)
class RunContext:
    flow_name: str
    run_id: str
    work_dir: Path
    input_dir: Path
    output_dir: Path
    cache_dir: Path
    artifacts_dir: Path
    logs_dir: Path
    temp_dir: Path
    state: dict = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        flow_name: str,
        base_work_dir: str | Path | None = None,
        *,
        launch_cwd: str | None = None,
        pipeline_dir: str | None = None,
    ) -> "RunContext":
        run_id = uuid4().hex[:12]
        root = Path(base_work_dir) if base_work_dir else Path(".sequor") / flow_name
        work_dir = root / "runs" / run_id
        runtime_dir = root / "runtime"
        input_dir = runtime_dir / "input"
        output_dir = runtime_dir / "output"
        cache_dir = root / "cache"
        artifacts_dir = work_dir / "artifacts"
        logs_dir = work_dir / "logs"
        temp_dir = work_dir / "tmp"
        for p in [work_dir, runtime_dir, input_dir, output_dir, cache_dir, artifacts_dir, logs_dir, temp_dir]:
            p.mkdir(parents=True, exist_ok=True)
        return cls(
            flow_name=flow_name,
            run_id=run_id,
            work_dir=work_dir,
            input_dir=input_dir,
            output_dir=output_dir,
            cache_dir=cache_dir,
            artifacts_dir=artifacts_dir,
            logs_dir=logs_dir,
            temp_dir=temp_dir,
            state={
                "launch_cwd": launch_cwd or str(Path.cwd()),
                "pipeline_dir": pipeline_dir or str(Path.cwd()),
            },
        )
