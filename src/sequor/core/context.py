from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


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
    state: dict = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        flow_name: str,
        base_work_dir: str | Path | None = None,
        public_output_dir: str | Path | None = None,
        *,
        launch_cwd: str | None = None,
        pipeline_dir: str | None = None,
    ) -> "RunContext":
        run_id = "current"
        root = Path(base_work_dir) if base_work_dir else Path(".sequor") / flow_name
        work_dir = root
        input_dir = root / "input"
        output_dir = Path(public_output_dir) if public_output_dir else root / "output"
        cache_dir = root / "cache"
        artifacts_dir = root / "artifacts"
        logs_dir = root / "logs"
        # input_dir/artifacts_dir/cache_dir are lazy-created on demand when tasks reference them.
        for p in [work_dir, output_dir, logs_dir]:
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
            state={
                "launch_cwd": launch_cwd or str(Path.cwd()),
                "pipeline_dir": pipeline_dir or str(Path.cwd()),
            },
        )
