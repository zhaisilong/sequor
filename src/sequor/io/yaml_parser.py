from __future__ import annotations

from pathlib import Path

import yaml

from sequor.core.flow import Flow
from sequor.core.models import TaskSpec


def parse_flow_dict(data: dict, *, pipeline_dir: Path | None = None) -> Flow:
    flow_cfg = data.get("flow", {})
    name = flow_cfg.get("name") or "default_flow"
    fail_fast = bool(flow_cfg.get("fail_fast", True))
    work_dir = flow_cfg.get("work_dir")
    output_dir = flow_cfg.get("output_dir")
    parallelism = int(flow_cfg.get("parallelism", 4))
    if work_dir and pipeline_dir:
        work_dir_path = Path(work_dir)
        if not work_dir_path.is_absolute():
            work_dir = str((pipeline_dir / work_dir_path).resolve())
    if output_dir and pipeline_dir:
        output_dir_path = Path(output_dir)
        if not output_dir_path.is_absolute():
            output_dir = str((pipeline_dir / output_dir_path).resolve())

    task_specs: list[TaskSpec] = []
    tasks = data.get("tasks", [])
    if not isinstance(tasks, list):
        raise ValueError("tasks must be a list")
    for idx, t in enumerate(tasks):
        if not isinstance(t, dict):
            raise ValueError(f"tasks[{idx}] must be a mapping")
        for key in ["name", "task_type"]:
            if key not in t:
                raise ValueError(f"tasks[{idx}] missing required field: {key}")
        task_specs.append(
            TaskSpec(
                name=t["name"],
                task_type=t["task_type"],
                planner=t.get("planner"),
                runner=t.get("runner"),
                depends_on=list(t.get("depends_on", [])),
                config=dict(t.get("config", {})),
                retry=int(t.get("retry", 0)),
                timeout=int(t.get("timeout", 600)) if t.get("timeout", 600) is not None else None,
                cache=bool(t.get("cache", True)),
                enabled=bool(t.get("enabled", True)),
                parallelism=int(t["parallelism"]) if t.get("parallelism") is not None else None,
            )
        )

    return Flow(
        name=name,
        tasks=task_specs,
        fail_fast=fail_fast,
        work_dir=work_dir,
        output_dir=output_dir,
        pipeline_dir=str(pipeline_dir.resolve()) if pipeline_dir else None,
        parallelism=parallelism,
    )


def parse_yaml_file(path: str | Path) -> Flow:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")
    return parse_flow_dict(data, pipeline_dir=p.parent.resolve())
