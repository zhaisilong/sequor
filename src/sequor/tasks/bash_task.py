from __future__ import annotations

import subprocess
from glob import glob
from pathlib import Path

from sequor.core.context import RunContext
from sequor.core.models import ItemResult, TaskSpec
from sequor.core.registry import register_builder, register_processor
from sequor.core.states import ItemState
from sequor.utils.hashing import file_sha256


def _render(s: str, mapping: dict[str, str]) -> str:
    return s.format(**mapping)


def _base_mapping(ctx: RunContext, spec: TaskSpec) -> dict[str, str]:
    pipeline_dir = str(ctx.state.get("pipeline_dir", ctx.work_dir))
    return {
        "work_dir": str(ctx.work_dir),
        "input_dir": str(ctx.input_dir),
        "output_dir": str(ctx.output_dir),
        "artifacts_dir": str(ctx.artifacts_dir),
        "task_name": spec.name,
        "pipeline_dir": pipeline_dir,
        "project_dir": pipeline_dir,
    }


@register_builder("bash")
class BashBuilder:
    def build(self, spec: TaskSpec, ctx: RunContext) -> list[dict]:
        cfg = spec.config
        mapping = _base_mapping(ctx, spec)
        if "items" in cfg:
            out = []
            for item in cfg["items"]:
                rendered_item = {
                    k: (_render(v, mapping) if isinstance(v, str) else v)
                    for k, v in item.items()
                }
                local = {**mapping, **{k: str(v) for k, v in rendered_item.items()}}
                cmd = _render(cfg["cmd"], local)
                out.append({**rendered_item, "cmd": cmd})
            return out
        if "pattern" in cfg:
            pattern = _render(cfg["pattern"], mapping)
            files = sorted(glob(pattern))
            out_dir = Path(_render(cfg.get("output_dir", "{output_dir}/" + spec.name), mapping))
            out_dir.mkdir(parents=True, exist_ok=True)
            out = []
            for fp in files:
                p = Path(fp)
                item = {
                    "input": str(p),
                    "input_name": p.name,
                    "output": str(out_dir / f"{p.stem}.txt"),
                }
                if cfg.get("include_input_hash", True):
                    item["input_hash"] = file_sha256(p)
                local = {**mapping, **{k: str(v) for k, v in item.items()}}
                cmd = _render(cfg["cmd"], local)
                out.append({**item, "cmd": cmd})
            return out
        cmd = cfg.get("cmd")
        if not cmd:
            raise ValueError(f"bash builder requires config.cmd for task {spec.name}")
        return [{"cmd": _render(cmd, mapping)}]


@register_processor("bash")
class BashProcessor:
    def process(self, arg: dict, spec: TaskSpec, ctx: RunContext) -> ItemResult:
        cmd = arg.get("cmd")
        if not cmd:
            raise ValueError(f"bash processor requires arg.cmd for task {spec.name}")
        item_id = str(arg.get("item_id") or arg.get("input_name") or "item")
        proc = subprocess.run(
            cmd,
            shell=True,
            executable="/bin/bash",
            cwd=arg.get("cwd", ctx.state.get("launch_cwd", str(ctx.work_dir))),
            text=True,
            capture_output=True,
            timeout=spec.timeout,
        )
        meta = {
            "cmd": cmd,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
        if proc.returncode == 0:
            return ItemResult(item_id=item_id, state=ItemState.SUCCESS, success=True, meta=meta)
        return ItemResult(
            item_id=item_id,
            state=ItemState.FAILED,
            success=False,
            error=f"Command failed with code {proc.returncode}",
            meta=meta,
        )
