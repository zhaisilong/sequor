from __future__ import annotations

from glob import glob
from pathlib import Path
from typing import Any

from sequor.core.context import RunContext
from sequor.core.models import ItemResult, TaskSpec
from sequor.core.registry import register_builder, register_processor
from sequor.core.states import ItemState
from sequor.utils.hashing import file_sha256
from sequor.utils.import_utils import load_object


def _render(s: str, ctx: RunContext, spec: TaskSpec) -> str:
    pipeline_dir = str(ctx.state.get("pipeline_dir", ctx.work_dir))
    return s.format(
        work_dir=str(ctx.work_dir),
        input_dir=str(ctx.input_dir),
        output_dir=str(ctx.output_dir),
        artifacts_dir=str(ctx.artifacts_dir),
        task_name=spec.name,
        pipeline_dir=pipeline_dir,
        project_dir=pipeline_dir,
    )


@register_builder("python")
class PythonBuilder:
    def build(self, spec: TaskSpec, ctx: RunContext) -> list[dict]:
        cfg = spec.config
        if "items" in cfg:
            return list(cfg["items"])
        pattern = cfg.get("pattern")
        if pattern:
            rendered = _render(pattern, ctx, spec)
            files = sorted(glob(rendered))
            out_dir = Path(_render(cfg.get("output_dir", "{output_dir}/" + spec.name), ctx, spec))
            out_dir.mkdir(parents=True, exist_ok=True)
            args_list: list[dict[str, Any]] = []
            suffix = cfg.get("output_suffix", ".out.txt")
            for fp in files:
                p = Path(fp)
                arg = {
                    "input_file": str(p),
                    "input_name": p.name,
                    "output_file": str(out_dir / f"{p.stem}{suffix}"),
                }
                if cfg.get("include_input_hash", True):
                    arg["input_file_hash"] = file_sha256(p)
                args_list.append(arg)
            return args_list
        return [dict(cfg.get("default_item", {}))]


@register_processor("python")
class PythonProcessor:
    def process(self, arg: dict, spec: TaskSpec, ctx: RunContext) -> ItemResult:
        fn_path = spec.config.get("function")
        if not fn_path:
            raise ValueError(f"python processor requires config.function for task {spec.name}")
        fn = load_object(fn_path)
        item_id = str(arg.get("item_id") or arg.get("input_name") or "item")
        try:
            output = fn(arg=arg, spec=spec, ctx=ctx)
        except TypeError:
            try:
                output = fn(arg, ctx)
            except TypeError:
                output = fn(arg)
        return ItemResult(
            item_id=item_id,
            state=ItemState.SUCCESS,
            success=True,
            output=output,
            meta={"processor": fn_path},
        )
