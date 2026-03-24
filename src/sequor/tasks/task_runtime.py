from __future__ import annotations

import csv
from glob import glob
import inspect
from pathlib import Path
import subprocess
import threading
from typing import Any

from sequor.core.context import RunContext
from sequor.core.models import ItemResult, TaskSpec
from sequor.core.registry import register_planner, register_runner
from sequor.core.states import ItemState
from sequor.utils.import_utils import load_object


def _base_mapping(ctx: RunContext, spec: TaskSpec) -> dict[str, str]:
    pipeline_dir = str(ctx.state.get("pipeline_dir", ctx.work_dir))
    return {
        "work_dir": str(ctx.work_dir),
        "input_dir": str(ctx.input_dir),
        "output_dir": str(ctx.output_dir),
        "cache_dir": str(ctx.cache_dir),
        "artifacts_dir": str(ctx.artifacts_dir),
        "task_name": spec.name,
        "pipeline_dir": pipeline_dir,
        "project_dir": pipeline_dir,
    }


def _render(value: str, mapping: dict[str, str]) -> str:
    return value.format(**mapping)


def _render_item_values(item: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    rendered: dict[str, Any] = {}
    for key, value in item.items():
        if isinstance(value, str):
            rendered[key] = _render(value, mapping)
        else:
            rendered[key] = value
    return rendered


def _validate_item_ids(items: list[dict[str, Any]], task_name: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(items):
        if "item_id" not in item:
            raise ValueError(f"task {task_name} item #{idx} missing required item_id")
        item_id = str(item["item_id"]).strip()
        if not item_id:
            raise ValueError(f"task {task_name} item #{idx} has empty item_id")
        if item_id in seen:
            raise ValueError(f"task {task_name} has duplicate item_id: {item_id}")
        seen.add(item_id)
        out.append(item)
    return out


class GenericPlanner:
    def plan(self, spec: TaskSpec, ctx: RunContext) -> list[dict[str, Any]]:
        cfg = spec.config
        mapping = _base_mapping(ctx, spec)

        from_task = cfg.get("from_task")
        if from_task:
            outputs = list(ctx.state.get("task_outputs", {}).get(str(from_task), []))
            items: list[dict[str, Any]] = []
            for idx, out in enumerate(outputs):
                if not isinstance(out, dict):
                    raise ValueError(
                        f"task {spec.name} from_task expects dict outputs with item_id, got {type(out)} at index {idx}"
                    )
                if "item_id" not in out:
                    raise ValueError(
                        f"task {spec.name} from_task output missing item_id at index {idx}"
                    )
                item = {"item_id": str(out["item_id"]), "arg": dict(out)}
                items.append(item)
            return _validate_item_ids(items, spec.name)

        if "items" in cfg:
            raw_items = cfg.get("items")
            if not isinstance(raw_items, list):
                raise ValueError(f"task {spec.name} config.items must be list")
            items: list[dict[str, Any]] = []
            for raw in raw_items:
                if not isinstance(raw, dict):
                    raise ValueError(f"task {spec.name} config.items entries must be dict")
                rendered = _render_item_values(raw, mapping)
                item_id = rendered.get("item_id")
                arg = dict(rendered)
                arg.pop("item_id", None)
                items.append({"item_id": item_id, "arg": arg})
            return _validate_item_ids(items, spec.name)

        if "pattern" in cfg:
            pattern = _render(str(cfg["pattern"]), mapping)
            item_id_tpl = cfg.get("item_id_template")
            if not item_id_tpl:
                raise ValueError(
                    f"task {spec.name} pattern planner requires config.item_id_template"
                )
            files = sorted(glob(pattern))
            out_dir_tpl = cfg.get("output_dir")
            items = []
            for fp in files:
                p = Path(fp)
                local = {**mapping, "input_file": str(p), "input_name": p.name, "input_stem": p.stem}
                arg: dict[str, Any] = {
                    "input_file": str(p),
                    "input_name": p.name,
                }
                if out_dir_tpl:
                    out_dir = Path(_render(str(out_dir_tpl), local))
                    arg["output_file"] = str(out_dir / p.name)
                    local["output_file"] = arg["output_file"]
                item_id = _render(str(item_id_tpl), local)
                if cfg.get("cmd_template"):
                    arg["cmd"] = _render(str(cfg["cmd_template"]), {**local, **{k: str(v) for k, v in arg.items()}})
                items.append({"item_id": item_id, "arg": arg})
            return _validate_item_ids(items, spec.name)

        default_item = cfg.get("default_item")
        if not isinstance(default_item, dict):
            raise ValueError(
                f"task {spec.name} requires config.items, config.pattern, config.from_task, or config.default_item"
            )
        rendered = _render_item_values(default_item, mapping)
        item_id = rendered.get("item_id")
        arg = dict(rendered)
        arg.pop("item_id", None)
        return _validate_item_ids([{"item_id": item_id, "arg": arg}], spec.name)


@register_planner("csv_manifest")
class CsvManifestPlanner:
    def plan(self, spec: TaskSpec, ctx: RunContext) -> list[dict[str, Any]]:
        cfg = spec.config
        manifest = cfg.get("manifest")
        if not manifest:
            raise ValueError(f"task {spec.name} csv_manifest planner requires config.manifest")

        manifest_path = Path(str(manifest))
        if not manifest_path.is_absolute():
            manifest_path = Path(str(ctx.state.get("pipeline_dir", "."))) / manifest_path
        manifest_path = manifest_path.resolve()

        mapping = _base_mapping(ctx, spec)
        item_id_field = str(cfg.get("item_id_field", "item_id"))

        items: list[dict[str, Any]] = []
        with manifest_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError(f"CSV manifest has no header: {manifest_path}")
            for row in reader:
                merged = dict(cfg.get("defaults", {}))
                merged.update(row)
                rendered = _render_item_values(merged, mapping)
                if item_id_field not in rendered:
                    raise ValueError(
                        f"task {spec.name} csv row missing {item_id_field} (required for item_id)"
                    )
                item_id = str(rendered[item_id_field])
                arg = dict(rendered)
                if cfg.get("cmd_template"):
                    local = {**mapping, **{k: str(v) for k, v in arg.items()}}
                    arg["cmd"] = _render(str(cfg["cmd_template"]), local)
                items.append({"item_id": item_id, "arg": arg})

        return _validate_item_ids(items, spec.name)


@register_planner("python_fn")
class PythonFnPlanner(GenericPlanner):
    pass


@register_planner("shell_cmd")
class ShellCmdPlanner(GenericPlanner):
    pass


@register_planner("script")
class ScriptPlanner(GenericPlanner):
    pass


def _call_with_supported_signature(fn, *, arg: dict[str, Any], spec: TaskSpec, ctx: Any):
    sig = inspect.signature(fn)
    params = sig.parameters
    if "arg" in params:
        kwargs: dict[str, Any] = {"arg": arg}
        if "spec" in params:
            kwargs["spec"] = spec
        if "ctx" in params:
            kwargs["ctx"] = ctx
        return fn(**kwargs)

    positional = [
        p
        for p in params.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(positional) >= 3:
        return fn(arg, spec, ctx)
    if len(positional) == 2:
        return fn(arg, ctx)
    return fn(arg)


def _call_with_soft_timeout(fn, timeout_s: int | None):
    if timeout_s is None:
        return fn()

    out: dict[str, Any] = {}
    err: dict[str, BaseException] = {}

    def _target() -> None:
        try:
            out["value"] = fn()
        except BaseException as exc:  # propagate original error type
            err["exc"] = exc

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    thread.join(max(0, int(timeout_s)))
    if thread.is_alive():
        raise TimeoutError(f"item timed out after {int(timeout_s)}s")
    if "exc" in err:
        raise err["exc"]
    return out.get("value")


class ScriptContext:
    def __init__(self, base: RunContext, task_name: str, allowed_shared_keys: set[str]) -> None:
        self._base = base
        self.task_name = task_name
        self._allowed_shared_keys = allowed_shared_keys

    @property
    def work_dir(self) -> Path:
        return self._base.work_dir

    @property
    def input_dir(self) -> Path:
        return self._base.input_dir

    @property
    def output_dir(self) -> Path:
        return self._base.output_dir

    @property
    def cache_dir(self) -> Path:
        return self._base.cache_dir

    @property
    def artifacts_dir(self) -> Path:
        return self._base.artifacts_dir

    @property
    def logs_dir(self) -> Path:
        return self._base.logs_dir

    @property
    def state(self) -> dict[str, Any]:
        return self._base.state

    def get_task_outputs(self, task_name: str) -> list[Any]:
        return list(self._base.state.get("task_outputs", {}).get(task_name, []))

    def set_shared(self, key: str, value: Any) -> None:
        if key not in self._allowed_shared_keys:
            raise ValueError(
                f"shared key '{key}' is not allowed for task {self.task_name}. "
                f"Allowed: {sorted(self._allowed_shared_keys)}"
            )
        shared = self._base.state.setdefault("shared", {})
        shared[key] = value

    def get_shared(self, key: str, default: Any = None) -> Any:
        return self._base.state.get("shared", {}).get(key, default)


@register_runner("python_fn")
class PythonFnRunner:
    def run(self, arg: dict[str, Any], spec: TaskSpec, ctx: RunContext) -> ItemResult:
        fn_path = spec.config.get("function")
        if not fn_path:
            raise ValueError(f"task {spec.name} requires config.function")
        fn = load_object(str(fn_path))
        try:
            output = _call_with_soft_timeout(
                lambda: _call_with_supported_signature(fn, arg=arg, spec=spec, ctx=ctx),
                spec.timeout,
            )
        except TimeoutError as exc:
            return ItemResult(
                item_id=str(arg.get("item_id", "")),
                state=ItemState.FAILED,
                success=False,
                error=str(exc),
                meta={"function": str(fn_path), "timeout": True, "timeout_seconds": spec.timeout},
            )
        return ItemResult(
            item_id=str(arg.get("item_id", "")),
            state=ItemState.SUCCESS,
            success=True,
            output=output,
            meta={"function": str(fn_path)},
        )


@register_runner("script")
class ScriptRunner:
    def run(self, arg: dict[str, Any], spec: TaskSpec, ctx: RunContext) -> ItemResult:
        fn_path = spec.config.get("function")
        if not fn_path:
            raise ValueError(f"task {spec.name} requires config.function")
        fn = load_object(str(fn_path))
        allowed = set(spec.config.get("context_write_keys", []))
        script_ctx = ScriptContext(ctx, task_name=spec.name, allowed_shared_keys=allowed)
        try:
            output = _call_with_soft_timeout(
                lambda: _call_with_supported_signature(fn, arg=arg, spec=spec, ctx=script_ctx),
                spec.timeout,
            )
        except TimeoutError as exc:
            return ItemResult(
                item_id=str(arg.get("item_id", "")),
                state=ItemState.FAILED,
                success=False,
                error=str(exc),
                meta={
                    "function": str(fn_path),
                    "context_write_keys": sorted(allowed),
                    "timeout": True,
                    "timeout_seconds": spec.timeout,
                },
            )
        return ItemResult(
            item_id=str(arg.get("item_id", "")),
            state=ItemState.SUCCESS,
            success=True,
            output=output,
            meta={"function": str(fn_path), "context_write_keys": sorted(allowed)},
        )


@register_runner("shell_cmd")
class ShellCmdRunner:
    def run(self, arg: dict[str, Any], spec: TaskSpec, ctx: RunContext) -> ItemResult:
        cmd = arg.get("cmd")
        if not cmd:
            cmd_template = spec.config.get("cmd_template")
            if not cmd_template:
                raise ValueError(f"task {spec.name} requires arg.cmd or config.cmd_template")
            mapping = _base_mapping(ctx, spec)
            local = {**mapping, **{k: str(v) for k, v in arg.items()}}
            cmd = _render(str(cmd_template), local)
        try:
            proc = subprocess.run(
                str(cmd),
                shell=True,
                executable="/bin/bash",
                cwd=str(arg.get("cwd", ctx.state.get("launch_cwd", str(ctx.work_dir)))),
                text=True,
                capture_output=True,
                timeout=spec.timeout,
            )
        except subprocess.TimeoutExpired as exc:
            return ItemResult(
                item_id=str(arg.get("item_id", "")),
                state=ItemState.FAILED,
                success=False,
                error=f"item timed out after {spec.timeout}s",
                output={
                    "cmd": str(cmd),
                    "returncode": None,
                    "stdout": exc.stdout,
                    "stderr": exc.stderr,
                },
                meta={"timeout": True, "timeout_seconds": spec.timeout},
            )
        output = {
            "cmd": str(cmd),
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
        if proc.returncode != 0:
            return ItemResult(
                item_id=str(arg.get("item_id", "")),
                state=ItemState.FAILED,
                success=False,
                output=output,
                error=f"Command failed with code {proc.returncode}",
            )
        return ItemResult(
            item_id=str(arg.get("item_id", "")),
            state=ItemState.SUCCESS,
            success=True,
            output=output,
        )
