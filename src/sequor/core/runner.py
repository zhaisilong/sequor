from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from typing import Any

from sequor.core.artifacts import write_manifest
from sequor.core.context import RunContext
from sequor.core.logger import create_logger, log_json
from sequor.core.models import ItemResult, TaskResult, TaskSpec
from sequor.core.registry import get_executor, get_planner, get_runner
from sequor.core.states import ItemState, TaskState
from sequor.utils.hashing import sha256_obj


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_item_id(item_id: str) -> str:
    value = item_id.strip()
    if not value:
        raise ValueError("item_id cannot be empty")
    if any(ch.isspace() for ch in value):
        raise ValueError(f"item_id contains whitespace: {item_id!r}")
    return value


class Runner:
    def __init__(self, ctx: RunContext) -> None:
        self.ctx = ctx

    def run_task(self, spec: TaskSpec) -> TaskResult:
        task_result = TaskResult(task_name=spec.name, state=TaskState.RUNNING, started_at=_utc_now())

        if not spec.enabled:
            task_result.state = TaskState.SKIPPED
            task_result.finished_at = _utc_now()
            task_result.stats = {"total": 0, "success": 0, "failed": 0, "cached": 0}
            return task_result

        planner_name = spec.planner or spec.task_type
        runner_name = spec.runner or spec.task_type
        is_script_task = runner_name == "script" or spec.task_type == "script"
        task_logger = None if is_script_task else create_logger(self.ctx.logs_dir / f"{spec.name}.log")
        planner = get_planner(planner_name)()
        item_runner = get_runner(runner_name)()
        planned = planner.plan(spec, self.ctx)
        if not isinstance(planned, list):
            raise ValueError(f"planner {planner_name} must return list, got {type(planned)}")

        seen: set[str] = set()
        normalized_items: list[dict[str, Any]] = []
        for idx, item in enumerate(planned):
            if not isinstance(item, dict):
                raise ValueError(f"task {spec.name} planned item #{idx} must be dict")
            if "item_id" not in item:
                raise ValueError(f"task {spec.name} planned item #{idx} missing required item_id")
            item_id = _normalize_item_id(str(item["item_id"]))
            if item_id in seen:
                raise ValueError(f"task {spec.name} has duplicate item_id: {item_id}")
            seen.add(item_id)
            arg = item.get("arg")
            if arg is None:
                arg = {k: v for k, v in item.items() if k != "item_id"}
            if not isinstance(arg, dict):
                raise ValueError(f"task {spec.name} item {item_id} arg must be dict")
            normalized_items.append({"item_id": item_id, "arg": dict(arg)})

        workers = spec.parallelism
        if workers is None:
            workers = int(self.ctx.state.get("flow_parallelism", 4))
        workers = max(1, int(workers))
        executor_cls = get_executor("parallel" if workers > 1 else "serial")
        executor = executor_cls(max_workers=workers) if workers > 1 else executor_cls()

        task_root = self.ctx.work_dir / "tasks" / spec.name
        manifests_dir = task_root / "manifests"
        if not is_script_task:
            manifests_dir.mkdir(parents=True, exist_ok=True)

        def _cache_fingerprint(arg: dict[str, Any]) -> str:
            key_arg = {k: v for k, v in arg.items() if k != "run_dir"}
            return sha256_obj({"arg": key_arg, "runner": runner_name, "task_type": spec.task_type})

        def run_one(item: dict[str, Any]) -> ItemResult:
            item_id = item["item_id"]
            arg = dict(item["arg"])
            arg["item_id"] = item_id

            manifest_path = manifests_dir / f"{item_id}.json" if not is_script_task else None
            cache_fingerprint = _cache_fingerprint(arg)
            if spec.cache and manifest_path is not None and manifest_path.exists():
                try:
                    prev = json.loads(manifest_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    prev = {}
                if (
                    prev.get("success") is True
                    and str(prev.get("cache_fingerprint", "")) == cache_fingerprint
                ):
                    return ItemResult(
                        item_id=item_id,
                        state=ItemState.CACHED,
                        success=True,
                        cached=True,
                        output=prev.get("output"),
                        artifacts=prev.get("artifacts", []),
                        meta={
                            "cache_hit": True,
                            "cache_source": str(manifest_path),
                            "cache_fingerprint": cache_fingerprint,
                        },
                        started_at=_utc_now(),
                        finished_at=_utc_now(),
                    )

            started = _utc_now()
            try:
                out = item_runner.run(arg=arg, spec=spec, ctx=self.ctx)
                if isinstance(out, ItemResult):
                    res = out
                else:
                    res = ItemResult(
                        item_id=item_id,
                        state=ItemState.SUCCESS,
                        success=True,
                        output=out,
                    )
                res.item_id = str(res.item_id or item_id)
                res.started_at = res.started_at or started
                res.finished_at = res.finished_at or _utc_now()
                payload = {
                    "item_id": item_id,
                    "state": str(res.state),
                    "success": res.success,
                    "cached": res.cached,
                    "error": res.error,
                    "arg": arg,
                    "output": res.output,
                    "artifacts": res.artifacts,
                    "meta": res.meta,
                    "runner": runner_name,
                    "cache_fingerprint": cache_fingerprint,
                    "started_at": res.started_at,
                    "finished_at": res.finished_at,
                }
                if manifest_path is not None:
                    write_manifest(manifest_path, payload)
                if task_logger is not None:
                    log_json(
                        task_logger,
                        {
                            "event": "item_finished",
                            "task": spec.name,
                            "item_id": item_id,
                            "state": str(res.state),
                            "cached": res.cached,
                            "error": res.error,
                        },
                    )
                return res
            except Exception as exc:
                finished = _utc_now()
                err = "".join(traceback.format_exception_only(type(exc), exc)).strip()
                res = ItemResult(
                    item_id=item_id,
                    state=ItemState.FAILED,
                    success=False,
                    error=err,
                    meta={"traceback": traceback.format_exc()},
                    started_at=started,
                    finished_at=finished,
                )
                if manifest_path is not None:
                    write_manifest(
                        manifest_path,
                        {
                            "item_id": item_id,
                            "state": "FAILED",
                            "success": False,
                            "cached": False,
                            "arg": arg,
                            "error": err,
                            "runner": runner_name,
                            "cache_fingerprint": cache_fingerprint,
                            "started_at": started,
                            "finished_at": finished,
                        },
                    )
                return res

        results = executor.run(normalized_items, run_one)
        task_result.item_results = results
        task_result.finished_at = _utc_now()
        task_outputs = [
            r.output for r in results if r.state in {ItemState.SUCCESS, ItemState.CACHED}
        ]
        self.ctx.state.setdefault("task_outputs", {})[spec.name] = task_outputs

        total = len(results)
        success = sum(1 for r in results if r.state in {ItemState.SUCCESS, ItemState.CACHED})
        failed = sum(1 for r in results if r.state == ItemState.FAILED)
        cached = sum(1 for r in results if r.state == ItemState.CACHED)
        task_result.stats = {"total": total, "success": success, "failed": failed, "cached": cached}

        if total == 0:
            task_result.state = TaskState.SKIPPED
        elif failed > 0:
            task_result.state = TaskState.FAILED
        elif cached == total:
            task_result.state = TaskState.CACHED
        else:
            task_result.state = TaskState.SUCCESS

        if not is_script_task:
            items_index: dict[str, dict[str, Any]] = {}
            for res in results:
                item_id = str(res.item_id)
                items_index[item_id] = {
                    "manifest": f"manifests/{item_id}.json",
                    "state": str(res.state),
                    "success": res.success,
                    "cached": res.cached,
                    "error": res.error,
                }
            write_manifest(
                task_root / "task_manifest.json",
                {
                    "task": spec.name,
                    "task_type": spec.task_type,
                    "planner": planner_name,
                    "runner": runner_name,
                    "parallelism": workers,
                    "state": str(task_result.state),
                    "stats": task_result.stats,
                    "items": items_index,
                    "started_at": task_result.started_at,
                    "finished_at": task_result.finished_at,
                },
            )
            if task_logger is not None:
                log_json(
                    task_logger,
                    {
                        "event": "task_finished",
                        "task": spec.name,
                        "state": str(task_result.state),
                        "stats": task_result.stats,
                    },
                )
        return task_result
