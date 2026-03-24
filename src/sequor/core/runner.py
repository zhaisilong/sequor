from __future__ import annotations

import traceback
from datetime import datetime, timezone
import re
from typing import Any

from sequor.core.artifacts import write_manifest
from sequor.core.cache import CacheStore
from sequor.core.context import RunContext
from sequor.core.logger import create_logger, log_json
from sequor.core.models import ItemResult, TaskResult, TaskSpec
from sequor.core.registry import get_builder, get_executor, get_processor
from sequor.core.states import ItemState, TaskState
from sequor.utils.hashing import sha256_obj


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _processor_identity(processor: Any, spec: TaskSpec) -> str:
    cls = processor.__class__
    return spec.config.get("processor_version") or f"{cls.__module__}.{cls.__name__}"


def _item_key(task_name: str, arg: dict, config: dict, processor_id: str) -> str:
    key_arg = {k: v for k, v in arg.items() if k != "run_dir"}
    return sha256_obj(
        {
            "task": task_name,
            "arg": key_arg,
            "config": config,
            "processor": processor_id,
        }
    )


def _safe_item_id(raw: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._")
    if not safe:
        return "item"
    return safe[:120]


class Runner:
    def __init__(self, ctx: RunContext) -> None:
        self.ctx = ctx
        self.cache = CacheStore(ctx.cache_dir)

    def run_task(self, spec: TaskSpec) -> TaskResult:
        started_at = _utc_now()
        task_result = TaskResult(task_name=spec.name, state=TaskState.RUNNING, started_at=started_at)
        task_logger = create_logger(self.ctx.logs_dir / f"{spec.name}.log")

        if not spec.enabled:
            task_result.state = TaskState.SKIPPED
            task_result.finished_at = _utc_now()
            task_result.stats = {"total": 0, "success": 0, "failed": 0, "cached": 0}
            return task_result

        builder = get_builder(spec.builder)()
        processor = get_processor(spec.processor)()
        executor_name = spec.executor or "serial"
        executor = get_executor(executor_name)()

        args_list = builder.build(spec, self.ctx)
        processor_id = _processor_identity(processor, spec)

        def run_one(arg: dict) -> ItemResult:
            raw_id = str(arg.get("item_id") or arg.get("input_name") or sha256_obj(arg)[:8])
            item_id = _safe_item_id(raw_id)
            item_dir = self.ctx.work_dir / "tasks" / spec.name / "items" / item_id
            item_dir.mkdir(parents=True, exist_ok=True)
            arg = {**arg, "item_id": item_id, "run_dir": str(item_dir)}
            key = _item_key(spec.name, arg, spec.config, processor_id)
            if spec.cache and self.cache.exists(key):
                cached = self.cache.get(key) or {}
                res = ItemResult(
                    item_id=item_id,
                    state=ItemState.CACHED,
                    success=True,
                    cached=True,
                    output=cached.get("output"),
                    artifacts=cached.get("artifacts", []),
                    meta={"cache_key": key, "cache_hit": True, "cached_from": cached.get("cached_at")},
                    started_at=_utc_now(),
                    finished_at=_utc_now(),
                )
                write_manifest(item_dir / "manifest.json", {"state": res.state, "cached": True, "meta": res.meta, "arg": arg})
                return res

            started = _utc_now()
            try:
                res = processor.process(arg=arg, spec=spec, ctx=self.ctx)
                res.started_at = res.started_at or started
                res.finished_at = res.finished_at or _utc_now()
                if spec.cache and res.success:
                    payload = {
                        "task_name": spec.name,
                        "item_id": item_id,
                        "output": res.output,
                        "artifacts": res.artifacts,
                        "meta": res.meta,
                        "cached_at": _utc_now(),
                    }
                    self.cache.set(
                        key,
                        payload,
                        index_entry={
                            "run_id": self.ctx.run_id,
                            "task_name": spec.name,
                            "item_id": item_id,
                            "cache_key": key,
                            "state": res.state,
                        },
                    )
                write_manifest(
                    item_dir / "manifest.json",
                    {
                        "state": str(res.state),
                        "success": res.success,
                        "cached": res.cached,
                        "error": res.error,
                        "arg": arg,
                        "output": res.output,
                        "artifacts": res.artifacts,
                        "meta": res.meta,
                        "started_at": res.started_at,
                        "finished_at": res.finished_at,
                    },
                )
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
                write_manifest(item_dir / "manifest.json", {"state": "FAILED", "arg": arg, "error": err})
                return res

        results = executor.run(args_list, run_one)
        task_result.item_results = results
        task_result.finished_at = _utc_now()
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

        write_manifest(
            self.ctx.work_dir / "tasks" / spec.name / "task_manifest.json",
            {
                "task": spec.name,
                "state": str(task_result.state),
                "stats": task_result.stats,
                "started_at": task_result.started_at,
                "finished_at": task_result.finished_at,
            },
        )
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
