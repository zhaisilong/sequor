from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from p_tqdm import p_map
from tqdm import tqdm

from sequor.core.models import ItemResult
from sequor.core.registry import register_executor


class BaseExecutor:
    def run(
        self, args_list: list[dict], fn: Callable[[dict], ItemResult]
    ) -> list[ItemResult]:
        raise NotImplementedError


@register_executor("serial")
class SerialExecutor(BaseExecutor):
    def run(
        self, args_list: list[dict], fn: Callable[[dict], ItemResult]
    ) -> list[ItemResult]:
        return [fn(arg) for arg in tqdm(args_list, total=len(args_list))]


@register_executor("parallel")
class ParallelExecutor(BaseExecutor):
    DEFAULT_MAX_WORKERS = 4

    def __init__(self, max_workers: int | None = None) -> None:
        self.max_workers = max_workers

    def run(
        self, args_list: list[dict], fn: Callable[[dict], ItemResult]
    ) -> list[ItemResult]:
        configured = self.max_workers or self.DEFAULT_MAX_WORKERS
        workers = min(max(1, int(configured)), max(1, len(args_list)))
        if not args_list:
            return []
        if workers <= 1:
            return [fn(arg) for arg in args_list]
        try:
            return p_map(fn, args_list, num_cpus=workers)
        except Exception:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                return list(pool.map(fn, args_list))
