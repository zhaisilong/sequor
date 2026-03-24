from __future__ import annotations

import argparse
import sys

from sequor.core.runner_summary import format_summary
from sequor.io.config_loader import load_flow_config


def _cmd_validate(path: str) -> int:
    _ = load_flow_config(path)
    print(f"VALID: {path}")
    return 0


def _cmd_list_tasks(path: str) -> int:
    flow = load_flow_config(path)
    print(f"Flow: {flow.name}")
    for t in flow.tasks:
        deps = ",".join(t.depends_on) if t.depends_on else "-"
        print(f"- {t.name} (builder={t.builder}, processor={t.processor}, executor={t.executor or 'serial'}, deps={deps})")
    return 0


def _cmd_run(path: str, work_dir: str | None = None) -> int:
    flow = load_flow_config(path)
    if work_dir:
        flow.work_dir = work_dir
    res = flow.run()
    print(format_summary(res))
    return 0 if str(res.state) == "SUCCESS" else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sequor")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run a pipeline")
    p_run.add_argument("pipeline")
    p_run.add_argument("--work-dir", default=None, help="Override flow.work_dir from YAML")

    p_validate = sub.add_parser("validate", help="Validate a pipeline")
    p_validate.add_argument("pipeline")

    p_list = sub.add_parser("list-tasks", help="List tasks from a pipeline")
    p_list.add_argument("pipeline")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "validate":
        return _cmd_validate(args.pipeline)
    if args.cmd == "list-tasks":
        return _cmd_list_tasks(args.pipeline)
    if args.cmd == "run":
        return _cmd_run(args.pipeline, work_dir=args.work_dir)

    parser.error(f"Unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
