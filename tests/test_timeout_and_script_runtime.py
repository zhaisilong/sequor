from __future__ import annotations

import textwrap
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sequor.io.config_loader import load_flow_config


class TimeoutAndScriptRuntimeTests(unittest.TestCase):
    def _write_pipeline(self, root: Path, name: str, body: str) -> Path:
        p = root / f"{name}.yaml"
        p.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
        return p

    def test_shell_cmd_item_timeout_fails(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            pipe = self._write_pipeline(
                root,
                "shell_timeout",
                """
                flow:
                  name: shell_timeout
                  fail_fast: true
                  parallelism: 1
                  work_dir: .sequor

                tasks:
                  - name: slow_shell
                    task_type: shell_cmd
                    timeout: 1
                    cache: false
                    config:
                      default_item:
                        item_id: one
                        cmd: "sleep 2"
                """,
            )
            flow = load_flow_config(pipe)
            res = flow.run()
            tr = res.task_results["slow_shell"]
            self.assertEqual(str(tr.state), "FAILED")
            self.assertEqual(tr.stats["failed"], 1)
            self.assertIn("timed out", (tr.item_results[0].error or "").lower())

    def test_python_fn_soft_timeout_fails_item(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            pipe = self._write_pipeline(
                root,
                "python_timeout",
                """
                flow:
                  name: python_timeout
                  fail_fast: true
                  parallelism: 1
                  work_dir: .sequor

                tasks:
                  - name: slow_python
                    task_type: python_fn
                    timeout: 1
                    cache: false
                    config:
                      function: tests.helpers.sleep_python_fn
                      default_item:
                        item_id: one
                        sleep: 2
                """,
            )
            flow = load_flow_config(pipe)
            res = flow.run()
            tr = res.task_results["slow_python"]
            self.assertEqual(str(tr.state), "FAILED")
            self.assertEqual(tr.stats["failed"], 1)
            self.assertIn("timed out", (tr.item_results[0].error or "").lower())

    def test_script_task_does_not_write_task_manifest_or_log(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            pipe = self._write_pipeline(
                root,
                "script_no_record",
                """
                flow:
                  name: script_no_record
                  fail_fast: true
                  parallelism: 1
                  work_dir: .sequor

                tasks:
                  - name: hook
                    task_type: script
                    timeout: 10
                    cache: true
                    config:
                      function: tests.helpers.script_emit
                      default_item:
                        item_id: one
                        value: ok
                """,
            )
            flow = load_flow_config(pipe)
            res = flow.run()
            tr = res.task_results["hook"]
            self.assertEqual(str(tr.state), "SUCCESS")

            work = root / ".sequor" / "script_no_record"
            self.assertFalse((work / "tasks" / "hook").exists())
            self.assertFalse((work / "logs" / "hook.log").exists())


if __name__ == "__main__":
    unittest.main()
