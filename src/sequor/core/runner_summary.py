from __future__ import annotations

from sequor.core.models import FlowResult


def format_summary(result: FlowResult) -> str:
    lines = [f"Flow: {result.flow_name} ({result.state})", f"Run ID: {result.run_id}"]
    for name, tr in result.task_results.items():
        st = tr.stats or {}
        lines.append(
            f"- {name}: {tr.state} (total={st.get('total', 0)} success={st.get('success', 0)} cached={st.get('cached', 0)} failed={st.get('failed', 0)})"
        )
    if result.work_dir:
        lines.append(f"Work Dir: {result.work_dir}")
    return "\n".join(lines)
