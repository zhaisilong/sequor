from __future__ import annotations

import json
from pathlib import Path


def summarize_rosetta(arg: dict, spec, ctx):
    rosetta_root = ctx.output_dir / "rosetta"
    case_dirs = sorted([p for p in rosetta_root.glob("*") if p.is_dir()])
    rows = []
    for case_dir in case_dirs:
        summary_file = case_dir / "summary.json"
        if not summary_file.exists():
            continue
        data = json.loads(summary_file.read_text(encoding="utf-8"))
        rows.append(
            {
                "case": case_dir.name,
                "best_score": data["best"]["score"],
                "best_tag": data["best"]["tag"],
                "nstruct": data["nstruct"],
            }
        )

    report_dir = ctx.output_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_json = report_dir / "rosetta_summary.json"
    report_tsv = report_dir / "rosetta_summary.tsv"
    report_json.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    lines = ["case\tbest_score\tbest_tag\tnstruct"]
    for r in rows:
        lines.append(f"{r['case']}\t{r['best_score']}\t{r['best_tag']}\t{r['nstruct']}")
    report_tsv.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {"cases": len(rows), "report_json": str(report_json), "report_tsv": str(report_tsv)}
