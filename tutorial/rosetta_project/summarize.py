from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize mock Rosetta outputs")
    parser.add_argument(
        "--rosetta_dir",
        default="tutorial/rosetta_project/output/rosetta",
        help="Directory containing per-case rosetta outputs",
    )
    parser.add_argument(
        "--report_dir",
        default="tutorial/rosetta_project/output/reports",
        help="Directory to write summary reports",
    )
    args = parser.parse_args()

    rosetta_dir = Path(args.rosetta_dir).resolve()
    report_dir = Path(args.report_dir).resolve()
    report_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    if rosetta_dir.exists():
        for case_dir in sorted(p for p in rosetta_dir.iterdir() if p.is_dir()):
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

    report_json = report_dir / "rosetta_summary.json"
    report_tsv = report_dir / "rosetta_summary.tsv"
    report_json.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    lines = ["case\tbest_score\tbest_tag\tnstruct"]
    for row in rows:
        lines.append(f"{row['case']}\t{row['best_score']}\t{row['best_tag']}\t{row['nstruct']}")
    report_tsv.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"wrote {len(rows)} rows -> {report_tsv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
