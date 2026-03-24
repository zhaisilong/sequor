#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize uppercase outputs into CSV")
    parser.add_argument("--output_dir", default="tutorial/upper_case/output/upper")
    parser.add_argument("--summary_csv", default="tutorial/upper_case/output/summary.csv")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    summary_csv = Path(args.summary_csv)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)

    rows: list[tuple[str, int]] = []
    for path in sorted(output_dir.glob("*.txt")):
        text = path.read_text(encoding="utf-8")
        rows.append((path.name, len(text)))

    with summary_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["file_name", "chars"])
        for file_name, chars in rows:
            writer.writerow([file_name, chars])

    print(f"rows={len(rows)}")
    print(f"summary_csv={summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
