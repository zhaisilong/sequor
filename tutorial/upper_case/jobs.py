from __future__ import annotations

import json
from pathlib import Path

SENTENCES = [
    "sequor runs local workflows.",
    "this tutorial demonstrates dag execution.",
    "cache makes reruns much faster.",
    "every item keeps traceable artifacts.",
    "python and bash tasks can be mixed.",
    "parallel executor handles independent items.",
]


def generate_inputs(arg: dict, spec, ctx):
    cfg = spec.config
    count = int(cfg.get("count", 12))
    source_dir = ctx.input_dir / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        p = source_dir / f"sample_{i:02d}.txt"
        lines = [
            SENTENCES[i % len(SENTENCES)],
            SENTENCES[(i + 1) % len(SENTENCES)],
            SENTENCES[(i + 2) % len(SENTENCES)],
        ]
        p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"generated": count, "dir": str(source_dir)}


def uppercase_file(arg: dict, spec, ctx):
    src = Path(arg["input_file"])
    dst = Path(arg["output_file"])
    dst.parent.mkdir(parents=True, exist_ok=True)
    text = src.read_text(encoding="utf-8")
    out = text.upper()
    dst.write_text(out, encoding="utf-8")
    return {
        "input": str(src),
        "output": str(dst),
        "chars_in": len(text),
        "chars_out": len(out),
    }


def summarize(arg: dict, spec, ctx):
    upper_dir = ctx.output_dir / "upper"
    files = sorted(upper_dir.glob("*.txt"))
    total_chars = 0
    details = []
    for p in files:
        txt = p.read_text(encoding="utf-8")
        total_chars += len(txt)
        details.append({"file": p.name, "chars": len(txt)})

    summary_dir = ctx.output_dir / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    report_txt = summary_dir / "report.txt"
    report_json = summary_dir / "report.json"

    report_txt.write_text(
        "\n".join(
            [
                "UPPER CASE TUTORIAL SUMMARY",
                f"files={len(files)}",
                f"total_chars={total_chars}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    report_json.write_text(json.dumps({"files": len(files), "total_chars": total_chars, "details": details}, indent=2), encoding="utf-8")
    return {"report_txt": str(report_txt), "report_json": str(report_json), "files": len(files)}
