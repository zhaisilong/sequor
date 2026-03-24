from __future__ import annotations
from pathlib import Path


def item_id_from_input(arg: dict) -> str:
    return Path(str(arg["input_name"])).stem


def uppercase_file(arg: dict, spec, ctx):
    src = Path(str(arg["input_file"]))
    dst = Path(str(arg["output_file"]))
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
