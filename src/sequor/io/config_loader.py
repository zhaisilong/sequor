from __future__ import annotations

from pathlib import Path

from sequor.core.flow import Flow
from sequor.io.yaml_parser import parse_yaml_file


def load_flow_config(path: str | Path) -> Flow:
    return parse_yaml_file(path)
