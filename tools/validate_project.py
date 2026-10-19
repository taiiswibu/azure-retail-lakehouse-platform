#!/usr/bin/env python3
"""Fast, cloud-free structural validation for CI and handoff."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = [
    "README.md",
    "databricks.yml",
    "infra/main.bicep",
    "resources/jobs.yml",
    "notebooks/01_bronze_batch.py",
    "notebooks/02_bronze_stream.py",
    "notebooks/03_silver.py",
    "notebooks/04_gold.py",
    "notebooks/05_quality.py",
    "docs/DEPLOYMENT_VI.md",
]
SECRET_PATTERNS = [
    re.compile(r"SharedAccessKey=[A-Za-z0-9+/=]{20,}"),
    re.compile(r"AccountKey=[A-Za-z0-9+/=]{20,}"),
]


def main() -> int:
    errors: list[str] = []
    for relative in REQUIRED:
        if not (ROOT / relative).is_file():
            errors.append(f"Missing required file: {relative}")

    for path in ROOT.rglob("*.json"):
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"Invalid JSON: {path.relative_to(ROOT)}: {exc}")

    for path in [ROOT / "databricks.yml", ROOT / "resources/jobs.yml"]:
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"Invalid YAML: {path.relative_to(ROOT)}: {exc}")

    text_extensions = {".py", ".md", ".yml", ".yaml", ".json", ".sql", ".bicep", ".ps1"}
    for path in ROOT.rglob("*"):
        if path.is_file() and path.suffix.lower() in text_extensions and ".venv" not in path.parts:
            content = path.read_text(encoding="utf-8", errors="ignore")
            for pattern in SECRET_PATTERNS:
                if pattern.search(content):
                    errors.append(f"Potential credential committed in {path.relative_to(ROOT)}")

    if errors:
        print("Validation failed:")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("Project structure, JSON, YAML, and secret scan passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

