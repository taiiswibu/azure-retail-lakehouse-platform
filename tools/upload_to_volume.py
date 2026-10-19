#!/usr/bin/env python3
"""Upload generated batch files to a Unity Catalog external volume with Databricks CLI."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, nargs="?", default=Path("data/generated/batch"))
    parser.add_argument("--catalog", default="retail_dev")
    parser.add_argument("--profile", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = args.source.resolve()
    if not source.is_dir():
        raise SystemExit(f"Source directory does not exist: {source}")
    if not shutil.which("databricks"):
        raise SystemExit("Databricks CLI is not installed or not available on PATH")
    destination_root = f"dbfs:/Volumes/{args.catalog}/landing/files"
    base_command = ["databricks"]
    if args.profile:
        base_command.extend(["--profile", args.profile])

    files = sorted(path for path in source.rglob("*") if path.is_file())
    if not files:
        raise SystemExit(f"No files found under: {source}")

    directories = sorted({path.relative_to(source).parent.as_posix() for path in files})
    for relative_directory in directories:
        if relative_directory == ".":
            continue
        command = [*base_command, "fs", "mkdirs", f"{destination_root}/{relative_directory}"]
        print(" ".join(command))
        if not args.dry_run:
            subprocess.run(command, check=True)

    for path in files:
        relative = path.relative_to(source).as_posix()
        command = [
            *base_command,
            "fs",
            "cp",
            str(path),
            f"{destination_root}/{relative}",
            "--overwrite",
        ]
        print(" ".join(command))
        if not args.dry_run:
            subprocess.run(command, check=True)


if __name__ == "__main__":
    main()

