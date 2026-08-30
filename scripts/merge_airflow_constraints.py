#!/usr/bin/env python3
"""Replace named pins in an Airflow constraints file with an in-repo overlay."""

from __future__ import annotations

import argparse
import re
import urllib.request
from pathlib import Path


def package_key(name: str) -> str:
    """Return a canonical package name for constraint matching."""
    return re.sub(r"[-_.]+", "-", name).lower()


def merge_constraints(base_text: str, overlay_lines: list[str]) -> str:
    overlay: dict[str, str] = {}
    for raw in overlay_lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        name, sep, _version = line.partition("==")
        if not sep:
            raise ValueError(f"overlay pin must be name==version: {line!r}")
        key = package_key(name)
        if key in overlay:
            raise ValueError(f"duplicate overlay pin for {name!r}")
        overlay[key] = line
    merged: list[str] = []
    seen: set[str] = set()
    for line in base_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "==" not in stripped:
            merged.append(line)
            continue
        name = stripped.split("==", 1)[0]
        key = package_key(name)
        if key in overlay:
            merged.append(overlay[key])
            seen.add(key)
        else:
            merged.append(line)
    for key, line in overlay.items():
        if key not in seen:
            merged.append(line)
    return "\n".join(merged) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--overlay", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with urllib.request.urlopen(args.base_url, timeout=60) as response:
        base_text = response.read().decode("utf-8")
    overlay_lines = args.overlay.read_text(encoding="utf-8").splitlines()
    args.output.write_text(merge_constraints(base_text, overlay_lines), encoding="utf-8")


if __name__ == "__main__":
    main()
