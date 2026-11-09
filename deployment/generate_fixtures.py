#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Compatibility entry point for the selected case's declared local fixture builder."""

from __future__ import annotations

import argparse
import importlib
import re
import sys
from pathlib import Path
from types import ModuleType

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def load_fixture_module(
    role: str = "builder",
    *,
    root: Path | None = None,
    use_case: str | None = None,
) -> ModuleType:
    """Load an explicit case-owned adapter, never a fallback for fixture-free cases."""
    if role not in {"builder", "validator"}:
        raise ValueError(f"Unknown fixture adapter role: {role}")
    # Keep direct script and historical ``sys.path`` imports dependency-free.
    for path in (_REPOSITORY_ROOT, _REPOSITORY_ROOT / "deployment" / "src"):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    from fabric_medallion_deployer.asset_paths import (
        resolve_asset_path,
        use_case_descriptor,
    )

    root = _REPOSITORY_ROOT if root is None else root.resolve()
    descriptor = use_case_descriptor(root, use_case)
    name = descriptor.get(f"fixture_{role}_module")
    if name is None:
        raise ValueError(
            f"Use case {descriptor['id']} does not declare a local fixture {role}"
        )
    if not isinstance(name, str) or not re.fullmatch(
        r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", name
    ):
        raise ValueError(f"Invalid fixture {role} module")
    key = "generator_sha256" if role == "builder" else "validator_sha256"
    relative = descriptor.get("provenance_assets", {}).get(key)
    if relative is None:
        raise ValueError(f"Fixture {role} has no declared provenance asset")
    expected = resolve_asset_path(root, relative, descriptor["id"])
    module = importlib.import_module(name)
    filename = getattr(module, "__file__", None)
    if filename is None or Path(filename).resolve() != expected:
        raise ValueError(f"Fixture {role} module differs from its declared asset")
    return module


def __getattr__(name: str) -> object:
    """Preserve historical named imports through the currently selected adapter."""
    if name.startswith("_"):
        raise AttributeError(name)
    return getattr(load_fixture_module(), name)


def main(argv: list[str] | None = None) -> int:
    """Consume only case selection and forward all other options to its builder."""
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--use-case")
    selected, remaining = parser.parse_known_args(argv)
    try:
        module = load_fixture_module(use_case=selected.use_case)
    except (ImportError, OSError, ValueError) as error:
        print(f"Fixture adapter error: {error}", file=sys.stderr)
        return 2
    original = sys.argv
    try:
        sys.argv = [str(module.__file__), *remaining]
        return module.main()
    finally:
        sys.argv = original


if __name__ == "__main__":
    raise SystemExit(main())
