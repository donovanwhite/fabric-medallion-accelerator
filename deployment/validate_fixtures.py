#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Thin compatibility API for the selected case's declared fixture validator."""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from deployment.generate_fixtures import load_fixture_module
from deployment.generate_fixtures import main as builder_main


def __getattr__(name: str) -> object:
    """Re-export named validator APIs without assuming any particular source case."""
    if name.startswith("_"):
        raise AttributeError(name)
    return getattr(load_fixture_module("validator"), name)


def main(argv: list[str] | None = None) -> int:
    """Launch the selected fixture adapter in validate-only mode."""
    args = sys.argv[1:] if argv is None else argv
    return builder_main(["--validate-only", *args])


if __name__ == "__main__":
    raise SystemExit(main())
