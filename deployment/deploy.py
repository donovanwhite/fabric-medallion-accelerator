#!/usr/bin/env python3
"""Repository-local entry point for the Fabric deployment package."""

from __future__ import annotations

import sys
from pathlib import Path

DEPLOYMENT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(DEPLOYMENT_ROOT / "src"))

from fabric_medallion_deployer.cli import main

if __name__ == "__main__":
    sys.exit(main())
