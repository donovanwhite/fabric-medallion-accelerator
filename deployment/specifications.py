#!/usr/bin/env python3
"""Validate or compile case Markdown; never deploy or activate generated drafts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "deployment" / "src"))

from fabric_medallion_deployer.spec_compiler import (
    compile_use_case,
    validate_case_documents,
)

LOGGER = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "compile"))
    parser.add_argument("--use-case", required=True)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Reject stale generated files without writing",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        if args.command == "validate":
            profile, policy, *_documents = validate_case_documents(ROOT, args.use_case)
            LOGGER.info(
                "Validated case %s against baseline %s; no generation or cloud calls",
                profile.case_id,
                policy["baseline_version"],
            )
        else:
            receipt = compile_use_case(ROOT, args.use_case, check=args.check)
            print(
                json.dumps(
                    {key: value for key, value in receipt.items() if key != "files"},
                    indent=2,
                )
            )
    except (OSError, ValueError, TypeError, KeyError) as error:
        LOGGER.error("Specification compilation blocked: %s", error)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
