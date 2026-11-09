#!/usr/bin/env python3
"""Bind or preview metadata locally and export a reviewed lifecycle definition.

No command authenticates or submits SQL/Fabric operations.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "deployment" / "src"))

from fabric_medallion_deployer.configuration import (
    bind_configuration,
    configuration_receipt,
    lifecycle_pipeline_definition,
    load_configuration_file,
)
from fabric_medallion_deployer.metadata import (
    canonical_json,
    compile_run_plan,
    metadata_digest,
)
from fabric_medallion_deployer.orchestrator import bootstrap_configuration
from fabric_medallion_deployer.use_cases import load_use_case

LOGGER = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--use-case")
    commands = parser.add_subparsers(dest="command", required=True)
    bind = commands.add_parser("bind")
    bind.add_argument("--bindings", type=Path, required=True)
    bind.add_argument("--configuration", type=Path)
    bind.add_argument("--output", type=Path, required=True)
    bind.add_argument("--version")
    bind.add_argument("--use-case", default=argparse.SUPPRESS)
    preview = commands.add_parser("preview")
    preview.add_argument("--configuration", type=Path, required=True)
    preview.add_argument("--run-id", required=True)
    preview.add_argument("--observed-utc", required=True)
    preview.add_argument("--use-case", default=argparse.SUPPRESS)
    export = commands.add_parser("export-operation")
    export.add_argument("--configuration", type=Path, required=True)
    export.add_argument("--bindings", type=Path, required=True)
    export.add_argument(
        "--operation",
        choices=("register", "validate", "approve", "activate", "rollback", "preview"),
        required=True,
    )
    export.add_argument("--validation-evidence", type=Path)
    export.add_argument("--validator-sha256")
    export.add_argument("--expected-active-version")
    export.add_argument("--approval")
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--use-case", default=argparse.SUPPRESS)
    return parser


def _write_new(path: Path, document: object) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(canonical_json(document) + "\n")


def main() -> int:
    args = create_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        profile = load_use_case(ROOT, args.use_case)
        config = (
            load_configuration_file(args.configuration)
            if args.configuration
            else bootstrap_configuration(ROOT, use_case=profile.case_id)
        )
        if config.to_document()["extensions"].get("use_case_id") != profile.case_id:
            raise ValueError("Configuration belongs to a different use case")
        if args.command == "bind":
            bindings = json.loads(args.bindings.read_text(encoding="utf-8"))
            if set(bindings) != {
                "workspace_id",
                "source_database_ids",
                "connection_ids",
            }:
                raise ValueError(
                    "Binding input fields differ from the supported contract"
                )
            version = args.version or (
                f"{profile.case_id}-bound-"
                + metadata_digest(
                    {
                        "definition_sha256": config.definition_sha256,
                        "bindings": bindings,
                    }
                )[:32]
            )
            bound = bind_configuration(config, **bindings, version_id=version)
            _write_new(args.output, bound.to_document())
            print(canonical_json(configuration_receipt(bound)))
        elif args.command == "preview":
            plan = compile_run_plan(config, args.run_id, args.observed_utc)
            print(plan.snapshot_json)
        else:
            bindings = json.loads(args.bindings.read_text(encoding="utf-8"))
            if set(bindings) != {
                "workspace_id",
                "control_database_id",
                "control_connection_id",
            }:
                raise ValueError(
                    "Control binding input fields differ from the supported contract"
                )
            definition = lifecycle_pipeline_definition(
                config,
                **bindings,
                operation=args.operation,
                validation_evidence=json.loads(
                    args.validation_evidence.read_text(encoding="utf-8")
                )
                if args.validation_evidence
                else None,
                validator_sha256=args.validator_sha256,
                expected_active_version=args.expected_active_version,
                activation_approval=args.approval,
            )
            _write_new(args.output, definition)
            LOGGER.info(
                "Exported %s; no SQL or Fabric operation was submitted", args.operation
            )
    except (OSError, ValueError, TypeError) as error:
        LOGGER.error("%s", error)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
