"""Local validation and explicit workspace deployment for selected use cases."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .deployer import (
    AcceleratorDeployer,
    DeploymentOptions,
    validate_local,
)
from .orchestrator import bootstrap_configuration, orchestrator_database_definition
from .use_cases import load_use_case

LOGGER = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Case-selected metadata medallion accelerator"
    )
    parser.add_argument("--use-case")
    parser.add_argument(
        "--source-connection",
        action="append",
        default=[],
        metavar="CODE=NAME",
        help="Override a selected case source connection; repeat for distinct source codes",
    )
    parser.add_argument(
        "--metadata-assets",
        action="store_true",
        help="Publish reusable runtime assets (the default); no source reseeding, activation or jobs",
    )
    parser.add_argument(
        "--workspace", help="Existing workspace name or ID, e.g. ws_medallion"
    )
    parser.add_argument(
        "--validate-local", action="store_true", help="Validate without cloud access"
    )
    for source in ("bas", "csd", "persal"):
        parser.add_argument(
            f"--{source}-sql-connection",
            help="Legacy explicit connection override; otherwise use the case declaration",
        )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--bootstrap-orchestrator",
        action="store_true",
        help="Bootstrap additive db_orchestrator and inactive draft (plus generic assets only with --metadata-assets); never reseed source databases",
    )
    parser.add_argument(
        "--orchestrator-sql-connection", default="conn_orchestrator_sql"
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Guide TTY connection setup and recheck",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        help="Persistent control-bootstrap or generic-publication operation journal",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reconcile the specified mode-specific bootstrap/publication journal",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read-only cloud preflight; no item updates",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def repository_root() -> Path:
    for parent in (Path.cwd(), *Path(__file__).resolve().parents):
        if (parent / "use_cases" / "catalog.json").is_file():
            return parent
    raise FileNotFoundError("Run from the cloned accelerator repository")


def main() -> int:
    args = create_parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    try:
        root = repository_root()
        profile = load_use_case(root, args.use_case)
        connections = {}
        for value in args.source_connection:
            code, separator, name = value.partition("=")
            if not separator or not code or not name or code in connections:
                raise ValueError(
                    "Source connections require distinct CODE=NAME bindings"
                )
            connections[code] = name
        runtime_assets = args.metadata_assets or not args.bootstrap_orchestrator
        if args.resume and args.state_file is None:
            raise ValueError("--resume requires --state-file")
        if args.dry_run and args.interactive:
            raise ValueError(
                "--dry-run is noninteractive and cannot request --interactive"
            )
        if (
            (args.bootstrap_orchestrator or runtime_assets)
            and not args.validate_local
            and not args.dry_run
            and args.state_file is None
        ):
            raise ValueError(
                "--bootstrap-orchestrator requires --state-file before any cloud operation"
            )
        if args.validate_local:
            if (
                args.workspace
                or args.overwrite
                or args.dry_run
                or args.interactive
                or args.resume
                or args.state_file
            ):
                raise ValueError(
                    "--validate-local cannot be combined with cloud deployment options"
                )
            if args.bootstrap_orchestrator and not runtime_assets:
                orchestrator_database_definition(
                    root, bootstrap_configuration(root, use_case=profile.case_id)
                )
            else:
                validate_local(root, use_case=profile.case_id)
            LOGGER.info(
                "Local validation passed; no authentication or cloud calls were made"
            )
            return 0
        if not args.workspace:
            raise ValueError(
                "--workspace is required unless --validate-local is selected"
            )
        options = DeploymentOptions(
            workspace=args.workspace,
            bas_sql_connection=args.bas_sql_connection,
            csd_sql_connection=args.csd_sql_connection,
            persal_sql_connection=args.persal_sql_connection,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
            bootstrap_orchestrator=args.bootstrap_orchestrator,
            orchestrator_sql_connection=args.orchestrator_sql_connection,
            interactive=args.interactive,
            state_file=args.state_file,
            resume=args.resume,
            metadata_assets=runtime_assets,
            use_case=profile.case_id,
            source_connections=connections,
        )
        items = AcceleratorDeployer(root, options).deploy()
        if args.dry_run:
            LOGGER.info("Cloud dry-run passed; no items changed")
        elif args.metadata_assets:
            LOGGER.info(
                "Published %d generic assets; no source reseed, activation or jobs. Runtime acceptance remains pending.",
                len(items),
            )
        else:
            LOGGER.info(
                "Deployment passed with %d items; no pipelines started", len(items)
            )
        return 0
    except KeyboardInterrupt:
        LOGGER.error("Interrupted")
        return 130
    except (ValueError, TypeError, FileNotFoundError) as error:
        LOGGER.error("%s", error)
        return 2
    except Exception:
        LOGGER.exception("Case deployment failed")
        return 1
