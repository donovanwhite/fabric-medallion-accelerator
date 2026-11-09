"""Explicit control-bootstrap flags and no-authentication local validation."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from fabric_medallion_deployer import cli

ROOT = Path(__file__).resolve().parents[2]


class CliTests(unittest.TestCase):
    def test_missing_or_unknown_case_fails_before_cloud_or_local_validation(self):
        for selection in ([], ["--use-case", "unknown"]):
            with (
                self.subTest(selection=selection),
                patch("sys.argv", ["deploy", "--validate-local", *selection]),
                patch.object(cli, "repository_root", return_value=ROOT),
                patch.object(cli, "AcceleratorDeployer") as deployer,
                patch.object(cli, "validate_local") as validate,
                self.assertLogs(cli.LOGGER, level="ERROR") as logged,
            ):
                self.assertEqual(cli.main(), 2)
            self.assertIn(
                "Unknown use case" if selection else "--use-case",
                "\n".join(logged.output),
            )
            deployer.assert_not_called()
            validate.assert_not_called()

    def test_registered_case_selection_is_forwarded_without_fallback(self):
        for case in ("inventory_example", "treasury"):
            with (
                self.subTest(case=case),
                patch("sys.argv", ["deploy", "--validate-local", "--use-case", case]),
                patch.object(cli, "repository_root", return_value=ROOT),
                patch.object(cli, "validate_local") as validate,
                patch.object(cli, "AcceleratorDeployer") as deployer,
            ):
                self.assertEqual(cli.main(), 0)
            validate.assert_called_once_with(ROOT, use_case=case)
            deployer.assert_not_called()

    def test_control_connection_and_journal_flags_are_explicit(self):
        args = cli.create_parser().parse_args(
            [
                "--workspace",
                "ws_kzn_treasury",
                "--bootstrap-orchestrator",
                "--state-file",
                "control-state.json",
                "--resume",
            ]
        )
        self.assertEqual(args.orchestrator_sql_connection, "conn_orchestrator_sql")
        self.assertTrue(args.bootstrap_orchestrator)
        self.assertTrue(args.resume)
        self.assertEqual(args.state_file, Path("control-state.json"))

    def test_control_local_validation_never_constructs_a_cloud_deployer(self):
        with (
            patch(
                "sys.argv",
                [
                    "deploy",
                    "--use-case",
                    "inventory_example",
                    "--validate-local",
                    "--bootstrap-orchestrator",
                ],
            ),
            patch.object(cli, "repository_root", return_value=ROOT),
            patch.object(cli, "AcceleratorDeployer") as deployer,
        ):
            self.assertEqual(cli.main(), 0)
        deployer.assert_not_called()

    def test_invalid_remote_modes_fail_before_deployer_creation(self):
        for arguments in (
            ["--validate-local", "--interactive"],
            ["--workspace", "test", "--dry-run", "--interactive"],
            ["--workspace", "test", "--resume"],
            ["--workspace", "test", "--bootstrap-orchestrator", "--resume"],
            ["--workspace", "test", "--metadata-assets"],
            ["--workspace", "test", "--metadata-assets", "--bootstrap-orchestrator"],
            [
                "--validate-local",
                "--metadata-assets",
                "--bootstrap-orchestrator",
                "--state-file",
                "state.json",
            ],
            [
                "--workspace",
                "test",
                "--metadata-assets",
                "--bootstrap-orchestrator",
                "--dry-run",
                "--interactive",
            ],
        ):
            with (
                self.subTest(arguments=arguments),
                patch(
                    "sys.argv",
                    ["deploy", "--use-case", "inventory_example", *arguments],
                ),
                patch.object(cli, "repository_root", return_value=ROOT),
                patch.object(cli, "AcceleratorDeployer") as deployer,
            ):
                self.assertEqual(cli.main(), 2)
            deployer.assert_not_called()

    def test_metadata_mode_is_the_default_and_still_accepts_old_flag(self):
        args = cli.create_parser().parse_args(
            [
                "--metadata-assets",
                "--bootstrap-orchestrator",
                "--state-file",
                "metadata.json",
                "--resume",
            ]
        )
        self.assertTrue(args.metadata_assets)
        self.assertTrue(args.bootstrap_orchestrator)
        self.assertEqual(args.state_file, Path("metadata.json"))
        self.assertTrue(args.resume)
        with (
            patch(
                "sys.argv",
                [
                    "deploy",
                    "--use-case",
                    "inventory_example",
                    "--workspace",
                    "test",
                    "--dry-run",
                ],
            ),
            patch.object(cli, "repository_root", return_value=ROOT),
            patch.object(cli, "AcceleratorDeployer") as deployer,
        ):
            deployer.return_value.deploy.return_value = {}
            self.assertEqual(cli.main(), 0)
        self.assertTrue(deployer.call_args.args[1].metadata_assets)

    def test_metadata_local_validation_is_offline_and_does_not_validate_source_seeds(
        self,
    ):
        with (
            patch(
                "sys.argv",
                [
                    "deploy",
                    "--use-case",
                    "inventory_example",
                    "--validate-local",
                    "--bootstrap-orchestrator",
                    "--metadata-assets",
                ],
            ),
            patch.object(cli, "repository_root", return_value=ROOT),
            patch.object(cli, "AcceleratorDeployer") as deployer,
            patch.object(cli, "validate_local") as legacy,
            patch("fabric_medallion_deployer.deployer.validate_seed_data") as seed,
        ):
            self.assertEqual(cli.main(), 0)
        deployer.assert_not_called()
        legacy.assert_called_once_with(ROOT, use_case="inventory_example")
        seed.assert_not_called()

    def test_metadata_state_resume_and_connections_are_forwarded(self):
        with (
            patch(
                "sys.argv",
                [
                    "deploy",
                    "--use-case",
                    "inventory_example",
                    "--workspace",
                    "test",
                    "--bootstrap-orchestrator",
                    "--metadata-assets",
                    "--state-file",
                    "metadata.json",
                    "--resume",
                    "--overwrite",
                    "--bas-sql-connection",
                    "bas-connection",
                    "--orchestrator-sql-connection",
                    "control-connection",
                ],
            ),
            patch.object(cli, "repository_root", return_value=ROOT),
            patch.object(cli, "AcceleratorDeployer") as deployer,
        ):
            deployer.return_value.deploy.return_value = {}
            self.assertEqual(cli.main(), 0)
        options = deployer.call_args.args[1]
        self.assertTrue(options.metadata_assets)
        self.assertTrue(options.bootstrap_orchestrator)
        self.assertTrue(options.resume)
        self.assertTrue(options.overwrite)
        self.assertEqual(options.state_file, Path("metadata.json"))
        self.assertEqual(options.bas_sql_connection, "bas-connection")
        self.assertEqual(options.orchestrator_sql_connection, "control-connection")

    def test_metadata_dry_run_needs_no_journal_and_remains_noninteractive(self):
        with (
            patch(
                "sys.argv",
                [
                    "deploy",
                    "--use-case",
                    "inventory_example",
                    "--workspace",
                    "test",
                    "--bootstrap-orchestrator",
                    "--metadata-assets",
                    "--dry-run",
                ],
            ),
            patch.object(cli, "repository_root", return_value=ROOT),
            patch.object(cli, "AcceleratorDeployer") as deployer,
        ):
            deployer.return_value.deploy.return_value = {}
            self.assertEqual(cli.main(), 0)
        options = deployer.call_args.args[1]
        self.assertTrue(options.metadata_assets)
        self.assertTrue(options.dry_run)
        self.assertFalse(options.interactive)
        self.assertIsNone(options.state_file)


class EntrypointCaseSelectionTests(unittest.TestCase):
    def test_operational_scripts_require_selection_and_reject_unknown_cases(self):
        commands = (
            ("specifications.py", ["validate"]),
            ("build.py", ["--check"]),
            ("deploy.py", ["--validate-local"]),
            (
                "configure.py",
                [
                    "preview",
                    "--configuration",
                    "unused.json",
                    "--run-id",
                    "test",
                    "--observed-utc",
                    "2026-11-09T08:00:00Z",
                ],
            ),
            ("generate_fixtures.py", ["--validate-only"]),
            ("validate_fixtures.py", []),
        )
        for script, arguments in commands:
            for selection in ([], ["--use-case", "unknown"]):
                with self.subTest(script=script, selection=selection):
                    result = subprocess.run(
                        [
                            sys.executable,
                            "-B",
                            str(ROOT / "deployment" / script),
                            *arguments,
                            *selection,
                        ],
                        cwd=ROOT,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertIn(
                        "Unknown use case" if selection else "--use-case",
                        result.stderr,
                    )


if __name__ == "__main__":
    unittest.main()
