"""Explicitly opt-in, rollback-only checks against the approved control database."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path

from fabric_medallion_deployer.metadata import (
    canonical_json,
    compile_run_plan,
    load_configuration,
)
from metadata_fixtures import metadata_document


def validation_fixtures(evidence_sha256: str) -> list[dict]:
    if len(evidence_sha256) != 64:
        raise ValueError("An explicit capability evidence hash is required")
    cases = (
        ("idempotent_registration", None),
        ("changed_definition_rejected", 51006),
        ("wrong_hash_rejected", 51003),
        ("credentials_rejected", 51005),
        ("failed_validation_rejected", 51010),
        ("string_true_validation_rejected", 51010),
        ("unbound_connection_rejected", 51021),
        ("unverified_capability_rejected", 51022),
        ("unapproved_activation_rejected", 51031),
        ("stale_activation_rejected", 51030),
        ("activation_rollback", None),
        ("file_valid_ftp_marker", None),
        ("file_valid_sftp_stable", None),
        ("file_source_directory_number_rejected", 51023),
        ("file_recursive_string_rejected", 51023),
        ("file_marker_number_rejected", 51023),
        ("file_stable_age_string_rejected", 51023),
        ("file_stable_for_boolean_rejected", 51023),
        ("file_format_case_rejected", 51023),
        ("file_invalid_copy_evidence_rejected", 51023),
        ("file_invalid_observer_evidence_rejected", 51023),
        ("file_missing_observer_rejected", 51023),
    )
    result = []
    for name, expected in cases:
        document = metadata_document()
        document["version_id"] = f"validation-{uuid.uuid4()}"
        document["capabilities"][0].update(
            verified=True, evidence_sha256=evidence_sha256
        )
        if name == "unbound_connection_rejected":
            document["connections"][0]["fabric_connection_id"] = None
        if name == "unverified_capability_rejected":
            document["capabilities"][0].update(verified=False, evidence_sha256=None)
        if name.startswith("file_"):
            source_type = "sftp" if name == "file_valid_sftp_stable" else "ftp"
            document["connections"][0].update(
                connector_type=source_type.capitalize(), authentication_type="Basic"
            )
            document["feeds"][0].update(source_type=source_type, geometry=None)
            contract = {
                "source_directory": "/exports",
                "file_pattern": "postings_*.csv",
                "recursive": False,
                "file_format": "csv",
                "arrival_policy": {"kind": "marker", "marker": "_SUCCESS"},
            }
            if name in {
                "file_valid_sftp_stable",
                "file_stable_age_string_rejected",
                "file_stable_for_boolean_rejected",
            }:
                contract["arrival_policy"] = {
                    "kind": "stable",
                    "minimum_age_seconds": 60,
                    "stable_for_seconds": 60,
                }
            if name == "file_source_directory_number_rejected":
                contract["source_directory"] = 123
            elif name == "file_recursive_string_rejected":
                contract["recursive"] = "false"
            elif name == "file_marker_number_rejected":
                contract["arrival_policy"]["marker"] = 123
            elif name == "file_stable_age_string_rejected":
                contract["arrival_policy"]["minimum_age_seconds"] = "60"
            elif name == "file_stable_for_boolean_rejected":
                contract["arrival_policy"]["stable_for_seconds"] = True
            elif name == "file_format_case_rejected":
                contract["file_format"] = "CSV"
            document["objects"][0].update(required_capabilities=[], contract=contract)
            capabilities = [f"{source_type}-copy-basic"]
            if name != "file_missing_observer_rejected":
                capabilities.append(f"{source_type}-arrival-observer")
            document["capabilities"].extend(
                {
                    "id": capability,
                    "verified": True,
                    "evidence_sha256": (
                        "z" * 64
                        if (
                            name == "file_invalid_copy_evidence_rejected"
                            and capability.endswith("-copy-basic")
                        )
                        or (
                            name == "file_invalid_observer_evidence_rejected"
                            and capability.endswith("-arrival-observer")
                        )
                        else evidence_sha256
                    ),
                }
                for capability in capabilities
            )
        if name == "credentials_rejected":
            document["extensions"]["password"] = "synthetic_rejection_fixture"
        elif not (name.startswith("file_") and name.endswith("_rejected")):
            load_configuration(document)
        # Negative fixtures bypass client validation to exercise the server guard.
        serialized = json.dumps(
            document, sort_keys=True, ensure_ascii=True, separators=(",", ":")
        )
        fingerprint = hashlib.sha256(serialized.encode("ascii")).hexdigest()
        changed = copy.deepcopy(document)
        changed["owner"] = "different_owner"
        changed_json = json.dumps(
            changed, sort_keys=True, ensure_ascii=True, separators=(",", ":")
        )
        report: dict = {"passed": True, "scope": "rollback_only_metadata_fixture"}
        if name == "failed_validation_rejected":
            report["passed"] = False
        elif name == "string_true_validation_rejected":
            report["passed"] = "true"
        result.append(
            {
                "case_name": name,
                "version_id": document["version_id"],
                "definition_json": serialized,
                "definition_sha256": "0" * 64
                if name == "wrong_hash_rejected"
                else fingerprint,
                "changed_json": changed_json,
                "changed_sha256": hashlib.sha256(
                    changed_json.encode("ascii")
                ).hexdigest(),
                "validator_hash": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "validation_json": canonical_json(report),
                "expected_active": "stale-version"
                if name == "stale_activation_rejected"
                else None,
                "expected_error": expected,
            }
        )
    return result


def run_validation_fixtures(evidence_sha256: str) -> list[dict]:
    cases = (
        ("run_prepare_idempotent", None),
        ("run_prepare_changed", 51104),
        ("run_unapproved", 51101),
        ("run_wrong_plan_hash", 51102),
        ("run_disabled_dependency", 51103),
        ("run_bounded_pages", None),
        ("run_same_target_writer", 51111),
        ("run_expired_lease", 51111),
        ("run_stale_fence", 51121),
        ("run_missing_source_lease", 51121),
        ("run_dependency_not_ready", 51122),
        ("run_duplicate_attempt", None),
        ("run_uncertain_blocks_retry", 51123),
        ("run_uncertain_blocks_release", 51112),
        ("run_definite_rejection_retry", None),
        ("run_duplicate_result", None),
        ("run_changed_result", 51132),
        ("run_failed_blocks_downstream", 51122),
        ("run_released_lease_next_fence", None),
        ("run_job_conflict", 51125),
        ("run_success_requires_evidence", 51130),
        ("run_result_wrong_target", 51130),
        ("run_result_wrong_batch", 51130),
        ("run_result_bad_metrics", 51130),
        ("run_missing_publication_evidence", 51130),
        ("run_uncertain_job_resolved", None),
        ("run_invalid_page", 51105),
    )
    result = []
    for name, expected in cases:
        document = metadata_document()
        document["version_id"] = f"validation-{uuid.uuid4()}"
        document["capabilities"][0].update(
            verified=True, evidence_sha256=evidence_sha256
        )
        config = load_configuration(document)
        run_id = f"validation-{uuid.uuid4()}"
        observed = "2026-11-06T08:30:00.123000Z"
        plan = compile_run_plan(
            config,
            run_id,
            observed,
            lifecycle_state="APPROVED",
            approved_definition_sha256=config.definition_sha256,
            require_ready=True,
        )
        task, downstream = plan.to_document()["tasks"]
        second_run = f"validation-{uuid.uuid4()}"
        attempt = str(uuid.uuid4())
        job = str(uuid.uuid4())
        body = {
            "format_version": 1,
            "run_id": run_id,
            "task_id": task["task_id"],
            "source_batch_id": task["source_batch_id"],
            "attempt_id": attempt,
            "fence": 1,
            "job_id": job,
            "status": "SUCCEEDED",
            "finished_utc": "2026-11-06T09:00:00Z",
            "artifact_refs": [
                {
                    "kind": "raw_batch",
                    "location": task["target_path"],
                    "version": None,
                    "sha256": evidence_sha256,
                }
            ],
            "readiness": ["succeeded"],
            "error_code": None,
            "metrics": {"rows_read": 1},
        }
        if name == "run_failed_blocks_downstream":
            body.update(
                status="FAILED",
                artifact_refs=[],
                readiness=[],
                error_code="SYNTHETIC_DQ_FAILURE",
            )
        if name == "run_success_requires_evidence":
            body["artifact_refs"] = []
        if name == "run_result_wrong_target":
            body["artifact_refs"][0]["location"] = "Files/bronze/other/fixture.csv"
        if name == "run_result_wrong_batch":
            body["source_batch_id"] = "f" * 64
        if name == "run_result_bad_metrics":
            body["metrics"]["rows_read"] = True
        if name == "run_missing_publication_evidence":
            body["readiness"].append("source_committed")
        if name == "run_disabled_dependency":
            document["objects"][0]["enabled"] = False
            config = load_configuration(document)
        serialized = canonical_json(body)
        changed = copy.deepcopy(body)
        changed["metrics"]["rows_read"] = 2
        changed_json = canonical_json(changed)
        result.append(
            {
                "case_name": name,
                "version_id": config.version_id,
                "definition_json": config.definition_json,
                "definition_sha256": config.definition_sha256,
                "validator_hash": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "validation_json": canonical_json(
                    {
                        "passed": True,
                        "scope": "rollback_only_metadata_fixture",
                    }
                ),
                "expected_error": expected,
                "run_id": run_id,
                "second_run_id": second_run,
                "observed_utc": observed,
                "expected_snapshot_sha256": "0" * 64
                if name == "run_wrong_plan_hash"
                else None
                if name == "run_disabled_dependency"
                else plan.snapshot_sha256,
                "expected_snapshot_json": plan.snapshot_json,
                "task_id": task["task_id"],
                "downstream_task_id": downstream["task_id"],
                "target_key": task["target_lock_key"],
                "downstream_target_key": downstream["target_lock_key"],
                "source_key": task["source_lock_key"],
                "attempt_id": attempt,
                "second_attempt_id": str(uuid.uuid4()),
                "job_id": job,
                "evidence_sha256": evidence_sha256,
                "result_json": serialized,
                "result_sha256": hashlib.sha256(serialized.encode("ascii")).hexdigest(),
                "changed_result_json": changed_json,
                "changed_result_sha256": hashlib.sha256(
                    changed_json.encode("ascii")
                ).hexdigest(),
            }
        )
    return result


@unittest.skipUnless(
    os.environ.get("TREASURY_ORCHESTRATOR_SQL_TESTS") == "1",
    "A2 opt-in: explicit control SQL context and approval required",
)
class OrchestratorLiveSqlTests(unittest.TestCase):
    def test_control_database_procedures_and_rollback(self) -> None:
        context = Path(os.environ["TREASURY_ORCHESTRATOR_SQL_CONTEXT"]).resolve()
        evidence = os.environ["TREASURY_CAPABILITY_EVIDENCE_SHA256"]
        shell = shutil.which("pwsh")
        self.assertIsNotNone(
            shell, "PowerShell 7 is required for the existing .NET TDS adapter"
        )
        output = Path(os.environ["TREASURY_ORCHESTRATOR_SQL_EVIDENCE"]).resolve()
        self.assertFalse(
            output.exists(), "Use a fresh evidence path, not an overwritten result"
        )
        with tempfile.TemporaryDirectory() as folder:
            fixtures = Path(folder) / "metadata-fixtures.json"
            cases = validation_fixtures(evidence) + run_validation_fixtures(evidence)
            fixtures.write_text(json.dumps(cases), encoding="utf-8")
            result = subprocess.run(
                [
                    shell,
                    "-NoLogo",
                    "-NoProfile",
                    "-File",
                    str(
                        Path(__file__).with_name("Invoke-OrchestratorSqlValidation.ps1")
                    ),
                    "-ContextPath",
                    str(context),
                    "-FixturePath",
                    str(fixtures),
                    "-OutputPath",
                    str(output),
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=600,
            )
        self.assertEqual(result.returncode, 0, result.stderr[-4000:])
        report = json.loads(output.read_text(encoding="utf-8-sig"))
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["passed"], len(cases))
        self.assertEqual(report["active_configurations"], 0)
        self.assertEqual(report["fixture_versions_retained"], 0)
        self.assertEqual(report["operational_fixture_rows_retained"], 0)


if __name__ == "__main__":
    unittest.main()
