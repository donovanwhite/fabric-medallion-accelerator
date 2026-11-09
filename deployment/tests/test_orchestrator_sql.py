"""Offline control-plane definition checks; live SQL proof is a separate gate."""

from __future__ import annotations

import base64
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from fabric_medallion_deployer.contracts import load_sources
from fabric_medallion_deployer.definitions import (
    validate_deployable_definition,
)
from fabric_medallion_deployer.metadata import (
    MetadataError,
    load_configuration,
    metadata_digest,
)
from fabric_medallion_deployer.orchestrator import (
    bootstrap_configuration,
    metadata_runtime_sources,
    metadata_worker_sha256,
    orchestrator_database_definition,
)
from test_orchestrator_sql_live import validation_fixtures

ROOT = Path(__file__).resolve().parents[2]


class OrchestratorDefinitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = bootstrap_configuration(ROOT, use_case="treasury")

    def test_seed_contains_all_44_source_contracts_without_claiming_live_support(
        self,
    ) -> None:
        self.assertEqual(len(self.config.feeds), 3)
        sources = load_sources(ROOT, use_case="treasury")
        for source in sources:
            feed = next(f for f in self.config.feeds if f.source_code == source.code)
            self.assertEqual(feed.legacy_contract_hash, source.contract_hash)
            self.assertEqual(feed.silver_schema, source.silver_schema)
            silver = [
                o
                for o in self.config.objects
                if o.feed_id == feed.id and o.layer == "silver"
            ]
            self.assertEqual(len(silver), len(source.tables))
            for table, obj in zip(source.tables, silver, strict=True):
                reconstructed = json.loads(obj.contract_json)
                reconstructed["columns"] = [
                    json.loads(c.definition_json) for c in obj.columns
                ]
                self.assertEqual(reconstructed, table)
        self.assertTrue(all(not c.verified for c in self.config.capabilities))
        self.assertTrue(
            all(c.fabric_connection_id is None for c in self.config.connections)
        )
        self.assertEqual(
            self.config.definition_sha256,
            bootstrap_configuration(ROOT, use_case="treasury").definition_sha256,
        )
        self.assertTrue(self.config.version_id.startswith("treasury-bootstrap-"))
        changed = bootstrap_configuration(
            ROOT, version_id="explicit-new-version", use_case="treasury"
        )
        self.assertEqual(changed.version_id, "explicit-new-version")

    def test_hash_order_is_seeded_independently_of_projection_ordinals(self) -> None:
        policies = self.config.to_document()["extensions"]["hash_policies"]
        for source in load_sources(ROOT, use_case="treasury"):
            for table in source.tables:
                policy = policies[f"{source.code}.silver.{table['name']}"]
                self.assertEqual(policy["key_columns"], table["keys"])
                self.assertEqual(
                    policy["value_columns"],
                    [
                        c["name"]
                        for c in table["columns"]
                        if c["name"] not in table["change_hash_exclusions"]
                    ],
                )

    def test_worker_hashes_match_normalized_notebook_source_text(self) -> None:
        for layer in ("silver", "gold"):
            worker = f"{layer}-worker"
            expected = metadata_digest(
                {
                    name: hashlib.sha256(source.encode("utf-8")).hexdigest()
                    for name, source in metadata_runtime_sources(
                        ROOT, layer=layer, use_case="treasury"
                    ).items()
                }
            )
            self.assertEqual(
                expected, metadata_worker_sha256(ROOT, layer=layer, use_case="treasury")
            )
            self.assertEqual(
                next(w.code_sha256 for w in self.config.workers if w.id == worker),
                expected,
            )

    def test_sql_project_does_not_seed_sources_or_activate_configuration(self) -> None:
        definition = orchestrator_database_definition(ROOT, self.config)
        validate_deployable_definition(definition)
        self.assertEqual(definition["format"], "sqlproj")
        parts = {
            p["path"]: base64.b64decode(p["payload"]).decode("utf-8")
            for p in definition["parts"]
        }
        self.assertIn("db_orchestrator", parts["sqldb.sqlproj"])
        self.assertIn(":r .\\Schema.sql", parts["Deploy.sql"])
        self.assertIn(":r .\\Procedures.sql", parts["Deploy.sql"])
        self.assertIn("cfg.usp_register_version", parts["Seed.sql"])
        self.assertNotIn("usp_activate_version", parts["Seed.sql"])
        self.assertNotIn("WHEN MATCHED THEN UPDATE", parts["Seed.sql"])
        self.assertNotIn("db_bas_finance", parts["Schema.sql"])
        self.assertNotIn("DROP TABLE", parts["Schema.sql"].upper())

    def test_control_project_embeds_the_exact_escaped_draft_without_reseeding(self):
        document = self.config.to_document()
        document["feeds"][0]["description"] = "Treasury's approved source"
        configuration = load_configuration(document)
        with patch(
            "fabric_medallion_deployer.orchestrator.bootstrap_configuration",
            side_effect=AssertionError("An explicit draft must not rebuild seed data"),
        ):
            definition = orchestrator_database_definition(ROOT, configuration)
            self.assertEqual(
                definition, orchestrator_database_definition(ROOT, configuration)
            )
        parts = {
            part["path"]: base64.b64decode(part["payload"]).decode("utf-8")
            for part in definition["parts"]
        }
        self.assertEqual(
            set(parts),
            {"sqldb.sqlproj", "Deploy.sql", "Schema.sql", "Procedures.sql", "Seed.sql"},
        )
        literal = (
            parts["Seed.sql"]
            .split("@definition_json=N'", 1)[1]
            .split("',\n    @definition_sha256=", 1)[0]
        )
        self.assertEqual(literal.replace("''", "'"), configuration.definition_json)
        self.assertIn(configuration.definition_sha256, parts["Seed.sql"])
        self.assertIn("Treasury''s", literal)
        self.assertNotIn("usp_activate_version", parts["Seed.sql"])

    def test_definition_declares_versioned_entities_and_separate_access_roles(
        self,
    ) -> None:
        schema = (ROOT / "artefacts" / "orchestrator" / "schema.sql").read_text(
            encoding="utf-8"
        )
        for table in (
            "configuration_version",
            "activation_history",
            "active_configuration",
            "connection_ref",
            "source_feed",
            "data_object",
            "object_column",
            "object_dependency",
            "dq_rule",
            "constraint_definition",
            "transformation",
            "storage_layout",
            "worker",
            "capability",
            "run",
            "run_task",
            "task_attempt",
            "target_lease",
            "task_result_inbox",
        ):
            self.assertRegex(schema, rf"CREATE TABLE (?:cfg|ctl)\.\[?{table}\]?\s*\(")
        for role in (
            "metadata_reader",
            "metadata_editor",
            "metadata_validator",
            "config_approver",
            "runtime_executor",
        ):
            self.assertIn(role, schema)
        self.assertNotIn("ADD MEMBER", schema)

    def test_procedures_require_hashes_evidence_and_compare_and_swap_activation(
        self,
    ) -> None:
        sql = (ROOT / "artefacts" / "orchestrator" / "procedures.sql").read_text(
            encoding="utf-8"
        )
        for name in (
            "usp_register_version",
            "usp_validate_version",
            "usp_approve_version",
            "usp_activate_version",
            "usp_preview_version",
        ):
            self.assertIn(f"CREATE OR ALTER PROCEDURE cfg.{name}", sql)
        self.assertIn("HASHBYTES('SHA2_256'", sql)
        self.assertIn("@expected_active_version", sql)
        self.assertIn("UPDLOCK, HOLDLOCK", sql)
        self.assertIn("XACT_ABORT ON", sql)
        self.assertIn("THROW", sql)
        self.assertIn("verified = 0", sql)
        self.assertNotIn("DROP TABLE", sql.upper())

    def test_control_procedures_declare_fences_and_idempotent_results(self) -> None:
        sql = (ROOT / "artefacts" / "orchestrator" / "procedures.sql").read_text(
            encoding="utf-8"
        )
        for name in (
            "usp_prepare_run",
            "usp_task_page",
            "usp_run_snapshot",
            "usp_attempt_state",
            "usp_lease_state",
            "usp_acquire_lease",
            "usp_release_lease",
            "usp_begin_attempt",
            "usp_record_submission",
            "usp_accept_result",
        ):
            self.assertIn(f"CREATE OR ALTER PROCEDURE ctl.{name}", sql)
            self.assertIn(f"GRANT EXECUTE ON OBJECT::ctl.{name}", sql)
        self.assertIn("release_evidence_sha256", sql)
        self.assertIn("SUBMISSION_PENDING", sql)
        self.assertIn("UNCERTAIN", sql)
        self.assertIn("NOT_SUBMITTED", sql)
        self.assertIn("reconciled_utc", sql)

    def test_copy_reservation_and_dispatch_keep_authoritative_execution_scope(
        self,
    ) -> None:
        sql = (ROOT / "artefacts" / "orchestrator" / "procedures.sql").read_text(
            encoding="utf-8"
        )
        for name in (
            "usp_prepare_copy_attempt",
            "usp_export_pipeline_dispatch",
            "usp_reconcile_pipeline_result",
            "usp_finish_bronze_dispatch",
        ):
            self.assertIn(f"CREATE OR ALTER PROCEDURE ctl.{name}", sql)
            self.assertIn(f"GRANT EXECUTE ON OBJECT::ctl.{name}", sql)
        self.assertIn("@execution_run_id", sql)
        self.assertIn("queryactivityruns", sql)
        self.assertIn("A prepared Copy cannot be rebound", sql)
        self.assertIn("dispatch_json", sql)
        self.assertIn("undispatched_task_ids", sql)

    def test_file_copy_page_declares_transport_geometry_without_sql_fallback(self):
        sql = (ROOT / "artefacts" / "orchestrator" / "procedures.sql").read_text(
            encoding="utf-8"
        )
        page = sql.split("CREATE OR ALTER PROCEDURE ctl.usp_copy_task_page", 1)[
            1
        ].split("\nGO\n", 1)[0]
        for field in (
            "source_directory",
            "file_pattern",
            "recursive",
            "file_format",
            "arrival_policy_json",
        ):
            self.assertIn(field, page)
        self.assertIn("('sql', 'ftp', 'sftp')", page)
        self.assertIn("CASE WHEN feed.source_type = 'sql'", page)

    def test_approval_and_run_preparation_require_file_capability_evidence(self):
        sql = (ROOT / "artefacts" / "orchestrator" / "procedures.sql").read_text(
            encoding="utf-8"
        )
        for name in ("cfg.usp_approve_version", "ctl.usp_prepare_run"):
            procedure = sql.split(f"CREATE OR ALTER PROCEDURE {name}", 1)[1].split(
                "\nGO\n", 1
            )[0]
            with self.subTest(procedure=name):
                self.assertIn("'-copy-'", procedure)
                self.assertIn("'-arrival-observer'", procedure)
                self.assertIn("authentication_type", procedure)
                self.assertIn("$.arrival_policy", procedure)
                self.assertIn("evidence_sha256 IS NOT NULL", procedure)
                self.assertIn("LEN(cap.evidence_sha256) = 64", procedure)
                self.assertIn("NOT LIKE '%[^0-9a-f]%'", procedure)
                for field, json_type in (
                    ("source_directory", 1),
                    ("file_pattern", 1),
                    ("recursive", 3),
                    ("file_format", 1),
                    ("arrival_policy", 5),
                    ("kind", 1),
                    ("marker", 1),
                    ("minimum_age_seconds", 2),
                    ("stable_for_seconds", 2),
                ):
                    self.assertIn(
                        f"[key] = '{field}' AND [type] = {json_type}", procedure
                    )
                if name == "ctl.usp_prepare_run":
                    self.assertLess(
                        procedure.index("Selected file sources require complete"),
                        procedure.index("IF @existing_snapshot IS NOT NULL"),
                    )

    def test_file_sql_approval_fixtures_cover_typed_contracts(self):
        cases = {
            case["case_name"]: case
            for case in validation_fixtures("e" * 64)
            if case["case_name"].startswith("file_")
        }
        valid = ("file_valid_ftp_marker", "file_valid_sftp_stable")
        invalid = (
            "file_source_directory_number_rejected",
            "file_recursive_string_rejected",
            "file_marker_number_rejected",
            "file_stable_age_string_rejected",
            "file_stable_for_boolean_rejected",
            "file_format_case_rejected",
            "file_invalid_copy_evidence_rejected",
            "file_invalid_observer_evidence_rejected",
        )
        self.assertEqual(
            set(cases), {*valid, *invalid, "file_missing_observer_rejected"}
        )
        for name in valid:
            with self.subTest(case=name):
                self.assertIsNone(cases[name]["expected_error"])
                load_configuration(json.loads(cases[name]["definition_json"]))
        for name in invalid:
            with self.subTest(case=name):
                self.assertEqual(cases[name]["expected_error"], 51023)
                with self.assertRaises(MetadataError):
                    load_configuration(json.loads(cases[name]["definition_json"]))
        self.assertEqual(
            cases["file_missing_observer_rejected"]["expected_error"], 51023
        )


if __name__ == "__main__":
    unittest.main()
