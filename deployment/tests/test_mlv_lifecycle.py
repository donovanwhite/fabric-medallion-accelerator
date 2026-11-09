# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Local-only MLV lifecycle/failure evidence; never contact Fabric."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from fabric_medallion_deployer.gold import mlv_definitions
from fabric_medallion_deployer.metadata import metadata_digest
from fabric_medallion_deployer.orchestrator import bootstrap_configuration
from fabric_medallion_deployer.use_cases import load_case_extension, load_use_case

ROOT = Path(__file__).resolve().parents[2]
FACTS = load_case_extension(load_use_case(ROOT, "treasury"), "gold_profile").FACTS
SPEC = importlib.util.spec_from_file_location(
    "treasury_mlv_tests", ROOT / "artefacts/orchestrator/mlv.py"
)
mlv = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mlv
SPEC.loader.exec_module(mlv)

FACT = "gold.gld.fact_gl_posting"
TARGET = "gold.gld.mlv_finance"
PARENT = "gold.gld.dim_department"
QUERY = (
    "SELECT CAST('SYNTHETIC_PROVISIONAL' AS STRING) AS data_scope, period, "
    "SUM(amount) AS amount, COUNT(*) AS source_row_count "
    f"FROM {FACT} GROUP BY period"
)
COLUMNS = (
    ("data_scope", "string"),
    ("period", "string"),
    ("amount", "decimal(38,4)"),
    ("source_row_count", "bigint"),
)
DEFINITION = mlv.MlvDefinition(
    "finance", TARGET, "1", QUERY, (FACT,), COLUMNS, ("period",)
)
EXPECTED = [
    ("SYNTHETIC_PROVISIONAL", "202601", Decimal("12.0000"), 2),
    ("SYNTHETIC_PROVISIONAL", "202602", None, 1),
]


def reference(table: str, version: int = 1) -> dict:
    return {"table": table, "location": "/local/" + table, "version": version}


def constraint(kind: str, definition: dict, *, enforcement: str = "runtime") -> dict:
    return {
        "id": kind + "_rule",
        "object_id": "finance",
        "kind": kind,
        "enforcement": enforcement,
        "definition": definition,
    }


def row_rule() -> dict:
    return constraint(
        "check",
        {
            "id": "non_null",
            "handler": "required_columns",
            "version": "1",
            "severity": "error",
            "action": "fail",
            "parameters": {"columns": ["period"]},
        },
    )


class OfflineRuntime:
    """Test double with actual multiset reconciliation, not canned readiness."""

    def __init__(self) -> None:
        self.definitions, self.executions = {}, []
        self.references = {name: reference(name) for name in (FACT, TARGET, PARENT)}
        self.expected, self.actual = deepcopy(EXPECTED), deepcopy(EXPECTED[::-1])
        self.enabled = True
        self.fail_execute = None
        self.on_reconcile = lambda: None
        self.violations = 0
        self.omit_constraint = False
        self.settings = {"format": "delta", "cdf_enabled": True}

    def schema_enabled(self):
        return self.enabled

    def definition(self, target):
        return self.definitions.get(target)

    def validate(self, definition):
        definition.parsed()

    def execute(self, ddl):
        self.executions.append(ddl)
        self.definitions[TARGET] = ddl
        if self.fail_execute:
            raise self.fail_execute

    def reference(self, table):
        return deepcopy(self.references[table])

    def source_settings(self, table):
        return dict(self.settings)

    def reconcile(self, definition, inputs, output):
        result = mlv.exact_rows(self.expected, self.actual)
        result["constraints"] = [
            {
                "id": "declared_grain",
                "kind": "unique",
                "enforcement": "runtime",
                "violations": self.violations,
            },
            *[
                {
                    "id": c["id"],
                    "kind": c["kind"],
                    "enforcement": c["enforcement"],
                    "violations": self.violations,
                }
                for c in definition.constraints
            ],
        ]
        if self.omit_constraint:
            result["constraints"] = result["constraints"][:1]
        self.on_reconcile()
        return result


class OfflineJobs:
    def __init__(self) -> None:
        self.posts, self.gets, self.history_reads = 0, 0, 0
        self.schedule_rows, self.history_rows = [], []
        self.statuses = ["Completed"]
        self.scope_data = {"id": "selected-lineage", "mode": "Optimal"}
        self.on_submit = lambda: None
        self.on_get = lambda: None
        self.delay = 2
        self.submit_error = None

    def scope(self, definitions):
        return deepcopy(self.scope_data)

    def schedules(self):
        return deepcopy(self.schedule_rows)

    def history(self):
        self.history_reads += 1
        return deepcopy(self.history_rows)

    def submit(self):
        self.posts += 1
        self.on_submit()
        if self.submit_error:
            raise self.submit_error
        return {
            "job_id": "job-1",
            "location": "retained-job-location",
            "retry_after": self.delay,
        }

    def get(self, job_id):
        self.gets += 1
        self.on_get()
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return {
            "id": job_id,
            "status": status,
            "failureReason": (
                {
                    "errorCode": "OFFLINE_FAILURE",
                    "message": "untrusted source error detail",
                }
                if status == "Failed"
                else None
            ),
        }


class LifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = mlv.LocalStateStore(self.directory.name)
        self.runtime, self.jobs = OfflineRuntime(), OfflineJobs()
        self.now, self.sleeps = 1794214800.0, []
        self.owned, self.ready = True, True
        self.inputs = {FACT: reference(FACT)}
        self.release = {
            "gold_release_id": "release-1",
            "status": "FACTS_READY",
            "execution_mode": "synthetic_demo",
            "contract_hash": "a" * 64,
            "input_manifest_json": json.dumps(
                {"_selection": {"runs": "sealed-sources"}}
            ),
            "output_manifest_json": json.dumps(
                {"fact": reference(FACT), "parent": reference(PARENT)}
            ),
        }
        self.lifecycle = self.new_lifecycle()

    def new_lifecycle(self):
        return mlv.MlvLifecycle(
            self.store,
            self.runtime,
            self.jobs,
            ownership_check=self.check_owner,
            release=self.release,
            release_check=self.check_release,
            clock=lambda: self.now,
            sleep=self.sleep,
        )

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def check_owner(self):
        if not self.owned:
            raise PermissionError("Lost target-set fence")

    def check_release(self):
        if not self.ready:
            raise mlv.MlvError("Selected Gold release is no longer ready")

    def register(self, definition=DEFINITION, approved=None):
        return self.lifecycle.register(
            definition,
            self.inputs,
            approved_sha256=definition.sha256 if approved is None else approved,
            approval_id="approval-1",
        )

    def observe(self, job, target):
        return mlv.RefreshObservation(job, target, "Full", "local-test-mode-evidence")

    def refresh(self, **kwargs):
        return self.lifecycle.refresh(
            "attempt-1",
            (DEFINITION,),
            self.inputs,
            observe_mode=kwargs.pop("observe_mode", self.observe),
            **kwargs,
        )

    def state(self):
        return self.store.read("refresh:attempt-1")

    def test_unchanged_definition_does_not_execute_ddl_again(self):
        self.register()
        self.register()
        self.assertEqual(len(self.runtime.executions), 1)
        self.assertIn("IF NOT EXISTS", self.runtime.executions[0])

    def test_changed_version_hash_requires_approval_and_reauthors_only_once(self):
        self.register()
        changed = replace(DEFINITION, version="2")
        self.assertNotEqual(changed.sha256, DEFINITION.sha256)
        with self.assertRaisesRegex(mlv.MlvError, "approval"):
            self.register(changed, approved=DEFINITION.sha256)
        self.register(changed)
        self.register(changed)
        self.assertEqual(len(self.runtime.executions), 2)
        self.assertTrue(self.runtime.executions[1].startswith("CREATE OR REPLACE"))

    def test_registration_intent_persisted_before_ddl(self):
        original = self.runtime.execute

        def execute(ddl):
            self.assertEqual(
                self.store.read("definition:" + TARGET)["status"], "AUTHORING_UNCERTAIN"
            )
            original(ddl)

        self.runtime.execute = execute
        self.register()

    def test_uncertain_ddl_response_is_read_back_never_reexecuted(self):
        self.runtime.fail_execute = OSError("Lost Spark response")
        with self.assertRaises(OSError):
            self.register()
        self.runtime.fail_execute = None
        self.register()
        self.assertEqual(len(self.runtime.executions), 1)

    def test_registration_requires_schema_ownership_release_and_current_inputs(self):
        for fault in ("schema", "owner", "release", "version", "location"):
            with self.subTest(fault=fault):
                self.runtime.enabled, self.owned, self.ready = True, True, True
                self.runtime.references[FACT] = reference(FACT)
                if fault == "schema":
                    self.runtime.enabled = False
                elif fault == "owner":
                    self.owned = False
                elif fault == "release":
                    self.ready = False
                else:
                    self.runtime.references[FACT][fault] = (
                        2 if fault == "version" else "/changed"
                    )
                with self.assertRaises((mlv.MlvError, PermissionError)):
                    self.register()
        self.assertEqual(self.runtime.executions, [])

    def test_unrecorded_existing_definition_cannot_be_adopted(self):
        self.runtime.definitions[TARGET] = DEFINITION.ddl(replace=False)
        with self.assertRaisesRegex(mlv.MlvError, "unowned"):
            self.register()
        self.assertEqual(self.runtime.executions, [])

    def test_registered_definition_drift_is_blocked_without_reauthoring(self):
        self.register()
        self.runtime.definitions[TARGET] = QUERY.replace("SUM(amount)", "SUM(other)")
        with self.assertRaises(mlv.MlvError):
            self.register()
        self.assertEqual(len(self.runtime.executions), 1)

    def test_full_refresh_exact_order_independent_readiness_is_technical_only(self):
        self.register()
        result = self.refresh()
        self.assertEqual(result["status"], "MLVS_VERIFIED")
        self.assertFalse(result["business_publication"])
        self.assertFalse(result["native_gates_passed"])
        self.assertEqual(result["outputs"][TARGET]["mode"]["mode"], "Full")
        self.assertIn("unmeasured", result["outputs"][TARGET]["capacity_impact"])
        self.assertEqual(self.jobs.posts, 1)
        self.assertEqual(self.sleeps, [2])

    def test_intent_and_job_identity_are_persisted_before_submission_and_monitor(self):
        self.register()
        self.jobs.on_submit = lambda: self.assertEqual(
            self.state()["status"], "SUBMISSION_UNCERTAIN"
        )
        self.jobs.on_get = lambda: self.assertEqual(self.state()["job_id"], "job-1")
        self.refresh()

    def test_uncertain_submit_queries_history_without_duplicate_post(self):
        self.register()
        self.jobs.submit_error = OSError("Lost POST response")
        with self.assertRaises(OSError):
            self.refresh()
        self.assertEqual(self.state()["status"], "SUBMISSION_UNCERTAIN")
        self.jobs.submit_error = None
        self.jobs.history_rows = [
            {
                "id": "job-1",
                "invokeType": "Manual",
                "status": "Completed",
                "startTimeUtc": datetime.fromtimestamp(self.now + 1, UTC).isoformat(),
            }
        ]
        self.lifecycle = self.new_lifecycle()  # A fresh interpreter's persisted state.
        with self.assertRaises(mlv.SubmissionUncertain):
            self.refresh()
        self.assertEqual(self.state()["recovery_candidates"], ["job-1"])
        self.assertEqual(self.jobs.posts, 1)
        self.assertGreater(self.jobs.history_reads, 1)
        result = self.refresh(
            recovered_job_id="job-1", recovery_evidence_ref="retained-attribution"
        )
        self.assertEqual(result["status"], "MLVS_VERIFIED")
        self.assertEqual(self.jobs.posts, 1)

    def test_uncertain_recovery_requires_attribution_even_for_singleton(self):
        self.register()
        self.jobs.submit_error = OSError()
        with self.assertRaises(OSError):
            self.refresh()
        self.jobs.history_rows = [
            {
                "id": "job-1",
                "invokeType": "Manual",
                "status": "Completed",
                "startTimeUtc": datetime.fromtimestamp(self.now + 1, UTC).isoformat(),
            }
        ]
        with self.assertRaises(ValueError):
            self.refresh(recovered_job_id="job-1")
        self.assertEqual(self.jobs.posts, 1)

    def test_resumed_monitor_after_get_failure_never_resubmits(self):
        self.register()
        self.jobs.on_get = Mock(side_effect=OSError("GET unavailable"))
        with self.assertRaises(OSError):
            self.refresh()
        self.assertEqual(self.state()["job_id"], "job-1")
        self.lifecycle = self.new_lifecycle()
        self.jobs.on_get = lambda: None
        self.refresh()
        self.assertEqual(self.jobs.posts, 1)

    def test_terminal_failure_propagates_after_persisting_evidence(self):
        self.register()
        self.jobs.statuses = ["Failed"]
        with self.assertRaises(mlv.MlvError):
            self.refresh()
        self.assertEqual(self.state()["status"], "REFRESH_FAILED")
        self.assertEqual(
            self.state()["last_job"]["failureReason"]["errorCode"], "OFFLINE_FAILURE"
        )
        self.assertNotIn("untrusted source error detail", json.dumps(self.state()))
        with self.assertRaises(mlv.MlvError):
            self.refresh()
        self.assertEqual(self.jobs.posts, 1)

    def test_cancelled_deduped_unknown_are_not_success(self):
        self.register()
        for status in ("Cancelled", "Deduped", "InventedSuccess"):
            self.jobs.statuses = [status]
            with self.subTest(status=status), self.assertRaises(mlv.MlvError):
                self.lifecycle.refresh(
                    status, (DEFINITION,), self.inputs, observe_mode=self.observe
                )

    def test_schedule_conflict_prevents_post(self):
        self.register()
        self.jobs.schedule_rows = [{"enabled": True, "id": "nightly"}]
        with self.assertRaisesRegex(mlv.MlvError, "Scheduled"):
            self.refresh()
        self.assertEqual(self.jobs.posts, 0)
        self.assertEqual(self.state()["status"], "BLOCKED")

    def test_disabled_schedule_does_not_conflict(self):
        self.register()
        self.jobs.schedule_rows = [{"enabled": False}]
        self.assertEqual(self.refresh()["status"], "MLVS_VERIFIED")

    def test_new_schedule_during_monitor_blocks_readiness(self):
        self.register()
        self.jobs.statuses = ["InProgress", "Completed"]
        self.jobs.on_get = lambda: self.jobs.schedule_rows.append({"enabled": True})
        with self.assertRaisesRegex(mlv.MlvError, "Scheduled"):
            self.refresh()
        self.assertEqual(self.jobs.posts, 1)

    def test_existing_active_job_blocks_submission(self):
        self.register()
        self.jobs.history_rows = [{"id": "other", "status": "InProgress"}]
        with self.assertRaisesRegex(mlv.MlvError, "conflicts"):
            self.refresh()
        self.assertEqual(self.jobs.posts, 0)

    def test_changed_input_during_refresh_blocks_readiness(self):
        self.register()
        self.jobs.on_get = lambda: self.runtime.references[FACT].update(version=2)
        with self.assertRaisesRegex(mlv.MlvError, "version changed"):
            self.refresh()
        self.assertNotEqual(self.state()["status"], "MLVS_VERIFIED")

    def test_output_version_change_during_reconciliation_blocks_readiness(self):
        self.register()
        self.runtime.on_reconcile = lambda: self.runtime.references[TARGET].update(
            version=2
        )
        with self.assertRaisesRegex(mlv.MlvError, "output version"):
            self.refresh()

    def test_scope_change_after_submission_blocks_resume(self):
        self.register()
        self.jobs.on_get = Mock(side_effect=OSError())
        with self.assertRaises(OSError):
            self.refresh()
        self.jobs.scope_data["mode"] = "Full"
        with self.assertRaisesRegex(mlv.MlvError, "definition changed"):
            self.refresh()
        self.assertEqual(self.jobs.posts, 1)

    def test_cdf_does_not_prove_incremental_and_missing_mode_blocks(self):
        self.register()
        with self.assertRaisesRegex(mlv.UnsupportedCapability, "mode evidence"):
            self.refresh(observe_mode=lambda job, target: None)
        self.assertNotEqual(self.state()["status"], "MLVS_VERIFIED")
        self.assertEqual(self.jobs.posts, 1)

    def test_mode_evidence_must_match_exact_job_and_target(self):
        self.register()
        with self.assertRaises(mlv.UnsupportedCapability):
            self.refresh(
                observe_mode=lambda job, target: replace(
                    self.observe(job, target), job_id="other-job"
                )
            )

    def test_full_fallback_valid_without_cdf_and_does_not_claim_incremental(self):
        self.register()
        self.runtime.settings["cdf_enabled"] = False
        self.assertEqual(self.refresh()["outputs"][TARGET]["mode"]["mode"], "Full")

    def test_incremental_requires_both_observation_and_source_prerequisites(self):
        self.register()
        self.runtime.settings["cdf_enabled"] = False
        observe = lambda job, target: replace(
            self.observe(job, target), mode="Incremental"
        )
        with self.assertRaisesRegex(mlv.MlvError, "prerequisites"):
            self.refresh(observe_mode=observe)

    def test_rows_dropped_added_changed_and_duplicate_multiplicity_fail_exactly(self):
        self.register()
        for name, actual in (
            ("dropped", EXPECTED[:1]),
            ("added", [*EXPECTED, EXPECTED[0]]),
            (
                "changed",
                [
                    ("SYNTHETIC_PROVISIONAL", "202601", Decimal("12.0001"), 2),
                    EXPECTED[1],
                ],
            ),
        ):
            self.runtime.actual = actual
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(mlv.MlvError, "reconciliation"),
            ):
                self.lifecycle.refresh(
                    name, (DEFINITION,), self.inputs, observe_mode=self.observe
                )
            state = self.store.read("refresh:" + name)
            self.assertIn("reconciliation", state["outputs"][TARGET])
            self.assertFalse(state["business_publication"])

    def test_constraint_violation_persists_counts_and_blocks(self):
        self.register()
        self.runtime.violations = 1
        with self.assertRaisesRegex(mlv.MlvError, "constraints"):
            self.refresh()
        self.assertEqual(
            self.state()["outputs"][TARGET]["reconciliation"]["constraints"][0][
                "violations"
            ],
            1,
        )

    def test_omitted_declared_constraint_cannot_pass(self):
        definition = replace(DEFINITION, constraints=(row_rule(),))
        self.register(definition)
        self.runtime.omit_constraint = True
        with self.assertRaisesRegex(mlv.MlvError, "constraints"):
            self.lifecycle.refresh(
                "omitted", (definition,), self.inputs, observe_mode=self.observe
            )

    def test_timeout_keeps_job_identity_and_never_resubmits(self):
        self.register()
        self.jobs.statuses = ["InProgress"]
        with self.assertRaises(TimeoutError):
            self.refresh(timeout_seconds=4, poll_seconds=1)
        self.assertEqual(self.state()["job_id"], "job-1")
        self.jobs.statuses = ["Completed"]
        self.now += 10
        self.refresh()
        self.assertEqual(self.jobs.posts, 1)

    def test_lost_ownership_and_failed_release_block_final_readiness(self):
        self.register()
        self.runtime.on_reconcile = lambda: setattr(self, "owned", False)
        with self.assertRaises(PermissionError):
            self.refresh()
        self.assertFalse(self.state()["business_publication"])

    def test_changed_release_identity_cannot_resume_same_attempt(self):
        self.register()
        self.refresh()
        self.release["gold_release_id"] = "other-release"
        self.lifecycle = self.new_lifecycle()
        with self.assertRaisesRegex(mlv.MlvError, "rebound"):
            self.refresh()
        self.assertEqual(self.jobs.posts, 1)

    def test_production_and_nonready_releases_are_not_accepted(self):
        for field, value in (("execution_mode", "production"), ("status", "BUILDING")):
            with self.subTest(field=field), self.assertRaises(mlv.MlvError):
                mlv.MlvLifecycle(
                    self.store,
                    self.runtime,
                    self.jobs,
                    ownership_check=self.check_owner,
                    release=self.release | {field: value},
                    release_check=self.check_release,
                )


class DefinitionTests(unittest.TestCase):
    def test_all_eleven_shipped_queries_are_supported_without_modifying_generator(self):
        definitions = mlv_definitions(
            {
                "target_lakehouse": "gold",
                "schema": "gld",
                "facts": FACTS,
            },
            use_case="treasury",
        )
        self.assertEqual(len(definitions), 11)
        for name, sql in definitions.items():
            with self.subTest(name=name):
                parsed = mlv.parse_mlv_sql(sql, f"gold.gld.{name}")
                self.assertTrue(parsed.source.startswith("gold.gld.fact_"))
                self.assertIn("source_row_count", parsed.columns)
                guarded = sql.replace(
                    "CREATE OR REPLACE MATERIALIZED LAKE VIEW",
                    "CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS",
                )
                self.assertEqual(parsed, mlv.parse_mlv_sql(guarded, f"gold.gld.{name}"))

    def test_single_approved_supported_ddl_and_grain_are_required(self):
        for sql in (
            QUERY + "; DROP TABLE gold.gld.fact_gl_posting",
            QUERY + " HAVING SUM(amount)>0",
            QUERY.replace("SUM(amount)", "ROUND(SUM(amount), 2)"),
            QUERY.replace(FACT, "gold.gld.raw_posting"),
            QUERY.replace("SYNTHETIC_PROVISIONAL", "CERTIFIED"),
            QUERY.replace("GROUP BY period", "GROUP BY period, period"),
            QUERY.replace("FROM", "FROM /* bypass */"),
            "CREATE OR REPLACE MATERIALIZED LAKE VIEW IF NOT EXISTS "
            + TARGET
            + " AS "
            + QUERY,
        ):
            with self.subTest(sql=sql), self.assertRaises((ValueError, mlv.MlvError)):
                mlv.parse_mlv_sql(sql, TARGET)
        with self.assertRaises(ValueError):
            replace(DEFINITION, grain=("other",)).parsed()
        with self.assertRaises(ValueError):
            replace(DEFINITION, inputs=("gold.gld.fact_other",)).parsed()

    def test_logical_key_does_not_imply_engine_enforcement(self):
        logical = constraint("unique", {"columns": ["period"]})
        replace(DEFINITION, constraints=(logical,)).parsed()
        with self.assertRaisesRegex(mlv.UnsupportedCapability, "not engine"):
            replace(
                DEFINITION, constraints=(logical | {"enforcement": "engine"},)
            ).parsed()

    def test_row_rule_uses_existing_schema_and_no_metadata_expression_eval(self):
        replace(DEFINITION, constraints=(row_rule(),)).parsed()
        rule = row_rule()
        rule["definition"]["parameters"] = {"expression": "__import__('os')"}
        with self.assertRaises(mlv.UnsupportedCapability):
            replace(DEFINITION, constraints=(rule,)).parsed()

    def test_approved_drop_exclusion_is_explicitly_unsupported_not_silent(self):
        rule = row_rule()
        rule["definition"]["action"] = "quarantine"
        rule["definition"]["parameters"]["approval_id"] = "approved-exclusion"
        with self.assertRaises(mlv.UnsupportedCapability):
            replace(DEFINITION, constraints=(rule,)).parsed()
        ddl = (
            f"CREATE MATERIALIZED LAKE VIEW {TARGET} "
            "(CONSTRAINT valid CHECK(amount>0) ON MISMATCH DROP) AS " + QUERY
        )
        with self.assertRaises(mlv.UnsupportedCapability):
            mlv.parse_mlv_sql(ddl, TARGET)

    def test_relationship_requires_explicit_pinned_parent_membership(self):
        rel = constraint(
            "relationship",
            {
                "columns": ["period"],
                "reference_table": PARENT,
                "reference_columns": ["period"],
            },
        )
        replace(DEFINITION, inputs=(FACT, PARENT), constraints=(rel,)).parsed()
        with self.assertRaises(ValueError):
            replace(DEFINITION, constraints=(rel,)).parsed()

    def test_exact_multiset_reconciliation_preserves_duplicates_nulls_and_decimals(
        self,
    ):
        a = [(None, Decimal("1.0001")), (None, Decimal("1.0001")), ("b", None)]
        self.assertEqual(mlv.exact_rows(a, a[::-1])["expected_only"], 0)
        self.assertEqual(mlv.exact_rows(a, a[1:])["expected_only"], 1)
        self.assertEqual(mlv.exact_rows(a[1:], a)["actual_only"], 1)
        for value in (float("nan"), Decimal("Infinity")):
            with self.assertRaises(mlv.MlvError):
                mlv.exact_rows([(value,)], [(value,)])


class MetadataConversionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bootstrap = bootstrap_configuration(
            ROOT, "mlv-conversion-fixture", use_case="treasury"
        ).to_document()

    def document(self):
        document = deepcopy(self.bootstrap)
        for obj in document["objects"]:
            if obj["layer"] == "gold":
                obj["enabled"] = True
        return document

    @staticmethod
    def first(document):
        obj = next(o for o in document["objects"] if o["materialization"] == "mlv")
        transformation = next(
            t for t in document["transformations"] if t["object_id"] == obj["id"]
        )
        return obj, transformation

    def test_all_eleven_shipped_objects_convert_with_ordered_spark_result_types(self):
        document = self.document()
        definitions = mlv.definitions_from_metadata(document)
        self.assertEqual(len(definitions), 11)
        self.assertEqual(
            [d.object_id for d in definitions],
            [o["id"] for o in document["objects"] if o["materialization"] == "mlv"],
        )
        for definition in definitions:
            with self.subTest(target=definition.target):
                definition.parsed()
                self.assertEqual(definition.version, "1")
                self.assertEqual(
                    definition.output_columns[-1], ("source_row_count", "bigint")
                )
                self.assertEqual(
                    definition.configuration_sha256, metadata_digest(document)
                )
                for column, kind in definition.output_columns:
                    self.assertNotEqual(kind, "long")
                    if column in definition.parsed().sums:
                        self.assertEqual(kind, "decimal(38,4)")
        posts = next(
            d for d in definitions if d.target.endswith(".mlv_post_population")
        )
        self.assertEqual(dict(posts.output_columns)["post_status"], "string")

    def test_disabled_objects_are_excluded_by_default_and_preview_can_include_them(
        self,
    ):
        self.assertEqual(mlv.definitions_from_metadata(self.bootstrap), ())
        self.assertEqual(
            len(mlv.definitions_from_metadata(self.bootstrap, enabled_only=False)), 11
        )
        document = self.document()
        obj, _transform = self.first(document)
        obj["enabled"] = False
        self.assertEqual(len(mlv.definitions_from_metadata(document)), 10)
        self.assertEqual(
            len(mlv.definitions_from_metadata(document, enabled_only=False)), 11
        )
        with self.assertRaises(ValueError):
            mlv.definitions_from_metadata(document, enabled_only=1)

    def test_invalid_or_reordered_output_columns_are_refused(self):
        for fault in (
            "order",
            "precision",
            "count",
            "complex",
            "nullable",
            "duplicate",
        ):
            document = self.document()
            obj, _transform = self.first(document)
            if fault == "order":
                obj["columns"].reverse()
            elif fault == "precision":
                next(c for c in obj["columns"] if c["data_type"] == "decimal")[
                    "precision"
                ] = 28
            elif fault == "count":
                obj["columns"][-1]["data_type"] = "int"
            elif fault == "complex":
                obj["columns"][0]["data_type"] = "array<string>"
            elif fault == "nullable":
                obj["columns"][0]["nullable"] = "true"
            else:
                obj["columns"].append(deepcopy(obj["columns"][0]))
            with (
                self.subTest(fault=fault),
                self.assertRaises((ValueError, mlv.MlvError)),
            ):
                mlv.definitions_from_metadata(document)

    def test_changed_sql_invalid_hash_version_or_duplicate_transform_are_refused(self):
        for fault in (
            "sql",
            "hash",
            "blank-version",
            "numeric-version",
            "dialect",
            "kind",
            "missing",
            "duplicate",
        ):
            document = self.document()
            obj, transform = self.first(document)
            if fault == "sql":
                transform["definition"]["mlv_query"] = (
                    obj["contract"]["mlv_query"]
                    + "; DROP TABLE gold.gld.fact_gl_posting"
                )
                transform["definition_sha256"] = metadata_digest(
                    transform["definition"]
                )
            elif fault == "hash":
                transform["definition_sha256"] = "0" * 64
            elif fault == "blank-version":
                transform["version"] = ""
            elif fault == "numeric-version":
                transform["version"] = 1
            elif fault == "dialect":
                transform["definition"]["dialect"] = "tsql"
                transform["definition_sha256"] = metadata_digest(
                    transform["definition"]
                )
            elif fault == "kind":
                transform["kind"] = "notebook"
            elif fault == "missing":
                document["transformations"].remove(transform)
            else:
                document["transformations"].append(deepcopy(transform))
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                mlv.definitions_from_metadata(document)

    def test_transformation_version_and_complete_configuration_are_hash_bound(self):
        document = self.document()
        first = mlv.definitions_from_metadata(document)[0]
        _obj, transform = self.first(document)
        transform["version"] = "2"
        changed = mlv.definitions_from_metadata(document)[0]
        self.assertEqual(changed.version, "2")
        self.assertNotEqual(changed.sha256, first.sha256)
        # Approval binding covers the entire pinned configuration, not SQL alone.
        document["version_id"] = "another-approved-version"
        rebound = mlv.definitions_from_metadata(document)[0]
        self.assertNotEqual(changed.configuration_sha256, rebound.configuration_sha256)
        self.assertNotEqual(changed.sha256, rebound.sha256)
        self.assertEqual(changed.version, rebound.version)

    def test_input_dependencies_grain_and_synthetic_gate_are_validated(self):
        for fault in ("dependency", "condition", "source-kind", "grain", "synthetic"):
            document = self.document()
            obj, _transform = self.first(document)
            edge = next(
                e for e in document["dependencies"] if e["downstream_id"] == obj["id"]
            )
            if fault == "dependency":
                edge["upstream_id"] = "missing"
            elif fault == "condition":
                edge["condition"] = "source_committed"
            elif fault == "source-kind":
                next(o for o in document["objects"] if o["id"] == edge["upstream_id"])[
                    "materialization"
                ] = "mlv"
            elif fault == "grain":
                obj["contract"]["grain"] = ["not_the_declared_grain"]
            else:
                obj["contract"]["synthetic_only"] = False
            with (
                self.subTest(fault=fault),
                self.assertRaises((ValueError, mlv.MlvError)),
            ):
                mlv.definitions_from_metadata(document)

    def test_constraint_schema_is_validated_and_detached_from_input(self):
        document = self.document()
        obj, _transform = self.first(document)
        rule = constraint("unique", {"columns": obj["contract"]["grain"]})
        rule["object_id"] = obj["id"]
        document["constraints"].append(rule)
        definition = mlv.definitions_from_metadata(document)[0]
        self.assertEqual(definition.constraints, (rule,))
        rule["definition"]["columns"] = ["undeclared"]
        self.assertNotEqual(
            definition.constraints[0]["definition"]["columns"], ["undeclared"]
        )
        with self.assertRaises(ValueError):
            mlv.definitions_from_metadata(document)
        rule["definition"] = None
        with self.assertRaises(ValueError):
            mlv.definitions_from_metadata(document)

    def test_engine_constraints_or_unhandled_rules_are_not_downgraded(self):
        document = self.document()
        obj, _transform = self.first(document)
        rule = constraint(
            "unique", {"columns": obj["contract"]["grain"]}, enforcement="engine"
        )
        rule["object_id"] = obj["id"]
        document["constraints"].append(rule)
        with self.assertRaises(mlv.UnsupportedCapability):
            mlv.definitions_from_metadata(document)
        document["constraints"].clear()
        document["rules"].append(
            {
                "id": "unhandled-mlv-rule",
                "object_id": obj["id"],
                "handler": "required_columns",
            }
        )
        with self.assertRaises(mlv.UnsupportedCapability):
            mlv.definitions_from_metadata(document)


class AdapterTests(unittest.TestCase):
    def test_selected_lineage_scope_and_actual_policy_are_checked(self):
        value = {
            "id": "exec",
            "settings": {"refreshMode": "Optimal"},
            "currentLakehouseExecutionContext": {
                "mode": "Selected",
                "selectedMlvs": ["gld.mlv_finance"],
            },
            "extendedLineageExecutionContext": {
                "mode": "Selected",
                "selectedLakehouses": [],
            },
        }
        client = SimpleNamespace(
            get_mlv_execution_definition=lambda *args: deepcopy(value)
        )
        jobs = mlv.FabricMlvJobs(client, "ws", "lh", "exec", lakehouse_name="gold")
        self.assertEqual(jobs.scope([DEFINITION]), value)
        value["currentLakehouseExecutionContext"]["mode"] = "All"
        with self.assertRaises(mlv.UnsupportedCapability):
            jobs.scope([DEFINITION])

    def test_notebook_filesystem_persistence_reads_back_exactly(self):
        data = {}
        fs = SimpleNamespace(
            mkdirs=lambda path: True,
            exists=lambda path: path in data,
            put=lambda path, value, overwrite: data.update({path: value}),
            head=lambda path, bound: data[path][:bound],
        )
        store = mlv.NotebookStateStore(fs, "Files/orchestration/mlv")
        self.assertIsNone(store.read("key"))
        store.write("key", {"status": "SUBMISSION_UNCERTAIN"})
        self.assertEqual(store.read("key")["status"], "SUBMISSION_UNCERTAIN")
        fs.put = lambda *args: False
        with self.assertRaises(mlv.MlvError):
            store.write("key", {})

    def test_atomic_local_store_survives_fresh_store_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            mlv.LocalStateStore(directory).write("intent", {"job_id": "job-1"})
            self.assertEqual(
                mlv.LocalStateStore(directory).read("intent"), {"job_id": "job-1"}
            )


@unittest.skipUnless(
    os.environ.get("TREASURY_MLV_SPARK_TESTS") == "1", "Opt-in offline Spark"
)
class SparkReconciliationTests(unittest.TestCase):
    """Exercise the real distributed helper; no Delta jars/downloads or Fabric."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("offline-mlv-reconciliation")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def runtime(self, actual_rows):
        fact = self.spark.createDataFrame(
            [
                ("202601", Decimal("5.0000")),
                ("202601", Decimal("7.0000")),
                ("202602", None),
            ],
            "period string, amount decimal(28,4)",
        )
        actual = self.spark.createDataFrame(
            actual_rows,
            "data_scope string, period string, amount decimal(38,4), source_row_count long",
        )
        runtime = mlv.SparkMlvRuntime(self.spark, schema_enabled=lambda: True)
        frames = {FACT: fact, TARGET: actual}
        runtime._read = lambda ref: frames[ref["table"]]
        return runtime, frames

    def test_exact_reconciliation_and_logical_constraint_counts(self):
        runtime, _frames = self.runtime(EXPECTED[::-1])
        definition = replace(
            DEFINITION,
            constraints=(row_rule(), constraint("unique", {"columns": ["period"]})),
        )
        result = runtime.reconcile(
            definition, {FACT: reference(FACT)}, reference(TARGET)
        )
        self.assertEqual((result["expected_only"], result["actual_only"]), (0, 0))
        self.assertEqual([c["violations"] for c in result["constraints"]], [0, 0, 0])

    def test_dropped_and_duplicate_rows_are_not_hidden(self):
        for rows, expected_only, actual_only in (
            (EXPECTED[:1], 1, 0),
            ([*EXPECTED, EXPECTED[0]], 0, 1),
        ):
            with self.subTest(rows=len(rows)):
                runtime, _frames = self.runtime(rows)
                result = runtime.reconcile(
                    DEFINITION, {FACT: reference(FACT)}, reference(TARGET)
                )
                self.assertEqual(
                    (result["expected_only"], result["actual_only"]),
                    (expected_only, actual_only),
                )

    def test_relationship_missing_parent_and_null_rows_are_violations(self):
        rel = constraint(
            "relationship",
            {
                "columns": ["period"],
                "reference_table": PARENT,
                "reference_columns": ["parent_period"],
            },
        )
        definition = replace(DEFINITION, constraints=(rel,), inputs=(FACT, PARENT))
        runtime, frames = self.runtime(EXPECTED)
        frames[PARENT] = self.spark.createDataFrame(
            [("202601",)], "parent_period string"
        )
        result = runtime.reconcile(
            definition,
            {FACT: reference(FACT), PARENT: reference(PARENT)},
            reference(TARGET),
        )
        self.assertEqual(result["constraints"][1]["violations"], 1)

    def test_schema_type_mismatch_is_not_coerced(self):
        runtime, frames = self.runtime(EXPECTED)
        frames[TARGET] = frames[TARGET].withColumn(
            "amount", frames[TARGET].amount.cast("double")
        )
        with self.assertRaisesRegex(mlv.MlvError, "schema"):
            runtime.reconcile(DEFINITION, {FACT: reference(FACT)}, reference(TARGET))

    def test_extra_columns_are_not_silently_dropped(self):
        from pyspark.sql import functions as F

        runtime, frames = self.runtime(EXPECTED)
        frames[TARGET] = frames[TARGET].withColumn("unexpected", F.lit(1))
        with self.assertRaisesRegex(mlv.MlvError, "schema"):
            runtime.reconcile(DEFINITION, {FACT: reference(FACT)}, reference(TARGET))


if __name__ == "__main__":
    unittest.main()
