"""Fenced attempts, immutable results and conservative submission recovery."""

from __future__ import annotations

import dataclasses
import json
import unittest
import uuid

import test_run_manifests as manifests


class RunLifecycleTests(manifests.ManifestFixture):
    def setUp(self) -> None:
        super().setUp()
        self.manifest = self.read(self.runtime.write_run_manifest(self.fs, self.plan))
        self.task = self.plan.to_document()["tasks"][0]
        self.attempt_id = str(uuid.UUID("22222222-2222-4222-8222-222222222222"))
        self.job_id = "33333333-3333-4333-8333-333333333333"

    def result(self, **overrides):
        options = {
            "attempt_id": self.attempt_id,
            "fence": 1,
            "job_id": self.job_id,
            "status": "SUCCEEDED",
            "finished_utc": "2026-11-06T09:00:00Z",
            "artifact_refs": [
                {
                    "kind": "raw_batch",
                    "location": self.task["target_path"],
                    "version": None,
                    "sha256": "a" * 64,
                }
            ],
        }
        options.update(overrides)
        return self.runtime.make_task_result(
            self.manifest, self.task["task_id"], **options
        )

    def attempt(self, **overrides):
        values = {
            "run_id": self.plan.run_id,
            "task_id": self.task["task_id"],
            "attempt_id": self.attempt_id,
            "fence": 1,
            "status": "SUBMITTED",
            "job_id": self.job_id,
        }
        values.update(overrides)
        return self.runtime.RecordedAttempt(**values)

    def test_source_leases_are_stable_across_runs_but_batches_are_distinct(self):
        from fabric_medallion_deployer.metadata import compile_run_plan

        other = compile_run_plan(
            self.config, "run-2", "2026-11-07T08:30:00Z"
        ).to_document()["tasks"]
        tasks = self.plan.to_document()["tasks"]
        self.assertEqual(tasks[0]["source_lock_key"], tasks[1]["source_lock_key"])
        self.assertEqual(tasks[0]["source_lock_key"], other[0]["source_lock_key"])
        self.assertEqual(tasks[0]["target_lock_key"], other[0]["target_lock_key"])
        self.assertNotEqual(tasks[0]["source_batch_id"], other[0]["source_batch_id"])

    def test_result_is_detached_typed_and_content_addressed(self):
        result = self.result()
        document = result.to_document()
        self.assertEqual(document["readiness"], ["succeeded"])
        self.assertEqual(document["source_batch_id"], self.task["source_batch_id"])
        self.assertEqual(len(result.result_sha256), 64)
        document["artifact_refs"].clear()
        self.assertEqual(len(result.to_document()["artifact_refs"]), 1)
        self.assertEqual(self.runtime.load_task_result(result.result_json), result)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            result.fence = 2

    def test_result_write_and_reconciliation_are_idempotent(self):
        result = self.result()
        calls = []

        def ingest(body, digest):
            calls.append((body, digest))

        path = self.runtime.persist_task_result(self.fs, result, ingest=ingest)
        self.assertEqual(
            self.runtime.persist_task_result(self.fs, result, ingest=ingest),
            path,
        )
        self.assertIn(f"/{self.task['task_id']}/{self.attempt_id}.json", path)
        self.assertEqual(calls, [(result.result_json, result.result_sha256)] * 2)
        self.assertEqual(
            self.runtime.read_task_result(
                self.fs,
                path,
                run_id=result.run_id,
                task_id=result.task_id,
                attempt_id=result.attempt_id,
                expected_sha256=result.result_sha256,
            ),
            result,
        )

    def test_sql_failure_retains_the_exact_result_for_reconciliation(self):
        result = self.result()

        def unavailable(_body, _digest):
            raise TimeoutError("synthetic control timeout")

        with self.assertRaises(TimeoutError):
            self.runtime.persist_task_result(self.fs, result, ingest=unavailable)
        writes = []
        path = self.runtime.persist_task_result(
            self.fs, result, ingest=lambda body, digest: writes.append(digest)
        )
        self.assertTrue(self.fs.exists(path))
        self.assertEqual(writes, [result.result_sha256])

    def test_an_attempt_result_cannot_be_overwritten(self):
        self.runtime.persist_task_result(self.fs, self.result())
        with self.assertRaisesRegex(RuntimeError, "changed|overwrite"):
            self.runtime.persist_task_result(
                self.fs,
                self.result(
                    status="FAILED",
                    error_code="DQ_BLOCKING_FAILURE",
                    artifact_refs=[],
                ),
            )

    def test_invalid_fences_identities_statuses_and_error_payloads_fail(self):
        for values in (
            {"fence": True},
            {"fence": 0},
            {"fence": -1},
            {"attempt_id": "../other"},
            {"job_id": "job?access_token=synthetic"},
            {"status": "completed"},
            {"finished_utc": "2026-11-06T09:00:00+02:00"},
            {"status": "FAILED", "error_code": "password=synthetic"},
            {"status": "FAILED", "error_code": None},
            {"status": "SUCCEEDED", "error_code": "FAILED"},
            {"artifact_refs": []},
            {"metrics": {"rows_written": True}},
            {"metrics": {"source_row": "not permitted"}},
        ):
            with self.subTest(values=values), self.assertRaises(RuntimeError):
                self.result(**values)

    def test_readiness_requires_matching_publication_evidence(self):
        with self.assertRaisesRegex(RuntimeError, "readiness|evidence"):
            self.result(readiness=["succeeded", "source_committed"])
        with self.assertRaisesRegex(RuntimeError, "readiness|failed|failure"):
            self.result(
                status="FAILED",
                error_code="SOURCE_VALIDATION_FAILED",
                artifact_refs=[],
                readiness=["succeeded"],
            )
        with self.assertRaisesRegex(RuntimeError, "credential|field|artifact"):
            self.result(
                artifact_refs=[
                    {
                        "kind": "raw_batch",
                        "location": self.task["target_path"],
                        "version": None,
                        "sha256": "a" * 64,
                        "password": "synthetic_rejection_fixture",
                    }
                ]
            )

    def test_active_jobs_are_never_retried_even_if_a_result_exists(self):
        for state in ("NotStarted", "InProgress"):
            decision = self.runtime.decide_recovery(
                self.attempt(),
                observed_job_status=state,
                result=self.result(),
                outputs_verified=True,
                publication_checked=True,
            )
            self.assertEqual(decision.action, "WAIT")

    def test_uncertain_submission_without_job_identity_requires_reconciliation(self):
        decision = self.runtime.decide_recovery(
            self.attempt(status="UNCERTAIN", job_id=None),
            publication_checked=True,
        )
        self.assertEqual(decision.action, "MANUAL_RECONCILIATION")
        self.assertNotIn(self.job_id, json.dumps(dataclasses.asdict(decision)))

    def test_known_rejection_and_verified_failed_job_can_allow_retry(self):
        rejected = self.runtime.decide_recovery(
            self.attempt(status="NOT_SUBMITTED", job_id=None)
        )
        self.assertEqual(rejected.action, "RETRY_ALLOWED")
        unchecked = self.runtime.decide_recovery(
            self.attempt(), observed_job_status="Failed"
        )
        self.assertEqual(unchecked.action, "MANUAL_RECONCILIATION")
        checked = self.runtime.decide_recovery(
            self.attempt(),
            observed_job_status="Failed",
            publication_checked=True,
        )
        self.assertEqual(checked.action, "RETRY_ALLOWED")

    def test_completed_jobs_require_a_verified_matching_result(self):
        missing = self.runtime.decide_recovery(
            self.attempt(), observed_job_status="Completed"
        )
        self.assertEqual(missing.action, "MANUAL_RECONCILIATION")
        unverified = self.runtime.decide_recovery(
            self.attempt(),
            observed_job_status="Completed",
            result=self.result(),
        )
        self.assertEqual(unverified.action, "MANUAL_RECONCILIATION")
        accepted = self.runtime.decide_recovery(
            self.attempt(),
            observed_job_status="Completed",
            result=self.result(),
            outputs_verified=True,
        )
        self.assertEqual(accepted.action, "RECONCILE")
        with self.assertRaisesRegex(RuntimeError, "identity|fence|attempt"):
            self.runtime.decide_recovery(
                self.attempt(fence=2),
                observed_job_status="Completed",
                result=self.result(),
                outputs_verified=True,
            )

    def test_failed_results_do_not_create_successful_downstream_readiness(self):
        failure = self.result(
            status="FAILED",
            error_code="SOURCE_VALIDATION_FAILED",
            artifact_refs=[],
        )
        self.assertEqual(failure.to_document()["readiness"], [])
        decision = self.runtime.decide_recovery(
            self.attempt(),
            observed_job_status="Failed",
            result=failure,
        )
        self.assertEqual(decision.action, "RECONCILE")
        with self.assertRaisesRegex(RuntimeError, "job|success"):
            self.runtime.decide_recovery(
                self.attempt(),
                observed_job_status="Failed",
                result=self.result(),
                outputs_verified=True,
            )


if __name__ == "__main__":
    unittest.main()
