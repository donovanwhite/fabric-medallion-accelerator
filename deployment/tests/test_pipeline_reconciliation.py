"""Authoritative pipeline dispatch, activity evidence and bounded SQL callbacks."""

from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import unittest
import uuid
from types import SimpleNamespace

import test_run_manifests as manifests
from fabric_medallion_deployer.metadata import compile_run_plan, load_configuration


class PipelineReconciliationTests(manifests.ManifestFixture):
    def setUp(self) -> None:
        super().setUp()
        self.manifest = self.read(self.runtime.write_run_manifest(self.fs, self.plan))
        self.task = self.plan.to_document()["tasks"][0]
        self.attempt_id = str(uuid.UUID(int=70))
        self.workspace_id = str(uuid.UUID(int=71))
        self.pipeline_run_id = str(uuid.UUID(int=72))
        self.activity_id = str(uuid.UUID(int=73))
        self.location = (
            "https://api.fabric.microsoft.com/v1/workspaces/"
            + self.workspace_id
            + "/datapipelines/pipelineruns/"
            + self.pipeline_run_id
            + "/queryactivityruns"
        )
        self.payload = {
            "format_version": 1,
            "run_id": self.plan.run_id,
            "version_id": self.config.version_id,
            "snapshot_sha256": self.plan.snapshot_sha256,
            "layer": "bronze",
            "attempts": [
                {
                    "task_id": self.task["task_id"],
                    "attempt_id": self.attempt_id,
                    "fence": 1,
                    "source_fence": 1,
                    "status": "SUBMISSION_PENDING",
                    "job_location": self.location,
                }
            ],
        }

    def dispatch(self):
        return self.runtime.make_pipeline_dispatch(self.manifest, self.payload)

    def partial_dispatch(self):
        document = self.config.to_document()
        extra = copy.deepcopy(document["objects"][0])
        extra.update(id="bas-bronze-unsubmitted", name="unsubmitted")
        extra["contract"]["source_object"] = "unsubmitted"
        document["objects"].append(extra)
        self.config = load_configuration(document)
        self.plan = compile_run_plan(
            self.config,
            self.plan.run_id,
            "2026-11-06T08:30:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=self.config.definition_sha256,
            require_ready=True,
        )
        self.fs = manifests.LocalFs(self.fs.root / "partial")
        self.manifest = self.read(self.runtime.write_run_manifest(self.fs, self.plan))
        tasks = self.plan.to_document()["tasks"]
        missing = next(
            task["task_id"] for task in tasks if task["object_id"] == extra["id"]
        )
        self.payload.update(
            format_version=2,
            snapshot_sha256=self.plan.snapshot_sha256,
            undispatched_task_ids=[missing],
        )
        return self.dispatch(), missing

    def test_partial_bronze_dispatch_accounts_for_every_selected_task(self):
        dispatch, missing = self.partial_dispatch()
        self.assertEqual(dispatch.to_document()["undispatched_task_ids"], [missing])
        for values in ([], [missing, missing], [self.task["task_id"]], ["f" * 64]):
            payload = copy.deepcopy(self.payload)
            payload["undispatched_task_ids"] = values
            with self.subTest(values=values), self.assertRaises(RuntimeError):
                self.runtime.make_pipeline_dispatch(self.manifest, payload)

    def test_partial_dispatch_persists_failed_copy_and_unsubmitted_membership(self):
        dispatch, missing = self.partial_dispatch()

        class Client:
            def request(_self, method, location, *, body, expected):
                return SimpleNamespace(
                    content=json.dumps([self.activity(status="Failed")]).encode("utf-8")
                )

        result = self.runtime.reconcile_bronze_pipeline(
            None,
            self.fs,
            self.manifest,
            dispatch,
            client=Client(),
            observed_utc="2026-11-06T09:01:00Z",
            ingestion=None,
        )
        self.assertTrue(result["has_failed"])
        self.assertEqual(len(result["callbacks"]), 1)
        receipt = json.loads(self.fs.head(result["recovery_path"], 10000))
        self.assertEqual(receipt["undispatched_task_ids"], [missing])
        self.assertEqual(receipt["unresolved_attempts"], [])
        self.assertEqual(len(receipt["terminal_results"]), 1)

    def test_missing_or_running_copy_remains_uncertain_without_invented_result(self):
        for rows in ([], [self.activity(status="InProgress")]):
            with self.subTest(rows=rows):

                class Client:
                    content = json.dumps(rows).encode("utf-8")

                    def request(_self, method, location, *, body, expected):
                        return SimpleNamespace(content=_self.content)

                result = self.runtime.reconcile_bronze_pipeline(
                    None,
                    self.fs,
                    self.manifest,
                    self.dispatch(),
                    client=Client(),
                    observed_utc="2026-11-06T09:01:00Z",
                    ingestion=None,
                )
                self.assertTrue(result["has_failed"])
                self.assertEqual(result["callbacks"], [])
                receipt = json.loads(self.fs.head(result["recovery_path"], 10000))
                self.assertEqual(len(receipt["unresolved_attempts"]), 1)
                self.assertEqual(receipt["terminal_results"], [])
                self.assertEqual(
                    receipt["unresolved_attempts"][0]["attempt_id"], self.attempt_id
                )
                self.assertFalse(
                    self.fs.exists(
                        f"Files/orchestration/run-1/results/{self.task['task_id']}/{self.attempt_id}.json"
                    )
                )

    def test_no_reserved_copy_is_reported_as_unsubmitted_not_success(self):
        self.payload.update(
            format_version=2, attempts=[], undispatched_task_ids=[self.task["task_id"]]
        )
        result = self.runtime.reconcile_bronze_pipeline(
            None,
            self.fs,
            self.manifest,
            self.dispatch(),
            client=None,
            observed_utc="2026-11-06T09:01:00Z",
            ingestion=None,
        )
        self.assertTrue(result["has_failed"])
        self.assertEqual(result["callbacks"], [])
        self.assertEqual(result["undispatched_count"], 1)

    def test_terminal_copy_results_survive_another_uncertain_reservation(self):
        _, missing = self.partial_dispatch()
        extra = copy.deepcopy(self.payload["attempts"][0])
        extra.update(task_id=missing, attempt_id=str(uuid.UUID(int=75)))
        self.payload.update(
            attempts=[*self.payload["attempts"], extra], undispatched_task_ids=[]
        )

        class Client:
            def request(_self, method, location, *, body, expected):
                return SimpleNamespace(
                    content=json.dumps([self.activity(status="Failed")]).encode("utf-8")
                )

        result = self.runtime.reconcile_bronze_pipeline(
            None,
            self.fs,
            self.manifest,
            self.dispatch(),
            client=Client(),
            observed_utc="2026-11-06T09:01:00Z",
            ingestion=None,
        )
        self.assertEqual(len(result["callbacks"]), 1)
        self.assertEqual(result["unresolved_count"], 1)
        self.assertTrue(result["has_failed"])
        self.runtime.verify_pipeline_callbacks(
            self.fs,
            self.manifest,
            result["callbacks"][0]["path"],
            result["callbacks"][0]["sha256"],
        )
        self.assertNotIn("DO_NOT_PERSIST", self.fs.head(result["recovery_path"], 10000))

    def activity(self, **overrides):
        value = {
            "pipelineRunId": self.pipeline_run_id,
            "activityRunId": self.activity_id,
            "activityName": "copy_sql_to_bronze",
            "activityType": "Copy",
            "status": "Succeeded",
            "activityRunEnd": "2026-11-06T09:00:00.1234567Z",
            "userProperties": {
                "run_id": self.plan.run_id,
                "task_id": self.task["task_id"],
                "attempt_id": self.attempt_id,
            },
            "input": {"sql": "DO_NOT_PERSIST"},
            "output": {"sample": "DO_NOT_PERSIST"},
            "error": {"message": "DO_NOT_PERSIST"},
        }
        value.update(overrides)
        return value

    def test_dispatch_csv_preserves_exact_prepared_membership_and_execution_scope(self):
        dispatch = self.dispatch()
        stream = io.StringIO()
        writer = csv.writer(stream, escapechar="\\", quoting=csv.QUOTE_ALL)
        writer.writerow(["dispatch_json"])
        writer.writerow([dispatch.dispatch_json])
        path = "Files/orchestration/run-1/bronze_dispatch.csv"
        self.fs.put(path, "\ufeff" + stream.getvalue())
        restored = self.runtime.read_pipeline_dispatch(
            self.fs,
            path,
            self.manifest,
            expected_sha256=dispatch.dispatch_sha256,
            layer="bronze",
        )
        self.assertEqual(restored, dispatch)
        for changes in (
            {"attempts": []},
            {"run_id": "another-run"},
            {"snapshot_sha256": "f" * 64},
        ):
            value = copy.deepcopy(self.payload)
            value.update(changes)
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                self.runtime.make_pipeline_dispatch(self.manifest, value)
        value = copy.deepcopy(self.payload)
        value["attempts"][0]["job_location"] = self.location + "?token=not-allowed"
        with self.assertRaises(RuntimeError):
            self.runtime.make_pipeline_dispatch(self.manifest, value)

    def test_notebook_workers_receive_fences_from_authoritative_layer_dispatch(self):
        payload = copy.deepcopy(self.payload)
        payload["layer"] = "silver"
        payload["attempts"][0]["task_id"] = self.plan.to_document()["tasks"][1][
            "task_id"
        ]
        pipeline = self.runtime.make_pipeline_dispatch(self.manifest, payload)
        worker = self.runtime.notebook_layer_dispatch(self.manifest, pipeline)
        self.assertEqual(worker.layer, "silver")
        self.assertEqual(
            worker.to_document()["attempts"],
            [
                {
                    key: payload["attempts"][0][key]
                    for key in ("task_id", "attempt_id", "fence", "source_fence")
                }
            ],
        )
        self.assertNotIn("job_location", worker.dispatch_json)
        self.assertNotEqual(worker.dispatch_sha256, pipeline.dispatch_sha256)

    def test_silver_dispatch_accepts_sql_exported_null_without_changing_its_hash(self):
        payload = copy.deepcopy(self.payload)
        payload["layer"] = "silver"
        payload["attempts"][0]["task_id"] = self.plan.to_document()["tasks"][1][
            "task_id"
        ]
        payload["undispatched_task_ids"] = None
        exported = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(exported.encode("ascii")).hexdigest()
        stream = io.StringIO()
        writer = csv.writer(stream, escapechar="\\", quoting=csv.QUOTE_ALL)
        writer.writerow(["dispatch_json"])
        writer.writerow([exported])
        path = "Files/orchestration/run-1/silver_dispatch.csv"
        self.fs.put(path, stream.getvalue())

        dispatch = self.runtime.read_pipeline_dispatch(
            self.fs,
            path,
            self.manifest,
            expected_sha256=digest,
            layer="silver",
        )
        self.assertEqual(dispatch.dispatch_json, exported)
        self.assertEqual(dispatch.dispatch_sha256, digest)
        worker = self.runtime.notebook_layer_dispatch(self.manifest, dispatch)
        self.assertEqual(worker.to_document()["attempts"][0]["fence"], 1)

        for invalid in ([], [payload["attempts"][0]["task_id"]], "unexpected"):
            payload["undispatched_task_ids"] = invalid
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                self.runtime.make_pipeline_dispatch(self.manifest, payload)

    def test_activity_evidence_is_terminal_exact_and_metadata_only(self):
        calls = []
        response = [self.activity()]

        class Client:
            def request(self, method, location, *, body, expected):
                calls.append((method, location, body, expected))
                return SimpleNamespace(content=json.dumps(response).encode("utf-8"))

        evidence = self.runtime.pipeline_activity_evidence(
            Client(),
            self.manifest,
            self.dispatch(),
            observed_utc="2026-11-06T09:01:00Z",
        )
        value = evidence[self.task["task_id"]]
        self.assertEqual(value["job_id"], self.activity_id)
        self.assertEqual(value["observed_job_status"], "Completed")
        self.assertEqual(value["finished_utc"], "2026-11-06T09:00:00.123456Z")
        self.assertNotIn("DO_NOT_PERSIST", json.dumps(value))
        self.assertEqual(calls[0][:2], ("POST", self.location))
        self.assertEqual(calls[0][2]["lastUpdatedAfter"], "2026-11-06T08:30:00.000000Z")
        for rows in (
            [],
            [self.activity(), self.activity()],
            [self.activity(status="InProgress")],
            [self.activity(pipelineRunId=str(uuid.UUID(int=74)))],
        ):
            response[:] = rows
            with self.subTest(rows=rows), self.assertRaises(RuntimeError):
                self.runtime.pipeline_activity_evidence(
                    Client(),
                    self.manifest,
                    self.dispatch(),
                    observed_utc="2026-11-06T09:01:00Z",
                )

    def test_callbacks_require_persisted_result_and_are_bounded_immutable_csv(self):
        result = self.runtime.make_task_result(
            self.manifest,
            self.task["task_id"],
            attempt_id=self.attempt_id,
            fence=1,
            job_id=self.activity_id,
            status="SUCCEEDED",
            finished_utc="2026-11-06T09:00:00Z",
            artifact_refs=[
                {
                    "kind": "raw_batch",
                    "location": self.task["target_path"],
                    "version": None,
                    "sha256": "a" * 64,
                }
            ],
        )
        evidence = {
            "job_id": self.activity_id,
            "job_location": self.location,
            "observed_job_status": "Completed",
            "job_evidence_sha256": "e" * 64,
        }
        with self.assertRaises(FileNotFoundError):
            self.runtime.write_pipeline_callbacks(
                self.fs, self.manifest, [(result, evidence)]
            )
        self.runtime.persist_task_result(self.fs, result)
        pages = self.runtime.write_pipeline_callbacks(
            self.fs, self.manifest, [(result, evidence)], max_bytes=4096
        )
        self.assertEqual(len(pages), 1)
        text = self.fs.head(pages[0]["path"], 4096)
        rows = list(csv.DictReader(io.StringIO(text), escapechar="\\"))
        self.assertEqual(rows[0]["result_json"], result.result_json)
        self.assertEqual(rows[0]["job_location"], self.location)
        self.assertEqual(
            pages,
            self.runtime.write_pipeline_callbacks(
                self.fs, self.manifest, [(result, evidence)], max_bytes=4096
            ),
        )
        self.runtime.verify_pipeline_callbacks(
            self.fs, self.manifest, pages[0]["path"], pages[0]["sha256"]
        )
        self.fs.put(
            pages[0]["path"], text.replace('"e' + "e" * 63, '"f' + "e" * 63), True
        )
        with self.assertRaisesRegex(RuntimeError, "hash"):
            self.runtime.verify_pipeline_callbacks(
                self.fs, self.manifest, pages[0]["path"], pages[0]["sha256"]
            )
        with self.assertRaisesRegex(RuntimeError, "bound"):
            self.runtime.write_pipeline_callbacks(
                self.fs, self.manifest, [(result, evidence)], max_bytes=10
            )

    def test_copy_outputs_are_sealed_before_callback_and_failures_stay_failed(self):
        self.fs.put(self.task["target_path"], "id,amount\n1,123.4500\n")

        class Client:
            def request(_self, method, location, *, body, expected):
                return SimpleNamespace(
                    content=json.dumps([self.activity()]).encode("utf-8")
                )

        ingestion = SimpleNamespace(
            read_raw=lambda spark, fs, context, table: context["raw_source_files"][
                table["name"]
            ],
            snapshot_fingerprint=lambda frame: ("a" * 64, 1),
            _operation_errors=lambda: (RuntimeError, ValueError, OSError),
        )
        result = self.runtime.reconcile_bronze_pipeline(
            None,
            self.fs,
            self.manifest,
            self.dispatch(),
            client=Client(),
            observed_utc="2026-11-06T09:01:00Z",
            ingestion=ingestion,
        )
        self.assertFalse(result["has_failed"])
        self.assertEqual(len(result["callbacks"]), 1)
        text = self.fs.head(result["callbacks"][0]["path"], 10000)
        row = next(csv.DictReader(io.StringIO(text), escapechar="\\"))
        value = json.loads(row["result_json"])
        self.assertEqual(value["metrics"]["rows_read"], 1)
        self.assertEqual(value["artifact_refs"][0]["sha256"], "a" * 64)
        self.assertTrue(
            self.fs.exists(
                f"Files/orchestration/run-1/results/{self.task['task_id']}/{self.attempt_id}.json"
            )
        )

    def file_copy_case(self, source_type, case):
        document = self.config.to_document()
        document["connections"][0].update(
            connector_type=source_type.capitalize(), authentication_type="Basic"
        )
        document["feeds"][0].update(source_type=source_type, geometry=None)
        document["objects"][0].update(
            required_capabilities=[],
            contract={
                "source_directory": "/exports",
                "file_pattern": "postings_*.csv",
                "recursive": False,
                "file_format": "csv",
                "arrival_policy": {"kind": "marker", "marker": "_SUCCESS"},
            },
        )
        document["capabilities"].extend(
            {
                "id": name,
                "verified": True,
                "evidence_sha256": "e" * 64,
            }
            for name in (
                f"{source_type}-copy-basic",
                f"{source_type}-arrival-observer",
            )
        )
        config = load_configuration(document)
        plan = compile_run_plan(
            config,
            "run-1",
            "2026-11-06T08:30:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=config.definition_sha256,
            require_ready=True,
        )
        fs = manifests.LocalFs(self.fs.root / f"{source_type}-{case}")
        manifest_path = self.runtime.write_run_manifest(fs, plan)
        manifest = self.runtime.read_run_manifest(
            fs,
            manifest_path,
            expected_snapshot_sha256=plan.snapshot_sha256,
            run_id=plan.run_id,
            version_id=config.version_id,
            load_configuration=load_configuration,
        )
        task = plan.to_document()["tasks"][0]
        payload = copy.deepcopy(self.payload)
        payload["snapshot_sha256"] = plan.snapshot_sha256
        payload["attempts"][0]["task_id"] = task["task_id"]
        dispatch = self.runtime.make_pipeline_dispatch(manifest, payload)
        text = "id,amount\n1,123.4500\n"
        fs.put(task["target_path"] + "/postings_01.csv", text)
        arrival = self.runtime.seal_file_arrival(
            [
                {
                    "relative_path": "postings_01.csv",
                    "size_bytes": len(text.encode("ascii")),
                    "modified_utc": "2026-11-06T08:00:00Z",
                }
            ],
            {"kind": "marker", "marker": "_SUCCESS"},
            observed_utc="2026-11-06T08:30:00Z",
            present_markers=["_SUCCESS"],
        )
        return fs, manifest, task, dispatch, arrival

    def test_completed_file_copy_without_remote_reobservation_stays_unresolved(self):
        ingestion = SimpleNamespace(
            read_raw=lambda *_args: self.fail("Unverified file Copy cannot be read"),
            snapshot_fingerprint=lambda _frame: ("a" * 64, 1),
            _operation_errors=lambda: (RuntimeError, ValueError, OSError),
        )
        for source_type in ("ftp", "sftp"):
            with self.subTest(source_type=source_type):
                fs, manifest, task, dispatch, arrival = self.file_copy_case(
                    source_type, "completed"
                )
                activity = self.activity(
                    activityName=f"copy_{source_type}_to_bronze",
                    userProperties={
                        "run_id": manifest.run_id,
                        "task_id": task["task_id"],
                        "attempt_id": self.attempt_id,
                    },
                )

                class Client:
                    content = json.dumps([activity]).encode("utf-8")

                    def request(_self, method, location, *, body, expected):
                        return SimpleNamespace(content=_self.content)

                result = self.runtime.reconcile_bronze_pipeline(
                    None,
                    fs,
                    manifest,
                    dispatch,
                    client=Client(),
                    observed_utc="2026-11-06T09:01:00Z",
                    ingestion=ingestion,
                    file_arrivals={task["object_id"]: arrival},
                )
                self.assertTrue(result["has_failed"])
                self.assertEqual(result["unresolved_count"], 1)
                self.assertEqual(result["callbacks"], [])
                recovery = json.loads(fs.head(result["recovery_path"], 10000))
                self.assertEqual(
                    recovery["unresolved_attempts"][0]["error_code"],
                    "COPY_SOURCE_REOBSERVATION_UNAVAILABLE",
                )
                self.assertEqual(recovery["terminal_results"], [])
                self.assertFalse(
                    fs.exists(
                        f"Files/orchestration/run-1/results/{task['task_id']}/{self.attempt_id}.json"
                    )
                )

    def test_failed_file_copy_is_reconciled_without_reading_partial_files(self):
        ingestion = SimpleNamespace(
            read_raw=lambda *_args: self.fail("Failed file Copy cannot be read"),
            _operation_errors=lambda: (RuntimeError, ValueError, OSError),
        )
        for source_type in ("ftp", "sftp"):
            for service_status in ("Failed", "Cancelled"):
                with self.subTest(source_type=source_type, status=service_status):
                    fs, manifest, task, dispatch, arrival = self.file_copy_case(
                        source_type, service_status.lower()
                    )
                    activity = self.activity(
                        activityName=f"copy_{source_type}_to_bronze",
                        status=service_status,
                        userProperties={
                            "run_id": manifest.run_id,
                            "task_id": task["task_id"],
                            "attempt_id": self.attempt_id,
                        },
                    )

                    class Client:
                        content = json.dumps([activity]).encode("utf-8")

                        def request(_self, method, location, *, body, expected):
                            return SimpleNamespace(content=_self.content)

                    result = self.runtime.reconcile_bronze_pipeline(
                        None,
                        fs,
                        manifest,
                        dispatch,
                        client=Client(),
                        observed_utc="2026-11-06T09:01:00Z",
                        ingestion=ingestion,
                        file_arrivals={task["object_id"]: arrival},
                    )
                    self.assertTrue(result["has_failed"])
                    self.assertEqual(result["unresolved_count"], 0)
                    self.assertEqual(len(result["callbacks"]), 1)
                    recovery = json.loads(fs.head(result["recovery_path"], 10000))
                    self.assertEqual(recovery["unresolved_attempts"], [])
                    self.assertEqual(len(recovery["terminal_results"]), 1)
                    row = next(
                        csv.DictReader(
                            io.StringIO(
                                fs.head(result["callbacks"][0]["path"], 10000)
                            ),
                            escapechar="\\",
                        )
                    )
                    value = json.loads(row["result_json"])
                    self.assertEqual(value["status"], service_status.upper())
                    self.assertEqual(value["error_code"], "COPY_" + service_status.upper())
                    self.assertEqual(value["readiness"], [])

    def test_failed_copy_does_not_inspect_or_certify_partial_raw_files(self):
        class Client:
            def request(_self, method, location, *, body, expected):
                return SimpleNamespace(
                    content=json.dumps([self.activity(status="Failed")]).encode("utf-8")
                )

        ingestion = SimpleNamespace(
            read_raw=lambda *args: self.fail("Failed Copy input cannot be certified"),
            _operation_errors=lambda: (RuntimeError, ValueError, OSError),
        )
        result = self.runtime.reconcile_bronze_pipeline(
            None,
            self.fs,
            self.manifest,
            self.dispatch(),
            client=Client(),
            observed_utc="2026-11-06T09:01:00Z",
            ingestion=ingestion,
        )
        self.assertTrue(result["has_failed"])
        row = next(
            csv.DictReader(
                io.StringIO(self.fs.head(result["callbacks"][0]["path"], 10000)),
                escapechar="\\",
            )
        )
        value = json.loads(row["result_json"])
        self.assertEqual(value["status"], "FAILED")
        self.assertEqual(value["readiness"], [])
        self.assertEqual(value["error_code"], "COPY_FAILED")

    def test_silver_parent_failure_persists_failed_results_without_publication(self):
        payload = copy.deepcopy(self.payload)
        payload["layer"] = "silver"
        payload["attempts"][0]["task_id"] = self.plan.to_document()["tasks"][1][
            "task_id"
        ]
        dispatch = self.runtime.make_pipeline_dispatch(self.manifest, payload)
        activity = self.activity(
            activityType="TridentNotebook",
            activityName="orchestrate_silver",
            status="Failed",
            userProperties={
                "run_id": self.plan.run_id,
                "layer": "silver",
                "dispatch_sha256": dispatch.dispatch_sha256,
            },
        )

        class Client:
            def request(_self, method, location, *, body, expected):
                return SimpleNamespace(content=json.dumps([activity]).encode("utf-8"))

        result = self.runtime.reconcile_silver_pipeline(
            None,
            self.fs,
            self.manifest,
            dispatch,
            client=Client(),
            observed_utc="2026-11-06T09:01:00Z",
            lakehouse="lh_brz_slv_treasury",
            ingestion=None,
            load_configuration=None,
        )
        row = next(
            csv.DictReader(
                io.StringIO(self.fs.head(result["callbacks"][0]["path"], 10000)),
                escapechar="\\",
            )
        )
        value = json.loads(row["result_json"])
        self.assertTrue(result["has_failed"])
        self.assertEqual(value["status"], "FAILED")
        self.assertEqual(value["readiness"], [])

    def test_silver_results_require_verified_stages_and_authoritative_source_commit(
        self,
    ):
        import test_runtime_controls as legacy
        from fabric_medallion_deployer.metadata import load_configuration

        self.fs.put(self.task["target_path"], "id,amount\n1,123.4500\n")
        payload = copy.deepcopy(self.payload)
        payload["layer"] = "silver"
        task = self.plan.to_document()["tasks"][1]
        payload["attempts"][0]["task_id"] = task["task_id"]
        pipeline = self.runtime.make_pipeline_dispatch(self.manifest, payload)
        worker = self.runtime.notebook_layer_dispatch(self.manifest, pipeline)
        contexts = self.runtime.prepare_silver_contexts(
            self.fs, self.manifest, load_configuration=load_configuration
        )
        calls = []

        class Ingestion:
            manifest_processing_context = staticmethod(
                legacy.runtime.manifest_processing_context
            )

            def load_silver(_self, spark, fs, context, *, table_names=None):
                return [
                    {
                        "status": "SUCCESS",
                        "source_rows": 1,
                        "silver_rows": 1,
                        "quarantine_rows": 0,
                    }
                ]

            def delta_version(_self, spark, target):
                return 3

            def cleanup_snapshots(_self, spark, fs, context):
                calls.append("retention_verified")

            def source_commit_evidence(_self, spark, context):
                calls.append("commit_verified")
                return {
                    "kind": "source_commit",
                    "location": "lh_brz_slv_treasury.audit.bronze_silver_validation",
                    "version": 5,
                    "sha256": "e" * 64,
                }

            def _operation_errors(_self):
                return (RuntimeError, ValueError, OSError)

        ingestion = Ingestion()
        self.runtime.execute_silver_task(
            None,
            self.fs,
            self.manifest,
            worker,
            task_id=task["task_id"],
            worker_id="silver-worker",
            context_path=contexts["bas-finance-sql"]["path"],
            context_sha256=contexts["bas-finance-sql"]["sha256"],
            lakehouse="lh_brz_slv_treasury",
            code_sha256="b" * 64,
            ingestion=ingestion,
        )
        activity = self.activity(
            activityType="TridentNotebook",
            activityName="orchestrate_silver",
            userProperties={
                "run_id": self.plan.run_id,
                "layer": "silver",
                "dispatch_sha256": pipeline.dispatch_sha256,
            },
        )

        class Client:
            def request(_self, method, location, *, body, expected):
                return SimpleNamespace(content=json.dumps([activity]).encode("utf-8"))

        result = self.runtime.reconcile_silver_pipeline(
            None,
            self.fs,
            self.manifest,
            pipeline,
            client=Client(),
            observed_utc="2026-11-06T09:01:00Z",
            lakehouse="lh_brz_slv_treasury",
            ingestion=ingestion,
            load_configuration=load_configuration,
        )
        row = next(
            csv.DictReader(
                io.StringIO(self.fs.head(result["callbacks"][0]["path"], 10000)),
                escapechar="\\",
            )
        )
        value = json.loads(row["result_json"])
        self.assertFalse(result["has_failed"])
        self.assertEqual(calls, ["retention_verified", "commit_verified"])
        self.assertEqual(value["readiness"], ["succeeded", "source_committed"])
        self.assertEqual([ref["version"] for ref in value["artifact_refs"]], [3, 5])


if __name__ == "__main__":
    unittest.main()
