"""Generic Gold packaging, source storage isolation and fenced worker contracts."""

from __future__ import annotations

import base64
import csv
import importlib.util
import io
import json
import sys
import unittest
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
from fabric_medallion_deployer.metadata import canonical_json, metadata_digest
from fabric_medallion_deployer.orchestrator import (
    METADATA_NOTEBOOKS,
    bootstrap_configuration,
    metadata_notebook_definition,
    metadata_notebook_document,
    metadata_worker_sha256,
)
from test_gold_dag import executable_gold_manifest, reconstruct
from test_gold_dag import gold as gold_runtime
from test_run_manifests import ManifestFixture

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "artefacts" / "orchestrator" / "gold_orchestration.py"


class GoldNotebookTests(unittest.TestCase):
    def test_generic_gold_assets_have_no_active_contract_and_compile_cleanly(self):
        for step in ("gold", "gold_worker"):
            document = metadata_notebook_document(ROOT, step)
            nbformat.validate(nbformat.from_dict(document))
            namespace = {}
            for cell in document["cells"]:
                if cell["cell_type"] != "code":
                    continue
                code = "".join(cell["source"])
                self.assertNotIn("GOLD_CONFIG =", code)
                self.assertNotIn("SOURCE_CONFIG =", code)
                self.assertEqual(cell["outputs"], [])
                compile(code, METADATA_NOTEBOOKS[step], "exec")
                if cell["id"] != "execute":
                    exec(compile(code, step, "exec"), namespace)  # noqa: S102
            self.assertEqual(
                namespace["WORKER_CODE_SHA256"],
                metadata_worker_sha256(ROOT, layer="gold"),
            )
            self.assertTrue(callable(namespace["GOLD"].build_gold_object))
            self.assertTrue(callable(namespace["GOLD_CONTROL"].run_gold_parent))

    def test_gold_packaged_hash_matches_bootstrap_and_covers_all_worker_code(self):
        config = bootstrap_configuration(ROOT)
        worker = next(w for w in config.workers if w.id == "gold-worker")
        self.assertEqual(worker.code_sha256, metadata_worker_sha256(ROOT, layer="gold"))
        silver_hash = metadata_worker_sha256(ROOT)
        with patch(
            "fabric_medallion_deployer.orchestrator.metadata_runtime_sources",
            return_value={"gold": "changed implementation"},
        ):
            self.assertNotEqual(
                worker.code_sha256, metadata_worker_sha256(ROOT, layer="gold")
            )
        self.assertNotEqual(worker.code_sha256, silver_hash)

    def test_gold_binding_requires_explicit_source_lakehouse_and_is_not_silver_bound(
        self,
    ):
        workspace, target, source = (str(uuid.UUID(int=i)) for i in (101, 102, 103))
        arguments = {
            "workspace_id": workspace,
            "lakehouse_id": target,
            "lakehouse_name": "lh_gld_treasury",
        }
        with self.assertRaisesRegex(ValueError, "source"):
            metadata_notebook_definition(ROOT, "gold", **arguments)
        definition = metadata_notebook_definition(
            ROOT,
            "gold_worker",
            **arguments,
            source_workspace_id=workspace,
            source_lakehouse_id=source,
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        binding = document["metadata"]["dependencies"]["lakehouse"]
        self.assertEqual(binding["default_lakehouse"], target)
        self.assertIn({"id": source}, binding["known_lakehouses"])
        cell = next(c for c in document["cells"] if c["id"] == "source-bindings")
        namespace = {}
        exec("".join(cell["source"]), namespace)  # noqa: S102
        self.assertEqual(namespace["source_lakehouse_id"], source)
        with self.assertRaisesRegex(ValueError, "Lakehouse"):
            metadata_notebook_definition(
                ROOT,
                "gold",
                **(arguments | {"lakehouse_name": "lh_brz_slv_treasury"}),
                source_workspace_id=workspace,
                source_lakehouse_id=source,
            )

    def test_unbound_gold_execution_fails_before_manifest_access(self):
        for step in ("gold", "gold_worker"):
            namespace = {
                "notebookutils": SimpleNamespace(
                    fs=Mock(),
                    runtime=SimpleNamespace(
                        context={"defaultLakehouseName": "lh_gld_treasury"}
                    ),
                )
            }
            document = metadata_notebook_document(ROOT, step)
            with self.assertRaisesRegex(ValueError, "source"):
                for cell in document["cells"]:
                    if cell["cell_type"] == "code":
                        exec("".join(cell["source"]), namespace)  # noqa: S102
            namespace["notebookutils"].fs.head.assert_not_called()

    def test_gold_notebooks_dispatch_from_clean_namespaces_with_explicit_source_bindings(
        self,
    ):
        class NotebookExit(Exception):
            pass

        for step in ("gold", "gold_worker"):
            returned, calls = [], []

            def exit_value(value, captured=returned):
                captured.append(json.loads(value))
                raise NotebookExit

            utilities = SimpleNamespace(
                fs=Mock(),
                runtime=SimpleNamespace(
                    context={"defaultLakehouseName": "lh_gld_treasury"}
                ),
                notebook=SimpleNamespace(exit=exit_value),
            )
            namespace = {"notebookutils": utilities, "spark": None}
            manifest = SimpleNamespace(
                to_document=lambda: {
                    "configuration": {
                        "feeds": [],
                        "extensions": {"use_case_id": "treasury"},
                    },
                    "tasks": [],
                }
            )
            document = metadata_notebook_document(ROOT, step)
            with self.subTest(step=step), self.assertRaises(NotebookExit):
                for cell in document["cells"]:
                    if cell["cell_type"] != "code":
                        continue
                    exec("".join(cell["source"]), namespace)  # noqa: S102
                    if cell["id"] == "parameters":
                        namespace.update(
                            run_id="run",
                            task_id="task",
                            version_id="version",
                            source_workspace_id=str(uuid.UUID(int=101)),
                            source_lakehouse_id=str(uuid.UUID(int=103)),
                            manifest_path="Files/orchestration/run/manifest.json",
                        )
                    elif cell["id"] == "packaged-runtime":
                        runtime = namespace["ORCHESTRATION"]
                        runtime.read_run_manifest = (
                            lambda *_args, retained=manifest, **_kwargs: retained
                        )
                        runtime.read_pipeline_dispatch = lambda *_args, **_kwargs: (
                            "pipeline"
                        )
                        runtime.read_layer_dispatch = lambda *_args, **_kwargs: (
                            "dispatch"
                        )
                        controller = namespace["GOLD_CONTROL"]

                        def run(*args, captured=calls, **kwargs):
                            captured.append((args, kwargs))
                            return {"status": "FACTS_READY"}

                        def worker(*args, captured=calls, **kwargs):
                            captured.append((args, kwargs))
                            return "Files/orchestration/run/gold/task/attempt.json"

                        controller.run_gold_parent = run
                        controller.execute_gold_task = worker
            self.assertEqual(len(calls), 1)
            self.assertIn("@onelake.dfs.fabric.microsoft.com/", calls[0][0][1].root)
            self.assertEqual(
                returned,
                [{"status": "FACTS_READY"}]
                if step == "gold"
                else [
                    {
                        "task_id": "task",
                        "stage_path": "Files/orchestration/run/gold/task/attempt.json",
                    }
                ],
            )


class GoldControllerTests(ManifestFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        spec = importlib.util.spec_from_file_location("gold_controller_test", MODULE)
        cls.controller = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.controller
        spec.loader.exec_module(cls.controller)

    def setUp(self):
        super().setUp()
        document, self.code = executable_gold_manifest()
        for task in document["tasks"]:
            task["source_lock_key"] = None
            task["source_batch_id"] = None
        self.manifest = self.runtime.VerifiedRunManifest(
            document["run_id"],
            document["version_id"],
            canonical_json(document),
            metadata_digest(document),
        )
        self.config = reconstruct(document, self.code)
        attempts = [
            {
                "task_id": task["task_id"],
                "attempt_id": str(uuid.UUID(int=i)),
                "fence": 2,
                "source_fence": None,
            }
            for i, task in enumerate(document["tasks"], 1)
            if task["layer"] == "gold"
        ]
        self.dispatch = self.runtime.make_layer_dispatch(
            self.manifest, "gold", attempts
        )
        location = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{uuid.UUID(int=401)}"
            f"/datapipelines/pipelineruns/{uuid.UUID(int=402)}/queryactivityruns"
        )
        self.pipeline = self.runtime.make_pipeline_dispatch(
            self.manifest,
            self.dispatch.to_document()
            | {
                "attempts": [
                    attempt | {"status": "SUBMISSION_PENDING", "job_location": location}
                    for attempt in attempts
                ]
            },
        )
        self.preparation = {
            "table": "lh_gld_treasury.gld_treasury.ctl_gold_release",
            "location": "file:/gold/control",
            "version": 1,
            "gold_release_id": self.manifest.run_id,
            "input_sha256": "a" * 64,
        }

    def fake_gold(self):
        return SimpleNamespace(
            gold_config_from_manifest=Mock(return_value=self.config),
            prepare_gold=Mock(return_value=self.preparation),
            build_gold_dag=gold_runtime.build_gold_dag,
            finalize_gold=Mock(return_value={}),
            fail_gold_release=Mock(),
            load_gold_result=Mock(
                side_effect=lambda *_args: {
                    "output": {
                        "table": self.config["_metadata"]["tasks"][_args[3]]["target"],
                        "location": "file:/gold/output",
                        "version": 7,
                        "row_count": 3,
                    }
                }
            ),
            _gold_digest=metadata_digest,
        )

    def persist_preparation(self):
        path = self.controller._preparation_path(self.manifest, self.dispatch)
        self.fs.put(
            path,
            canonical_json(
                {
                    **self.controller._identity(
                        self.manifest, self.dispatch, self.code
                    ),
                    "preparation": self.preparation,
                }
            ),
        )

    def persist_stage(self, task_id):
        attempt = self.runtime._dispatch_attempt(self.dispatch, task_id)
        reference = {
            "table": "lh_gld_treasury.gld_treasury.ctl_gold_task",
            "location": "file:/gold/tasks",
            "version": 2,
            "gold_release_id": self.manifest.run_id,
            "task_id": task_id,
            "attempt_id": attempt["attempt_id"],
            "fence": attempt["fence"],
            "job_id": "native-reference-run",
            "result_sha256": "e" * 64,
        }
        path = self.controller._stage_path(self.manifest, task_id, attempt)
        self.fs.put(
            path,
            canonical_json(
                {
                    **self.controller._stage_identity(
                        self.manifest,
                        self.dispatch,
                        self.code,
                        task_id,
                        self.runtime,
                    ),
                    "status": "SUCCEEDED",
                    "error_code": None,
                    "result": reference,
                }
            ),
        )
        return path, reference

    def test_source_filesystem_qualifies_only_resolved_lakehouse_and_scoped_writes(
        self,
    ):
        fs = Mock()
        workspace, source = str(uuid.UUID(int=301)), str(uuid.UUID(int=302))
        bound = self.controller.source_filesystem(fs, workspace, source)
        root = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{source}"
        bound.head("Files/orchestration/run/manifest.json", 10)
        fs.head.assert_called_once_with(
            root + "/Files/orchestration/run/manifest.json", 10
        )
        bound.exists(root + "/Files/bronze/source/table.csv")
        fs.exists.assert_called_once_with(root + "/Files/bronze/source/table.csv")
        for path in (
            "../Files/x",
            "Files/../x",
            root + "-other/Files/x",
            "https://other/x",
        ):
            with self.subTest(path=path), self.assertRaises(ValueError):
                bound.exists(path)
        with self.assertRaises(ValueError):
            bound.put("Files/bronze/source/table.csv", "changed")
        with self.assertRaises(ValueError):
            self.controller.source_filesystem(fs, "display-name", source)
        journal = self.controller.MlvJournalFileSystem(bound)
        journal.put("Files/orchestration/mlv_lifecycle/state.json", "state", True)
        fs.put.assert_called_once_with(
            root + "/Files/orchestration/mlv_lifecycle/state.json", "state", True
        )
        with self.assertRaises(ValueError):
            journal.put("Files/orchestration/run/snapshot.json", "changed", True)

    def test_worker_rejects_missing_native_identity_before_gold_writes(self):
        gold = Mock()
        with self.assertRaisesRegex(RuntimeError, "reference run"):
            self.controller.execute_gold_task(
                None,
                self.fs,
                self.manifest,
                self.dispatch,
                task_id=next(iter(self.config["_metadata"]["tasks"].values()))[
                    "task_id"
                ],
                worker_id="gold-worker",
                code_sha256=self.code,
                lakehouse="lh_gld_treasury",
                preparation_json="{}",
                runtime_context={},
                control=self.runtime,
                gold=gold,
                ingestion=Mock(),
            )
        gold.build_gold_object.assert_not_called()

    def test_worker_uses_persisted_preparation_dispatch_and_native_job_identity(self):
        self.persist_preparation()
        gold = self.fake_gold()
        name, task = next(
            (name, task)
            for name, task in self.config["_metadata"]["tasks"].items()
            if not task["dependencies"]
        )
        attempt = self.runtime._dispatch_attempt(self.dispatch, task["task_id"])
        reference = {
            "attempt_id": attempt["attempt_id"],
            "fence": attempt["fence"],
            "job_id": "native-job",
        }
        gold.build_gold_object = Mock(return_value=reference)
        arguments = {
            "task_id": task["task_id"],
            "worker_id": "gold-worker",
            "code_sha256": self.code,
            "lakehouse": "lh_gld_treasury",
            "preparation_json": canonical_json(self.preparation),
            "runtime_context": {"isReferenceRun": True, "currentRunId": "native-job"},
            "control": self.runtime,
            "gold": gold,
            "ingestion": SimpleNamespace(_operation_errors=lambda: (RuntimeError,)),
        }
        path = self.controller.execute_gold_task(
            None,
            self.fs,
            self.manifest,
            self.dispatch,
            **arguments,
        )
        call = gold.build_gold_object.call_args
        self.assertEqual(call.args[4], name)
        self.assertEqual(call.kwargs["fence"], attempt["fence"])
        self.assertEqual(call.kwargs["job_id"], "native-job")
        self.assertEqual(json.loads(self.fs.head(path, 100000))["result"], reference)
        with self.assertRaisesRegex(RuntimeError, "preparation"):
            self.controller.execute_gold_task(
                None,
                self.fs,
                self.manifest,
                self.dispatch,
                **(arguments | {"preparation_json": "{}"}),
            )
        gold.build_gold_object.assert_called_once()

    def test_worker_failure_is_persisted_and_does_not_invent_success(self):
        self.persist_preparation()
        gold = self.fake_gold()
        gold.build_gold_object = Mock(side_effect=RuntimeError("private source detail"))
        task = next(
            task
            for task in self.config["_metadata"]["tasks"].values()
            if not task["dependencies"]
        )
        with self.assertRaisesRegex(RuntimeError, "readiness"):
            self.controller.execute_gold_task(
                None,
                self.fs,
                self.manifest,
                self.dispatch,
                task_id=task["task_id"],
                worker_id="gold-worker",
                code_sha256=self.code,
                lakehouse="lh_gld_treasury",
                preparation_json=canonical_json(self.preparation),
                runtime_context={"isReferenceRun": True, "currentRunId": "native-job"},
                control=self.runtime,
                gold=gold,
                ingestion=SimpleNamespace(_operation_errors=lambda: (RuntimeError,)),
            )
        path = self.controller._stage_path(
            self.manifest,
            task["task_id"],
            self.runtime._dispatch_attempt(self.dispatch, task["task_id"]),
        )
        text = self.fs.head(path, 100000)
        self.assertEqual(json.loads(text)["status"], "FAILED")
        self.assertNotIn("private source detail", text)

    def test_parent_validates_dag_and_complete_results_before_finalization(self):
        gold = self.fake_gold()
        events = []
        test = self

        class Runner:
            def validateDAG(self, dag):
                events.append("validated")
                return True

            def runMultiple(self, dag):
                events.append("executed")
                return {
                    activity["name"]: {
                        "exception": None,
                        "exitVal": json.dumps(
                            {
                                "task_id": activity["args"]["task_id"],
                                "stage_path": test.persist_stage(
                                    activity["args"]["task_id"]
                                )[0],
                            }
                        ),
                    }
                    for activity in dag["activities"]
                }

        references = {
            source["source_feed_id"]: {"path": "context.json", "sha256": "b" * 64}
            for source in self.config["sources"]
        }
        ingestion = SimpleNamespace(
            manifest_processing_context=Mock(
                return_value={
                    "pipeline_run_id": "source-batch",
                    "processing_date": date(2026, 11, 3),
                }
            ),
        )
        fs = SimpleNamespace(
            exists=self.fs.exists,
            ls=self.fs.ls,
            head=self.fs.head,
            put=self.fs.put,
            mkdirs=self.fs.mkdirs,
            root="abfss://resolved@onelake.dfs.fabric.microsoft.com/resolved",
        )
        self.fs.put("context.json", "{}")
        with patch.object(
            self.runtime, "prepare_silver_contexts", return_value=references
        ):
            result = self.controller.run_gold_parent(
                None,
                fs,
                self.manifest,
                self.pipeline,
                manifest_path=f"Files/orchestration/{self.manifest.run_id}/manifest.json",
                lakehouse="lh_gld_treasury",
                code_sha256=self.code,
                concurrency=2,
                runner=Runner(),
                control=self.runtime,
                gold=gold,
                ingestion=ingestion,
                load_configuration=lambda _document: None,
            )
        self.assertEqual(events, ["validated", "executed"])
        self.assertEqual(result["status"], "FACTS_READY")
        self.assertEqual(result["objects"], len(self.config["_metadata"]["tasks"]))
        gold.finalize_gold.assert_called_once()
        gold.fail_gold_release.assert_not_called()
        self.assertEqual(
            set(gold.finalize_gold.call_args.args[3]),
            set(self.config["_metadata"]["tasks"]),
        )
        self.assertEqual(
            gold.prepare_gold.call_args.args[6],
            fs.root + "/Files/bronze",
        )
        gold.finalize_gold.reset_mock()
        invalid_runner = SimpleNamespace(
            validateDAG=Mock(return_value=False),
            runMultiple=Mock(),
        )
        with (
            patch.object(
                self.runtime, "prepare_silver_contexts", return_value=references
            ),
            self.assertRaisesRegex(RuntimeError, "DAG validation"),
        ):
            self.controller.run_gold_parent(
                None,
                fs,
                self.manifest,
                self.pipeline,
                manifest_path=f"Files/orchestration/{self.manifest.run_id}/manifest.json",
                lakehouse="lh_gld_treasury",
                code_sha256=self.code,
                concurrency=2,
                runner=invalid_runner,
                control=self.runtime,
                gold=gold,
                ingestion=ingestion,
                load_configuration=lambda _document: None,
            )
        invalid_runner.runMultiple.assert_not_called()
        gold.finalize_gold.assert_not_called()
        gold.fail_gold_release.assert_called_once()

    def test_partial_or_changed_stage_blocks_finalization(self):
        self.persist_preparation()
        gold = self.fake_gold()
        name, task = next(iter(self.config["_metadata"]["tasks"].items()))
        path, _reference = self.persist_stage(task["task_id"])
        for field, value in (
            ("fence", 100),
            ("code_sha256", "0" * 64),
            ("status", "FAILED"),
        ):
            original = json.loads(self.fs.head(path, 100000))
            self.fs.put(path, canonical_json(original | {field: value}), overwrite=True)
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(RuntimeError, "staged result"),
            ):
                self.controller._read_result(
                    None,
                    self.fs,
                    self.manifest,
                    self.dispatch,
                    self.config,
                    self.preparation,
                    name,
                    control=self.runtime,
                    gold=gold,
                )
            self.fs.put(path, canonical_json(original), overwrite=True)
        gold.finalize_gold.assert_not_called()

    def test_reconciliation_seals_readiness_and_requires_exact_receipt(self):
        self.persist_preparation()
        for task in self.config["_metadata"]["tasks"].values():
            self.persist_stage(task["task_id"])
        gold = self.fake_gold()
        evidence = {
            attempt["task_id"]: {
                "job_id": "pipeline-activity",
                "job_location": attempt["job_location"],
                "observed_job_status": "Completed",
                "finished_utc": "2026-11-03T10:00:00Z",
                "job_evidence_sha256": "f" * 64,
            }
            for attempt in self.pipeline.to_document()["attempts"]
        }
        ingestion = SimpleNamespace(
            delta_version=lambda *_args: 8,
            _operation_errors=lambda: (RuntimeError, ValueError),
        )
        with patch.object(
            self.runtime, "pipeline_activity_evidence", return_value=evidence
        ):
            result = self.controller.reconcile_gold_pipeline(
                None,
                self.fs,
                self.manifest,
                self.pipeline,
                client=None,
                observed_utc="2026-11-03T10:00:00Z",
                lakehouse="lh_gld_treasury",
                code_sha256=self.code,
                control=self.runtime,
                gold=gold,
                ingestion=ingestion,
            )
        self.assertFalse(result["has_failed"])
        self.assertTrue(result["callbacks"])
        options = {
            "path": result["gold_reconciliation_path"],
            "expected_sha256": result["gold_reconciliation_sha256"],
            "code_sha256": self.code,
            "control": self.runtime,
            "gold": gold,
        }
        self.assertEqual(
            self.controller.require_gold_success(
                self.fs,
                self.manifest,
                self.pipeline,
                **options,
            )["status"],
            "GOLD_RESULTS_RECONCILED",
        )
        with self.assertRaisesRegex(RuntimeError, "reconciliation"):
            self.controller.require_gold_success(
                self.fs,
                self.manifest,
                self.pipeline,
                **(options | {"expected_sha256": "0" * 64}),
            )
        # Callback payloads, not status labels, carry the exact output evidence.
        callback = self.fs.head(result["callbacks"][0]["path"], 1000000)
        self.assertIn("facts_ready", callback)
        self.assertNotIn("gold_released", callback)
        callback_results = [
            json.loads(row["result_json"])
            for row in csv.DictReader(io.StringIO(callback), escapechar="\\")
        ]
        self.assertTrue(all(result["metrics"] == {} for result in callback_results))

    def test_failed_parent_reconciles_failure_and_withholds_success(self):
        gold = self.fake_gold()
        evidence = {
            attempt["task_id"]: {
                "job_id": "pipeline-activity",
                "job_location": attempt["job_location"],
                "observed_job_status": "Failed",
                "finished_utc": "2026-11-03T10:00:00Z",
                "job_evidence_sha256": "f" * 64,
            }
            for attempt in self.pipeline.to_document()["attempts"]
        }
        with patch.object(
            self.runtime, "pipeline_activity_evidence", return_value=evidence
        ):
            result = self.controller.reconcile_gold_pipeline(
                None,
                self.fs,
                self.manifest,
                self.pipeline,
                client=None,
                observed_utc="2026-11-03T10:00:00Z",
                lakehouse="lh_gld_treasury",
                code_sha256=self.code,
                control=self.runtime,
                gold=gold,
                ingestion=SimpleNamespace(
                    _operation_errors=lambda: (RuntimeError, ValueError)
                ),
            )
        self.assertTrue(result["has_failed"])
        self.assertTrue(result["callbacks"])
        gold.finalize_gold.assert_not_called()
        with self.assertRaisesRegex(RuntimeError, "readiness"):
            self.controller.require_gold_success(
                self.fs,
                self.manifest,
                self.pipeline,
                path=result["gold_reconciliation_path"],
                expected_sha256=result["gold_reconciliation_sha256"],
                code_sha256=self.code,
                control=self.runtime,
                gold=gold,
            )


if __name__ == "__main__":
    unittest.main()
