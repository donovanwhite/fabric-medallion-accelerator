"""Pinned object-level notebook DAG contracts, fencing and failure fan-in."""

from __future__ import annotations

import copy
import json
import unittest
import uuid

import test_run_manifests as manifests
import test_runtime_controls as legacy
from fabric_medallion_deployer.metadata import compile_run_plan, load_configuration
from metadata_fixtures import metadata_document


class NotebookDagTests(manifests.ManifestFixture):
    def setUp(self) -> None:
        super().setUp()
        document = metadata_document()
        document["capabilities"][0].update(verified=True, evidence_sha256="e" * 64)
        bronze, silver = map(copy.deepcopy, document["objects"])
        bronze.update(id="other-bronze", name="other")
        silver.update(id="other-silver", name="other")
        document["objects"].extend([bronze, silver])
        document["dependencies"].append(
            {
                "upstream_id": bronze["id"],
                "downstream_id": silver["id"],
                "condition": "succeeded",
            }
        )
        document["extensions"]["hash_policies"][silver["id"]] = copy.deepcopy(
            document["extensions"]["hash_policies"]["bas-silver-postings"]
        )
        for rule in copy.deepcopy(document["rules"]):
            if (
                rule["object_id"] == "bas-silver-postings"
                and rule["handler"] != "legacy.source_business_checks"
            ):
                rule.update(id="other-" + rule["id"], object_id=silver["id"])
                document["rules"].append(rule)
        self.config = load_configuration(document)
        self.plan = compile_run_plan(
            self.config,
            "run-1",
            "2026-11-06T08:30:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=self.config.definition_sha256,
            require_ready=True,
        )
        path = "Files/orchestration/run-1/dag-manifest.json"
        self.fs.put(path, self.plan.snapshot_json)
        self.manifest = self.runtime.read_run_manifest(
            self.fs,
            path,
            expected_snapshot_sha256=self.plan.snapshot_sha256,
            run_id=self.plan.run_id,
            version_id=self.config.version_id,
            load_configuration=load_configuration,
        )
        self.contexts = {
            "bas-finance-sql": {
                "path": "Files/orchestration/run-1/contexts/source.json",
                "sha256": "c" * 64,
            }
        }
        tasks = [
            task
            for task in self.plan.to_document()["tasks"]
            if task["layer"] == "silver"
        ]
        self.dispatch = self.runtime.make_layer_dispatch(
            self.manifest,
            "silver",
            [
                {
                    "task_id": task["task_id"],
                    "attempt_id": str(uuid.UUID(int=100 + index)),
                    "fence": 1,
                    "source_fence": 1,
                }
                for index, task in enumerate(tasks)
            ],
        )

    def dag(self, **options):
        return self.runtime.build_silver_dag(
            self.manifest,
            self.contexts,
            self.dispatch,
            manifest_path="Files/orchestration/run-1/dag-manifest.json",
            dispatch_path="Files/orchestration/run-1/dispatch.json",
            lakehouse="lh_brz_slv_treasury",
            **options,
        )

    def test_independent_objects_share_no_interpreter_state_and_have_unique_paths(self):
        dag = self.dag(concurrency=2)
        self.assertEqual(dag["concurrency"], 2)
        self.assertEqual(len(dag["activities"]), 2)
        self.assertEqual(len({item["name"] for item in dag["activities"]}), 2)
        for activity in dag["activities"]:
            self.assertEqual(activity["path"], "nb_silver_worker")
            self.assertEqual(activity["retry"], 0)
            self.assertEqual(activity["dependencies"], [])
            self.assertIn("context_sha256", activity["args"])
            self.assertIn("dispatch_sha256", activity["args"])
            self.assertNotIn("dataframes", activity["args"])
            self.assertNotIn("configuration", activity["args"])

    def test_declared_same_layer_dependencies_are_preserved(self):
        document = self.config.to_document()
        document["dependencies"].append(
            {
                "upstream_id": "bas-silver-postings",
                "downstream_id": "other-silver",
                "condition": "succeeded",
            }
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
        path = "Files/orchestration/run-1/dependent.json"
        self.fs.put(path, plan.snapshot_json)
        self.manifest = self.runtime.read_run_manifest(
            self.fs,
            path,
            expected_snapshot_sha256=plan.snapshot_sha256,
            run_id=plan.run_id,
            version_id=config.version_id,
            load_configuration=load_configuration,
        )
        self.dispatch = self.runtime.make_layer_dispatch(
            self.manifest, "silver", self.dispatch.to_document()["attempts"]
        )
        dag = self.dag()
        parent, child = dag["activities"]
        self.assertEqual(child["dependencies"], [parent["name"]])

    def test_missing_context_wrong_binding_and_invalid_limits_fail_before_runs(self):
        self.contexts.clear()
        with self.assertRaisesRegex(RuntimeError, "context"):
            self.dag()
        self.contexts["bas-finance-sql"] = {
            "path": "Files/orchestration/run-1/contexts/source.json",
            "sha256": "c" * 64,
        }
        with self.assertRaisesRegex(RuntimeError, "Lakehouse|binding"):
            self.runtime.build_silver_dag(
                self.manifest,
                self.contexts,
                self.dispatch,
                manifest_path="Files/orchestration/run-1/dag-manifest.json",
                dispatch_path="Files/orchestration/run-1/dispatch.json",
                lakehouse="other",
            )
        for value in (0, True, 17):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                self.dag(concurrency=value)

    def test_dispatch_cannot_omit_rebind_or_duplicate_an_attempt(self):
        attempts = self.dispatch.to_document()["attempts"]
        for changed in (
            [],
            attempts + attempts,
            [{**attempts[0], "fence": 0}, attempts[1]],
            [{**attempts[0], "task_id": "f" * 64}, attempts[1]],
        ):
            with self.subTest(changed=changed), self.assertRaises(RuntimeError):
                self.runtime.make_layer_dispatch(self.manifest, "silver", changed)

    def test_native_runner_validation_and_failures_are_not_success_shaped(self):
        calls = []
        attempts = {
            attempt["task_id"]: attempt
            for attempt in self.dispatch.to_document()["attempts"]
        }

        class Runner:
            def validateDAG(self, value):
                calls.append(("validate", value))
                return True

            def runMultiple(_self, value):
                calls.append(("run", value))
                return {
                    activity["name"]: {
                        "exception": None,
                        "exitVal": json.dumps(
                            {
                                "task_id": activity["args"]["task_id"],
                                "stage_path": self.runtime._silver_stage_path(
                                    self.manifest.run_id,
                                    activity["args"]["task_id"],
                                    attempts[activity["args"]["task_id"]]["attempt_id"],
                                ),
                            }
                        ),
                    }
                    for activity in value["activities"]
                }

        self.runtime.run_notebook_dag(Runner(), self.dag(), self.dispatch)
        self.assertEqual([call[0] for call in calls], ["validate", "run"])

        class Failed(Runner):
            def runMultiple(self, value):
                return {
                    activity["name"]: {"exception": RuntimeError("synthetic failure")}
                    for activity in value["activities"]
                }

        with self.assertRaisesRegex(RuntimeError, "DAG|failed"):
            self.runtime.run_notebook_dag(Failed(), self.dag(), self.dispatch)

    def test_runner_rejects_ambiguous_or_mismatched_worker_acknowledgements(self):
        dag = self.dag()
        attempts = {
            attempt["task_id"]: attempt
            for attempt in self.dispatch.to_document()["attempts"]
        }
        successful = {
            activity["name"]: {
                "exception": None,
                "exitVal": json.dumps(
                    {
                        "task_id": activity["args"]["task_id"],
                        "stage_path": self.runtime._silver_stage_path(
                            self.manifest.run_id,
                            activity["args"]["task_id"],
                            attempts[activity["args"]["task_id"]]["attempt_id"],
                        ),
                    }
                ),
            }
            for activity in dag["activities"]
        }
        name = dag["activities"][0]["name"]
        expected = json.loads(successful[name]["exitVal"])
        cases = (
            {},
            {"exitVal": successful[name]["exitVal"]},
            {"exception": None},
            {"exception": None, "exitVal": ""},
            {"exception": None, "exitVal": "not-json"},
            {
                "exception": None,
                "exitVal": json.dumps({**expected, "task_id": "f" * 64}),
            },
            {
                "exception": None,
                "exitVal": json.dumps(
                    {
                        **expected,
                        "stage_path": self.runtime._silver_stage_path(
                            self.manifest.run_id,
                            expected["task_id"],
                            str(uuid.UUID(int=999)),
                        ),
                    }
                ),
            },
        )
        for value in cases:
            results = copy.deepcopy(successful)
            results[name] = value

            class Runner:
                payload = results

                def validateDAG(self, _dag):
                    return True

                def runMultiple(self, _dag):
                    return self.payload

            with (
                self.subTest(value=value),
                self.assertRaisesRegex(RuntimeError, "DAG|worker|acknowledg"),
            ):
                self.runtime.run_notebook_dag(Runner(), dag, self.dispatch)

    def test_runner_rejects_mismatched_dispatch_before_starting_any_worker(self):
        dag = self.dag()
        altered = []
        missing = copy.deepcopy(dag)
        missing["activities"].pop()
        altered.append(missing)
        for field, value in (
            ("run_id", "other-run"),
            ("task_id", "f" * 64),
            ("dispatch_sha256", "f" * 64),
        ):
            changed = copy.deepcopy(dag)
            changed["activities"][0]["args"][field] = value
            altered.append(changed)
        duplicate = copy.deepcopy(dag)
        duplicate["activities"][1]["args"]["task_id"] = duplicate["activities"][0][
            "args"
        ]["task_id"]
        altered.append(duplicate)

        class Runner:
            def validateDAG(self, _dag):
                raise AssertionError("Mismatched dispatch cannot start validation")

            def runMultiple(self, _dag):
                raise AssertionError("Mismatched dispatch cannot start workers")

        for value in altered:
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(RuntimeError, "DAG|worker"),
            ):
                self.runtime.run_notebook_dag(Runner(), value, self.dispatch)

    def test_worker_replay_and_source_fan_in_use_persisted_outputs(self):
        for task in self.plan.to_document()["tasks"]:
            if task["layer"] == "bronze":
                self.fs.put(task["target_path"], "id,amount\n1,12.3400\n")
        self.contexts = self.runtime.prepare_silver_contexts(
            self.fs, self.manifest, load_configuration=load_configuration
        )
        calls = []

        class Ingestion:
            manifest_processing_context = staticmethod(
                legacy.runtime.manifest_processing_context
            )

            def setup_namespaces(self, spark, context):
                calls.append("setup")

            def inventory(self, spark, fs, context, *, table_names=None):
                calls.append(("inventory", table_names))

            def load_silver(self, spark, fs, context, *, table_names=None):
                calls.append(("load", table_names))
                return [
                    {
                        "status": "SUCCESS",
                        "source_rows": 1,
                        "silver_rows": 1,
                        "quarantine_rows": 0,
                    }
                ]

            def delta_version(self, spark, target):
                return 3

            def validate_source(self, spark, fs, context):
                calls.append("commit")

            def _operation_errors(self):
                return (RuntimeError, ValueError, OSError)

        ingestion = Ingestion()
        self.runtime.initialize_silver_sources(
            None,
            self.fs,
            self.contexts,
            lakehouse="lh_brz_slv_treasury",
            ingestion=ingestion,
        )
        self.assertEqual(calls[0], "setup")
        self.assertEqual(calls[1][0], "inventory")
        stages = []
        for task in self.plan.to_document()["tasks"]:
            if task["layer"] != "silver":
                continue
            options = {
                "task_id": task["task_id"],
                "worker_id": "silver-worker",
                "context_path": self.contexts["bas-finance-sql"]["path"],
                "context_sha256": self.contexts["bas-finance-sql"]["sha256"],
                "lakehouse": "lh_brz_slv_treasury",
                "code_sha256": "b" * 64,
                "ingestion": ingestion,
            }
            path = self.runtime.execute_silver_task(
                None, self.fs, self.manifest, self.dispatch, **options
            )
            self.assertEqual(
                path,
                self.runtime.execute_silver_task(
                    None, self.fs, self.manifest, self.dispatch, **options
                ),
            )
            stages.append(path)
        self.assertEqual(
            len(
                [
                    call
                    for call in calls
                    if isinstance(call, tuple) and call[0] == "load"
                ]
            ),
            2,
        )
        result = self.runtime.finalize_silver_sources(
            None,
            self.fs,
            self.manifest,
            self.dispatch,
            self.contexts,
            lakehouse="lh_brz_slv_treasury",
            ingestion=ingestion,
        )
        self.assertEqual(result["status"], "SOURCE_COMMITTED")
        self.assertEqual(calls.count("commit"), 1)
        broken = json.loads(self.fs.head(stages[0], 10000))
        broken["quarantine_rows"] = 1
        self.fs.put(stages[0], json.dumps(broken), True)
        with self.assertRaisesRegex(RuntimeError, "publication|failed"):
            self.runtime.finalize_silver_sources(
                None,
                self.fs,
                self.manifest,
                self.dispatch,
                self.contexts,
                lakehouse="lh_brz_slv_treasury",
                ingestion=ingestion,
            )
        self.assertEqual(calls.count("commit"), 1)

        broken["source_rows"] = 2
        self.fs.put(stages[0], json.dumps(broken), True)
        result = self.runtime.finalize_silver_sources(
            None,
            self.fs,
            self.manifest,
            self.dispatch,
            self.contexts,
            lakehouse="lh_brz_slv_treasury",
            ingestion=ingestion,
        )
        self.assertEqual(result["status"], "SOURCE_COMMITTED")
        self.assertEqual(calls.count("commit"), 2)

    def test_omitted_source_context_cannot_look_like_complete_publication(self):
        with self.assertRaisesRegex(RuntimeError, "feed membership"):
            self.runtime.finalize_silver_sources(
                None,
                self.fs,
                self.manifest,
                self.dispatch,
                {},
                lakehouse="lh_brz_slv_treasury",
                ingestion=None,
            )


if __name__ == "__main__":
    unittest.main()
