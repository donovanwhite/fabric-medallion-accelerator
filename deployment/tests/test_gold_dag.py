# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Offline pinned Gold metadata, dependency and specialist dispatch contracts."""

from __future__ import annotations

import hashlib
import json
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from fabric_medallion_deployer.metadata import canonical_json, metadata_digest
from fabric_medallion_deployer.orchestrator import bootstrap_configuration
from test_gold_runtime import ROOT, gold, silver


def executable_gold_manifest():
    """A local test snapshot, not evidence of activation or Fabric approval."""
    configuration = bootstrap_configuration(
        ROOT, "local-gold-version", use_case="treasury"
    ).to_document()
    selected = [
        obj
        for obj in configuration["objects"]
        if obj["layer"] == "silver"
        or (obj["layer"] == "gold" and obj["materialization"] != "mlv")
    ]
    ids = {
        obj["id"]: hashlib.sha256(obj["id"].encode()).hexdigest() for obj in selected
    }
    for obj in selected:
        obj["enabled"] = True
    tasks = []
    for obj in selected:
        tasks.append(
            {
                "task_id": ids[obj["id"]],
                "object_id": obj["id"],
                "layer": obj["layer"],
                "worker_id": obj["worker_id"],
                "feed_id": obj["feed_id"],
                "target_path": ".".join(
                    [obj["lakehouse"], obj["target_schema"], obj["name"]]
                ),
                "target_lock_key": metadata_digest(obj["id"]),
                "dependencies": [
                    {
                        "task_id": ids[edge["upstream_id"]],
                        "condition": edge["condition"],
                    }
                    for edge in configuration["dependencies"]
                    if edge["downstream_id"] == obj["id"] and edge["upstream_id"] in ids
                ],
            }
        )
    manifest = {
        "format_version": 1,
        "run_id": "local-gold-run",
        "version_id": configuration["version_id"],
        "configuration": configuration,
        "definition_sha256": metadata_digest(configuration),
        "tasks": tasks,
        "can_execute": True,
        "blockers": [],
    }
    code = next(w for w in configuration["workers"] if w["id"] == "gold-worker")[
        "code_sha256"
    ]
    return manifest, code


def reconstruct(manifest, code):
    return gold.gold_config_from_manifest(
        manifest,
        snapshot_sha256=metadata_digest(manifest),
        code_sha256=code,
    )


def manifest_source_context(manifest, config, code="bas"):
    source = next(s for s in config["sources"] if s["source_code"] == code)
    feed = source["source_feed_id"]
    batch = metadata_digest({"feed_id": feed, "run_id": manifest["run_id"]})
    prefix = f"Files/bronze/{code}_finance_sql/20261103/{batch}"
    tables = deepcopy(source["tables"])
    return {
        "source_code": code,
        "lakehouse": source["lakehouse"],
        "silver_schema": source["silver_schema"],
        "quarantine_schema": source["quarantine_schema"],
        "bronze_root": f"Files/bronze/{code}_finance_sql",
        "contract_hash": source["contract_hash"],
        "seed_snapshot_date": "2026-11-03",
        "tables": tables,
        "metadata_version": manifest["version_id"],
        "metadata_run_id": manifest["run_id"],
        "source_feed_id": feed,
        "source_batch_id": batch,
        "source_type": "sql",
        "description_slug": "finance",
        "observed_utc": "2026-11-03T08:00:00Z",
        "processing_date": "2026-11-03",
        "pipeline_run_id": batch,
        "raw_source_files": {t["name"]: [f"{prefix}/{t['name']}.csv"] for t in tables},
        "raw_source_sizes": {t["name"]: [128] for t in tables},
        "raw_targets": {t["name"]: f"{prefix}/{t['name']}.csv" for t in tables},
        "source_selection_complete": True,
        "required_source_tables": sorted(t["name"] for t in tables),
        "selected_tables": sorted(t["name"] for t in tables),
        "bronze_receipt_sha256": "c" * 64,
        "source_rule_specs": [
            {k: v for k, v in rule.items() if k != "object_id"}
            for rule in manifest["configuration"]["rules"]
            if rule["object_id"].startswith(code + ".silver.")
            and rule["handler"] == "legacy.source_business_checks"
        ],
    }


class GoldManifestAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest, code = executable_gold_manifest()
        cls.config = reconstruct(cls.manifest, code)
        cls.source = cls.config["sources"][0]
        cls.context = manifest_source_context(cls.manifest, cls.config)
        cls.selection = {
            "pipeline_run_id": cls.context["source_batch_id"],
            "processing_date": "2026-11-03",
        }

    def adapt(self, context=None):
        payload = context or self.context
        return gold.manifest_gold_context(
            self.config,
            self.source,
            self.selection,
            canonical_json(payload),
            metadata_digest(payload),
            silver,
        )

    def test_preserves_batch_time_paths_membership_and_hash_order(self) -> None:
        context = self.adapt()
        self.assertEqual(context["bronze_root"], "Files/bronze/bas_finance_sql")
        self.assertEqual(context["pipeline_run_id"], self.context["source_batch_id"])
        self.assertEqual(
            context["batch_started_utc"].isoformat(), "2026-11-03T08:00:00+00:00"
        )
        self.assertEqual(context["raw_source_files"], self.context["raw_source_files"])
        self.assertEqual(
            silver.hash_column_order(context["tables"][0]),
            silver.hash_column_order(self.source["tables"][0]),
        )

    def test_partial_relabelled_and_changed_rule_contexts_are_blocked(self) -> None:
        for change in ("partial", "batch", "version", "rule"):
            context = deepcopy(self.context)
            if change == "partial":
                context["selected_tables"] = context["selected_tables"][:-1]
                context["source_selection_complete"] = False
            elif change == "batch":
                context["pipeline_run_id"] = self.manifest["run_id"]
            elif change == "version":
                context["metadata_version"] = "other-version"
            else:
                context["tables"][0]["rule_specs"] = []
            with (
                self.subTest(change=change),
                self.assertRaises((ValueError, RuntimeError)),
            ):
                self.adapt(context)

    def test_cross_lakehouse_qualification_does_not_mutate_the_contract(self) -> None:
        from types import SimpleNamespace

        original = self.adapt()
        calls = []
        fs = SimpleNamespace(
            exists=lambda path: True,
            head=lambda path, limit: "",
            ls=lambda path: calls.append(path) or [],
        )
        base = (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/silver-id/Files/bronze"
        )
        reader, bound = gold._gold_bronze_reader(fs, original, base)
        path = next(iter(bound["raw_source_files"].values()))[0]
        from pathlib import PurePosixPath

        reader.ls(str(PurePosixPath(path).parent))
        self.assertEqual(calls, [path.rsplit("/", 1)[0]])
        self.assertTrue(path.startswith(base + "/bas_finance_sql/"))
        self.assertEqual(original["raw_source_files"], self.context["raw_source_files"])


class PinnedGoldMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest, cls.code = executable_gold_manifest()

    def test_reconstruction_uses_no_seed_or_repository_reads(self) -> None:
        with (
            patch.object(Path, "open", side_effect=AssertionError("seed read")),
            patch.object(Path, "read_text", side_effect=AssertionError("seed read")),
        ):
            config = reconstruct(self.manifest, self.code)
        baseline = json.loads(
            (ROOT / "deployment/tests/fixtures/medallion_baseline.json").read_text()
        )["gold"]
        for field in (
            "execution_mode",
            "schema",
            "target_lakehouse",
            "dimensions",
            "facts",
            "blocked",
        ):
            self.assertEqual(config[field], baseline[field], field)
        extensions = self.manifest["configuration"]["extensions"]
        self.assertEqual(
            config["contract_hash"], extensions["legacy_gold_contract_hash"]
        )
        self.assertEqual(
            config["department_mapping"],
            extensions["gold_contract"]["department_mapping"],
        )
        self.assertEqual(
            config["supplier_crosswalk"],
            extensions["gold_contract"]["supplier_crosswalk"],
        )
        self.assertEqual(config["runtime_sha256"], self.code)
        self.assertTrue(all(source["synthetic_only"] for source in config["sources"]))
        self.assertEqual(
            next(d for d in config["dimensions"] if d["name"] == "dim_supplier")[
                "keys"
            ],
            ["SupplierNumber"],
        )

    def test_rejects_preview_tampering_code_drift_and_partial_selection(self) -> None:
        for change in ("preview", "code", "hash", "disabled", "partial", "definition"):
            manifest = deepcopy(self.manifest)
            code = self.code
            if change == "preview":
                manifest["can_execute"] = False
            elif change == "code":
                code = "0" * 64
            elif change == "hash":
                manifest["definition_sha256"] = "0" * 64
            elif change == "disabled":
                next(
                    o
                    for o in manifest["configuration"]["objects"]
                    if o["id"] == "gold.dim_supplier"
                )["enabled"] = False
                manifest["definition_sha256"] = metadata_digest(
                    manifest["configuration"]
                )
            elif change == "partial":
                manifest["tasks"] = [
                    t
                    for t in manifest["tasks"]
                    if t["object_id"] != "gold.fact_disbursement"
                ]
            else:
                manifest["configuration"]["transformations"][-12]["definition"][
                    "changed"
                ] = True
                manifest["definition_sha256"] = metadata_digest(
                    manifest["configuration"]
                )
            with (
                self.subTest(change=change),
                self.assertRaises((ValueError, RuntimeError)),
            ):
                reconstruct(manifest, code)

    def test_metadata_and_code_identity_are_not_business_hash_inputs(self) -> None:
        first = reconstruct(self.manifest, self.code)
        document = deepcopy(self.manifest)
        document["version_id"] = document["configuration"]["version_id"] = (
            "next-local-version"
        )
        next_code = "1" * 64
        next(
            w for w in document["configuration"]["workers"] if w["id"] == "gold-worker"
        )["code_sha256"] = next_code
        document["definition_sha256"] = metadata_digest(document["configuration"])
        second = reconstruct(document, next_code)
        self.assertNotEqual(first["_metadata"], second["_metadata"])
        self.assertEqual(first["contract_hash"], second["contract_hash"])
        self.assertEqual(first["facts"], second["facts"])
        self.assertEqual(first["dimensions"], second["dimensions"])

    def test_source_instances_edges_layouts_and_unregistered_rules_fail_closed(
        self,
    ) -> None:
        for change in (
            "instance",
            "schema",
            "dependency",
            "layout",
            "rule",
            "algorithm",
        ):
            manifest = deepcopy(self.manifest)
            configuration = manifest["configuration"]
            if change == "instance":
                configuration["feeds"][0]["source_instance"] = "unapproved_new_instance"
            elif change == "schema":
                next(
                    obj for obj in configuration["objects"] if obj["layer"] == "silver"
                )["target_schema"] = "unapproved_schema"
            elif change == "dependency":
                dimension = next(
                    task
                    for task in manifest["tasks"]
                    if task["object_id"] == "gold.dim_supplier"
                )
                dimension["dependencies"] = [
                    {
                        "task_id": next(
                            task
                            for task in manifest["tasks"]
                            if task["object_id"] == "bas.silver.atbs03bs"
                        )["task_id"],
                        "condition": "source_committed",
                    }
                ]
            elif change == "layout":
                next(
                    obj
                    for obj in configuration["objects"]
                    if obj["id"] == "gold.dim_supplier"
                )["layout"] = {
                    "layout_strategy": "partitioned",
                    "partition_columns": ["durable_key"],
                    "cluster_by_columns": [],
                }
            elif change == "rule":
                configuration["rules"].append(
                    {
                        "id": "local-unregistered-rule",
                        "object_id": "gold.dim_supplier",
                        "handler": "unregistered.gold_rule",
                        "version": "1",
                        "severity": "warning",
                        "action": "warn",
                        "parameters": {},
                    }
                )
            else:
                transform = next(
                    t
                    for t in configuration["transformations"]
                    if t["object_id"] == "gold.dim_supplier"
                )
                transform["definition"]["pattern"] = "guessed_business_algorithm"
                transform["definition_sha256"] = metadata_digest(
                    transform["definition"]
                )
            manifest["definition_sha256"] = metadata_digest(configuration)
            with (
                self.subTest(change=change),
                self.assertRaises((ValueError, RuntimeError)),
            ):
                reconstruct(manifest, self.code)

    def test_metadata_only_source_hash_fallback_matches_source_runtime_contract(
        self,
    ) -> None:
        manifest = deepcopy(self.manifest)
        manifest["configuration"]["feeds"][0]["legacy_contract_hash"] = None
        manifest["definition_sha256"] = metadata_digest(manifest["configuration"])
        config = reconstruct(manifest, self.code)
        source = config["sources"][0]
        self.assertEqual(
            source["contract_hash"],
            metadata_digest(
                {
                    "source_code": source["source_code"],
                    "silver_schema": source["silver_schema"],
                    "tables": source["tables"],
                }
            ),
        )
        self.assertEqual(
            config["contract_hash"],
            self.manifest["configuration"]["extensions"]["legacy_gold_contract_hash"],
        )


class GoldDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        manifest, code = executable_gold_manifest()
        cls.config = reconstruct(manifest, code)
        cls.reference = {
            "table": "lh_gld_treasury.gld_treasury.ctl_gold_release",
            "location": "file:/local-test/gold-release",
            "version": 0,
            "gold_release_id": "local-release",
            "input_sha256": "a" * 64,
        }

    def build(self, config=None, **kwargs):
        return gold.build_gold_dag(
            config or self.config,
            self.reference,
            manifest_path="Files/orchestration/local-gold-run/manifest.json",
            dispatch_path="Files/orchestration/local-gold-run/gold-dispatch.json",
            dispatch_sha256="b" * 64,
            lakehouse="lh_gld_treasury",
            **kwargs,
        )

    def test_independent_dimensions_fan_out_facts_wait_and_no_shared_frames(
        self,
    ) -> None:
        dag = self.build(concurrency=2)
        tasks = self.config["_metadata"]["tasks"]
        by_id = {
            activity["args"]["task_id"]: activity for activity in dag["activities"]
        }
        dimensions = [
            by_id[tasks[d["name"]]["task_id"]] for d in self.config["dimensions"]
        ]
        self.assertTrue(all(activity["dependencies"] == [] for activity in dimensions))
        dimension_names = {activity["name"] for activity in dimensions}
        for fact in self.config["facts"]:
            activity = by_id[tasks[fact["name"]]["task_id"]]
            self.assertTrue(dimension_names <= set(activity["dependencies"]))
            self.assertEqual(activity["path"], "nb_gold_worker")
            self.assertNotIn("config", activity["args"])
            self.assertNotIn("dimensions", activity["args"])
            self.assertEqual(
                json.loads(activity["args"]["gold_preparation_json"]), self.reference
            )
        self.assertEqual(dag["concurrency"], 2)
        self.assertTrue(all(activity["retry"] == 0 for activity in dag["activities"]))
        self.assertEqual(
            len({a["name"] for a in dag["activities"]}), len(dag["activities"])
        )

    def test_cycles_missing_dependencies_target_conflicts_and_bounds_fail(self) -> None:
        for change in (
            "cycle",
            "missing",
            "target",
            "binding",
            "concurrency",
            "timeout",
        ):
            config = deepcopy(self.config)
            kwargs = {}
            tasks = config["_metadata"]["tasks"]
            first = next(iter(tasks))
            if change == "cycle":
                tasks[first]["dependencies"].append(first)
            elif change == "missing":
                tasks[first]["dependencies"].append("absent")
            elif change == "target":
                values = list(tasks.values())
                values[1]["target"] = values[0]["target"]
            elif change == "binding":
                config["target_lakehouse"] = "another_lakehouse"
            elif change == "concurrency":
                kwargs["concurrency"] = True
            else:
                kwargs["timeout_seconds"] = 0
            with (
                self.subTest(change=change),
                self.assertRaises((ValueError, RuntimeError)),
            ):
                self.build(config, **kwargs)


class GoldSpecialistDispatchTests(unittest.TestCase):
    def test_specialists_retain_explicit_secondary_source_dependencies(self) -> None:
        manifest, code = executable_gold_manifest()
        configuration = manifest["configuration"]
        transform = next(
            value
            for value in configuration["transformations"]
            if value["object_id"] == "gold.fact_gl_posting"
        )
        transform["kind"] = "sql"
        transform["definition"] = {
            "dialect": "spark_sql",
            "query": "SELECT approved_projection FROM pinned_inputs",
            "implementation_sha256": "a" * 64,
            "validation_sha256": "b" * 64,
            "approval_id": "local-test-reference",
            "inputs": [
                "lh_brz_slv_treasury.slv_bas.atbs03bs",
                "lh_brz_slv_treasury.slv_bas.atbs07bs",
            ],
            "output_columns": [{"name": "fact_sk", "data_type": "bigint"}],
        }
        transform["definition_sha256"] = metadata_digest(transform["definition"])
        task = next(
            t for t in manifest["tasks"] if t["object_id"] == "gold.fact_gl_posting"
        )
        task["dependencies"].append(
            {
                "task_id": next(
                    t
                    for t in manifest["tasks"]
                    if t["object_id"] == "bas.silver.atbs07bs"
                )["task_id"],
                "condition": "source_committed",
            }
        )
        manifest["definition_sha256"] = metadata_digest(configuration)
        config = reconstruct(manifest, code)
        self.assertEqual(
            config["_metadata"]["tasks"]["fact_gl_posting"]["source_inputs"],
            [
                {"source": "bas", "table": "atbs03bs"},
                {"source": "bas", "table": "atbs07bs"},
            ],
        )

    def test_normal_result_fan_in_requires_exact_fenced_dispatch_identity(self) -> None:
        body = {"fence": 4, "job_id": "local-job"}
        config = {
            "_metadata": {},
            "target_lakehouse": "spark_catalog",
            "schema": "gld_local",
        }
        preparation = {"gold_release_id": "local-release"}
        inputs = {"_tasks": {"fact_sample": {"task_id": "task-1"}}}
        pinned = {
            "table": "spark_catalog.gld_local.ctl_gold_task",
            "version": 0,
            "location": "file:/local",
        }
        rows = [{"status": "SUCCEEDED", "result_json": canonical_json(body)}]
        with (
            patch.object(gold, "_prepared_for_task", return_value=inputs),
            patch.object(gold, "_gold_table_reference", return_value=pinned),
            patch.object(gold, "_task_result_rows", return_value=rows),
        ):
            references = gold.gold_result_references(
                None,
                config,
                preparation,
                {
                    "fact_sample": {
                        "attempt_id": "attempt-1",
                        "fence": 4,
                        "job_id": "local-job",
                    },
                },
            )
            self.assertEqual(references["fact_sample"]["fence"], 4)
            for attempt in (
                "attempt-1",
                {"attempt_id": "attempt-1", "fence": 5, "job_id": "local-job"},
                {"attempt_id": "attempt-1", "fence": 4, "job_id": "different-job"},
            ):
                with self.subTest(attempt=attempt), self.assertRaises(RuntimeError):
                    gold.gold_result_references(
                        None, config, preparation, {"fact_sample": attempt}
                    )

    def test_undeclared_specialist_output_persists_failure_and_withholds_writes(
        self,
    ) -> None:
        from types import SimpleNamespace

        table = "spark_catalog.slv_bas.sample"
        definition = {
            "implementation_sha256": "a" * 64,
            "approval_id": "local-test-reference",
            "dialect": "spark_sql",
            "query": "SELECT id FROM pinned_sample",
            "validation_sha256": "b" * 64,
            "inputs": [table],
            "output_columns": [{"name": "id", "data_type": "bigint"}],
        }
        transform = {
            "id": "local-specialist",
            "kind": "sql",
            "version": "1",
            "definition": definition,
            "definition_sha256": metadata_digest(definition),
        }
        task = {
            "task_id": "local-task",
            "name": "fact_specialist",
            "kind": "atomic_fact",
            "target": "spark_catalog.gld_local.fact_specialist",
            "dependencies": [],
            "spec": {"source": "bas", "table": "sample", "snapshot": False},
            "transformation": transform,
        }
        inputs = {
            "_release_id": "local-release",
            "_input_sha256": "d" * 64,
            "_tasks": {"fact_specialist": task},
            "_execution": {"code_sha256": "a" * 64},
            "_selection": {"runs": {}, "as_of_date": "2026-11-03"},
            "_contexts": {},
            "_bronze_base": "local-base",
            table: {"location": "file:/local", "version": 0},
        }
        registry = {
            "local-specialist": {
                "kind": "sql",
                "version": "1",
                "definition_sha256": transform["definition_sha256"],
                "implementation_sha256": "a" * 64,
                "approval_id": definition["approval_id"],
                "execute": lambda context: SimpleNamespace(schema=[]),
            }
        }
        with (
            patch.object(gold, "_prepared_for_task", return_value=inputs),
            patch.object(gold, "_task_result_rows", return_value=[]),
            patch.object(gold, "_ready_release", return_value=None),
            patch.object(gold, "_gold_contexts", return_value=(None, {})),
            patch.object(gold, "_gold_inputs", return_value=({}, {}, {})),
            patch.object(gold, "_write_gold_result") as audit,
            patch.object(gold, "_release_event") as release_event,
            patch.object(gold, "_merge_gold") as merge,
            self.assertRaisesRegex(RuntimeError, "undeclared output"),
        ):
            gold.build_gold_object(
                None,
                None,
                {
                    "sources": [
                        {
                            "source_code": "bas",
                            "lakehouse": "spark_catalog",
                            "silver_schema": "slv_bas",
                        }
                    ]
                },
                {"gold_release_id": "local-release", "input_sha256": "d" * 64},
                "fact_specialist",
                None,
                attempt_id="local-attempt",
                fence=1,
                job_id="local-job",
                code_sha256="a" * 64,
                implementations=registry,
            )
        merge.assert_not_called()
        self.assertEqual(audit.call_args.args[2]["status"], "FAILED")
        self.assertEqual(release_event.call_args.args[3], "FAILED")

    def test_metadata_runtime_cannot_fall_back_to_a_monolithic_notebook(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Normal Gold"):
            gold.run_gold(None, None, {"_metadata": {}}, "", "", None, "")

    def test_registered_sql_and_notebook_adapters_are_explicit_and_version_pinned(
        self,
    ) -> None:
        from types import SimpleNamespace

        frame = SimpleNamespace(
            schema=[
                SimpleNamespace(
                    name="id", dataType=SimpleNamespace(simpleString=lambda: "bigint")
                )
            ]
        )
        context = {"input_references": {"pinned_source": {"version": 3}}}
        for kind in ("sql", "notebook"):
            definition = {
                "implementation_sha256": "a" * 64,
                "approval_id": "local-test-approval-reference",
                "inputs": ["pinned_source"],
                "output_columns": [{"name": "id", "data_type": "bigint"}],
                **(
                    {
                        "dialect": "spark_sql",
                        "query": "SELECT id FROM pinned_source",
                        "validation_sha256": "b" * 64,
                    }
                    if kind == "sql"
                    else {"notebook_name": "nb_local_test_specialist"}
                ),
            }
            transform = {
                "id": "local-transform",
                "kind": kind,
                "version": "1",
                "definition": definition,
                "definition_sha256": metadata_digest(definition),
            }
            registry = {
                "local-transform": {
                    "kind": kind,
                    "version": "1",
                    "definition_sha256": transform["definition_sha256"],
                    "implementation_sha256": "a" * 64,
                    "approval_id": definition["approval_id"],
                    "execute": lambda inputs: frame,
                }
            }
            self.assertIs(
                gold.dispatch_gold_transformation(transform, registry, context), frame
            )
            for change in (
                "query",
                "code",
                "approval",
                "version",
                "unregistered",
                "output",
                "input",
            ):
                candidate, implementations, arguments = (
                    deepcopy(transform),
                    dict(registry),
                    deepcopy(context),
                )
                if change == "query":
                    candidate["definition"]["query"] = (
                        "SELECT changed FROM pinned_source"
                    )
                elif change == "code":
                    implementations["local-transform"] = registry["local-transform"] | {
                        "implementation_sha256": "c" * 64
                    }
                elif change == "approval":
                    implementations["local-transform"] = registry["local-transform"] | {
                        "approval_id": "different"
                    }
                elif change == "version":
                    candidate["version"] = "2"
                elif change == "unregistered":
                    implementations = {}
                elif change == "input":
                    arguments["input_references"]["undeclared_source"] = {"version": 4}
                else:
                    implementations["local-transform"] = registry["local-transform"] | {
                        "execute": lambda inputs: SimpleNamespace(schema=[])
                    }
                with (
                    self.subTest(kind=kind, change=change),
                    self.assertRaises((ValueError, RuntimeError)),
                ):
                    gold.dispatch_gold_transformation(
                        candidate, implementations, arguments
                    )


if __name__ == "__main__":
    unittest.main()
