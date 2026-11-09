"""Manifest-selected Bronze receipts and detached, source-batch runtime contexts."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import test_run_manifests as manifests
from fabric_medallion_deployer.metadata import (
    compile_run_plan,
    load_configuration,
)
from metadata_fixtures import metadata_document

ROOT = Path(__file__).resolve().parents[2]


class ManifestIngestionTests(manifests.ManifestFixture):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        spec = importlib.util.spec_from_file_location(
            "manifest_ingestion_runtime",
            ROOT / "artefacts" / "medallion" / "ingestion.py",
        )
        cls.ingestion = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.ingestion)

    def setUp(self) -> None:
        super().setUp()
        self.manifest = self.read(self.runtime.write_run_manifest(self.fs, self.plan))
        self.bronze = self.plan.to_document()["tasks"][0]
        self.fs.put(self.bronze["target_path"], "id,amount\n1,123.4500\n")

    def receipt(self):
        return self.runtime.seal_bronze_receipt(
            self.fs, self.manifest, "bas-finance-sql"
        )

    def context(self, receipt=None):
        value = self.runtime.source_runtime_config(
            self.manifest,
            receipt or self.receipt(),
            load_configuration=load_configuration,
        )
        serialized = json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        return self.ingestion.manifest_processing_context(
            serialized,
            hashlib.sha256(serialized.encode("ascii")).hexdigest(),
            expected_lakehouse="lh_brz_slv_treasury",
        )

    def file_source(self):
        document = metadata_document()
        policy = {"kind": "marker", "marker": "_SUCCESS"}
        document["connections"][0].update(
            connector_type="Ftp", authentication_type="Basic"
        )
        document["feeds"][0].update(source_type="ftp", geometry=None)
        document["objects"][0].update(
            contract={
                "source_directory": "/exports/finance",
                "file_pattern": "postings_*.csv",
                "recursive": True,
                "file_format": "csv",
                "arrival_policy": policy,
            },
            required_capabilities=[],
        )
        document["capabilities"].extend(
            [
                {
                    "id": name,
                    "verified": True,
                    "evidence_sha256": "e" * 64,
                }
                for name in ("ftp-copy-basic", "ftp-arrival-observer")
            ]
        )
        self.config = load_configuration(document)
        self.plan = compile_run_plan(
            self.config,
            "run-file-policy",
            "2026-11-06T08:30:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=self.config.definition_sha256,
            require_ready=True,
        )
        path = f"Files/orchestration/{self.plan.run_id}/file-manifest.json"
        self.fs.put(path, self.plan.snapshot_json)
        self.manifest = self.read(path)
        task = self.plan.to_document()["tasks"][0]
        files = []
        for name in ("postings_01.csv", "part/postings_02.csv"):
            text = "id,amount\n1,123.4500\n"
            self.fs.put(task["target_path"] + "/" + name, text)
            files.append(
                {
                    "relative_path": name,
                    "size_bytes": len(text.encode("ascii")),
                    "modified_utc": "2026-11-06T08:00:00Z",
                }
            )
        arrival = self.runtime.seal_file_arrival(
            files,
            policy,
            observed_utc="2026-11-06T08:30:00Z",
            present_markers=["_SUCCESS"],
        )
        return task, arrival, files

    def test_file_receipts_bind_arrival_policy_to_pinned_object_metadata(self):
        task, arrival, files = self.file_source()
        receipt = self.runtime.seal_bronze_receipt(
            self.fs,
            self.manifest,
            "bas-finance-sql",
            file_arrivals={task["object_id"]: arrival},
        )
        self.assertEqual(len(receipt.to_document()["tables"][0]["files"]), 2)
        other_policy = self.runtime.seal_file_arrival(
            files,
            {"kind": "marker", "marker": "_OTHER"},
            observed_utc="2026-11-06T08:30:00Z",
            present_markers=["_OTHER"],
        )
        with self.assertRaisesRegex(RuntimeError, "pinned|policy"):
            self.runtime.seal_bronze_receipt(
                self.fs,
                self.manifest,
                "bas-finance-sql",
                file_arrivals={task["object_id"]: other_policy},
            )

    def test_persisted_file_arrivals_drive_detached_silver_contexts(self):
        task, arrival, _ = self.file_source()
        reference = self.runtime.write_file_arrival_manifest(
            self.fs, self.manifest, {task["object_id"]: arrival}
        )
        restored = self.runtime.read_file_arrival_manifest(
            self.fs,
            reference["path"],
            self.manifest,
            expected_sha256=reference["sha256"],
        )
        self.assertEqual(restored[task["object_id"]], arrival)
        contexts = self.runtime.prepare_silver_contexts(
            self.fs,
            self.manifest,
            load_configuration=load_configuration,
            file_arrivals=restored,
        )
        self.assertEqual(set(contexts), {"bas-finance-sql"})
        context = json.loads(self.fs.head(contexts["bas-finance-sql"]["path"], 100000))
        self.assertEqual(
            context["observed_utc"], self.plan.to_document()["observed_utc"]
        )
        self.assertEqual(len(context["raw_source_files"]["postings"]), 2)

    def test_arrival_manifest_rejects_missing_wrong_object_and_nonmatching_files(self):
        task, arrival, files = self.file_source()
        for mapping in ({}, {"another-object": arrival}):
            with self.subTest(mapping=mapping), self.assertRaises(RuntimeError):
                self.runtime.write_file_arrival_manifest(
                    self.fs, self.manifest, mapping
                )
        invalid = copy.deepcopy(files)
        invalid[0]["relative_path"] = "unselected.csv"
        changed = self.runtime.seal_file_arrival(
            invalid,
            {"kind": "marker", "marker": "_SUCCESS"},
            observed_utc="2026-11-06T08:30:00Z",
            present_markers=["_SUCCESS"],
        )
        with self.assertRaisesRegex(RuntimeError, "pattern|selection"):
            self.runtime.write_file_arrival_manifest(
                self.fs, self.manifest, {task["object_id"]: changed}
            )

    def test_arrival_manifest_tampering_and_lost_marker_evidence_fail(self):
        task, arrival, _ = self.file_source()
        reference = self.runtime.write_file_arrival_manifest(
            self.fs, self.manifest, {task["object_id"]: arrival}
        )
        original = json.loads(self.fs.head(reference["path"], 10000))
        for field, value in (
            ("present_markers", []),
            ("allow_empty_files", "true"),
        ):
            document = copy.deepcopy(original)
            document["arrivals"][0]["evidence"][field] = value
            text = self.runtime._canonical(document)
            self.fs.put(reference["path"], text, True)
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                self.runtime.read_file_arrival_manifest(
                    self.fs,
                    reference["path"],
                    self.manifest,
                    expected_sha256=hashlib.sha256(text.encode("ascii")).hexdigest(),
                )
        with self.assertRaisesRegex(RuntimeError, "hash"):
            self.runtime.read_file_arrival_manifest(
                self.fs,
                reference["path"],
                self.manifest,
                expected_sha256=reference["sha256"],
            )

    def test_file_receipts_require_metadata_policy_and_exact_recursive_membership(self):
        task, arrival, files = self.file_source()
        wrong_policy = self.runtime.seal_file_arrival(
            files,
            {"kind": "marker", "marker": "_DONE"},
            observed_utc="2026-11-06T08:30:00Z",
            present_markers=["_DONE"],
        )
        with self.assertRaisesRegex(RuntimeError, "pinned|policy"):
            self.runtime.seal_bronze_receipt(
                self.fs,
                self.manifest,
                "bas-finance-sql",
                file_arrivals={task["object_id"]: wrong_policy},
            )
        self.fs.put(task["target_path"] + "/part/unselected.csv", "unexpected")
        with self.assertRaisesRegex(RuntimeError, "membership"):
            self.runtime.seal_bronze_receipt(
                self.fs,
                self.manifest,
                "bas-finance-sql",
                file_arrivals={task["object_id"]: arrival},
            )

    def test_changed_file_membership_blocks_initialization_before_data_writes(self):
        task, arrival, _files = self.file_source()
        contexts = self.runtime.prepare_silver_contexts(
            self.fs,
            self.manifest,
            load_configuration=load_configuration,
            file_arrivals={task["object_id"]: arrival},
        )
        self.fs.put(task["target_path"] + "/part/unselected.csv", "unexpected")
        ingestion = SimpleNamespace(
            manifest_processing_context=self.ingestion.manifest_processing_context,
            setup_namespaces=lambda *args: self.fail("No data writes are allowed"),
            inventory=lambda *args, **kwargs: self.fail("No inventory may run"),
        )
        with self.assertRaisesRegex(RuntimeError, "membership"):
            self.runtime.initialize_silver_sources(
                None,
                self.fs,
                contexts,
                lakehouse="lh_brz_slv_treasury",
                ingestion=ingestion,
            )

    def test_receipt_is_immutable_complete_and_detached(self):
        receipt = self.receipt()
        path = self.runtime.write_bronze_receipt(self.fs, receipt)
        self.assertEqual(self.runtime.write_bronze_receipt(self.fs, receipt), path)
        restored = self.runtime.read_bronze_receipt(
            self.fs,
            path,
            self.manifest,
            expected_sha256=receipt.receipt_sha256,
            feed_id="bas-finance-sql",
        )
        self.assertEqual(restored, receipt)
        document = restored.to_document()
        self.assertEqual(
            document["tables"][0]["files"][0]["path"], self.bronze["target_path"]
        )
        document["tables"].clear()
        self.assertEqual(len(restored.to_document()["tables"]), 1)

    def test_runtime_reuses_source_batch_timestamp_contract_and_hash_order(self):
        context = self.context()
        self.assertEqual(context["pipeline_run_id"], self.bronze["source_batch_id"])
        self.assertNotEqual(context["pipeline_run_id"], self.plan.run_id)
        self.assertEqual(context["metadata_run_id"], self.plan.run_id)
        self.assertEqual(context["metadata_version"], self.config.version_id)
        self.assertEqual(
            context["batch_started_utc"], datetime(2026, 11, 6, 8, 30, tzinfo=UTC)
        )
        self.assertEqual(context["processing_date"], date(2026, 11, 6))
        self.assertEqual(context["contract_hash"], "a" * 64)
        table = context["tables"][0]
        self.assertEqual(table["hash_policy"]["value_columns"], ["id", "amount"])
        self.assertEqual(
            self.ingestion.source_path(context, table), self.bronze["target_path"]
        )
        self.assertEqual(
            self.ingestion.source_files(context, table), (self.bronze["target_path"],)
        )

    def test_missing_tampered_or_changed_membership_cannot_process(self):
        receipt = self.receipt()
        path = self.runtime.write_bronze_receipt(self.fs, receipt)
        self.fs.put(path, receipt.receipt_json.replace('"run-1"', '"other"'), True)
        with self.assertRaisesRegex(RuntimeError, "hash|identity"):
            self.runtime.read_bronze_receipt(
                self.fs,
                path,
                self.manifest,
                expected_sha256=receipt.receipt_sha256,
                feed_id="bas-finance-sql",
            )
        (self.fs.root / self.bronze["target_path"]).unlink()
        with self.assertRaises(FileNotFoundError):
            self.receipt()

    def test_forged_runtime_paths_and_contract_hashes_are_rejected(self):
        value = self.runtime.source_runtime_config(
            self.manifest, self.receipt(), load_configuration=load_configuration
        )
        for changes in (
            {"bronze_root": "Files/bronze/other"},
            {"pipeline_run_id": "attempt-not-source-batch"},
            {"source_code": "../bas"},
            {"raw_source_files": {"postings": ["https://example.invalid/data.csv"]}},
            {"processing_date": "2026-11-05"},
            {"source_selection_complete": "true"},
        ):
            modified = copy.deepcopy(value)
            modified.update(changes)
            serialized = json.dumps(
                modified, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            )
            with (
                self.subTest(changes=changes),
                self.assertRaises((ValueError, RuntimeError)),
            ):
                self.ingestion.manifest_processing_context(
                    serialized,
                    hashlib.sha256(serialized.encode("ascii")).hexdigest(),
                    expected_lakehouse="lh_brz_slv_treasury",
                )

    def test_partial_source_selection_cannot_publish_source_commit(self):
        document = metadata_document()
        document["capabilities"][0].update(verified=True, evidence_sha256="e" * 64)
        second_bronze = copy.deepcopy(document["objects"][0])
        second_silver = copy.deepcopy(document["objects"][1])
        second_bronze.update(id="bas-bronze-other", name="other")
        second_silver.update(id="bas-silver-other", name="other")
        document["objects"].extend([second_bronze, second_silver])
        document["dependencies"].append(
            {
                "upstream_id": second_bronze["id"],
                "downstream_id": second_silver["id"],
                "condition": "succeeded",
            }
        )
        document["extensions"]["hash_policies"][second_silver["id"]] = copy.deepcopy(
            document["extensions"]["hash_policies"]["bas-silver-postings"]
        )
        config = load_configuration(document)
        plan = compile_run_plan(
            config,
            "run-1",
            "2026-11-06T08:30:00Z",
            object_ids=["bas-silver-postings"],
            lifecycle_state="APPROVED",
            approved_definition_sha256=config.definition_sha256,
            require_ready=True,
        )
        path = "Files/orchestration/run-1/partial.json"
        self.fs.put(path, plan.snapshot_json)
        manifest = self.runtime.read_run_manifest(
            self.fs,
            path,
            run_id=plan.run_id,
            version_id=config.version_id,
            expected_snapshot_sha256=plan.snapshot_sha256,
            load_configuration=load_configuration,
        )
        receipt = self.runtime.seal_bronze_receipt(self.fs, manifest, "bas-finance-sql")
        value = self.runtime.source_runtime_config(
            manifest, receipt, load_configuration=load_configuration
        )
        self.assertFalse(value["source_selection_complete"])
        self.assertEqual(value["required_source_tables"], ["other", "postings"])
        serialized = json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        context = self.ingestion.manifest_processing_context(
            serialized,
            hashlib.sha256(serialized.encode("ascii")).hexdigest(),
            expected_lakehouse="lh_brz_slv_treasury",
        )
        with self.assertRaisesRegex(RuntimeError, "complete|selection"):
            self.ingestion.validate_source(None, self.fs, context)


if __name__ == "__main__":
    unittest.main()
