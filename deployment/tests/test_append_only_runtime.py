"""Opt-in real Delta tests for one append-only Silver table per source table."""

from __future__ import annotations

import csv
import importlib.util
import os
import tempfile
import unittest
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "treasury_append_only_runtime",
    ROOT / "artefacts" / "medallion" / "ingestion.py",
)


@unittest.skipUnless(os.environ.get("TREASURY_DELTA_TESTS") == "1", "Opt-in Delta tier")
class AppendOnlyRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        cls.warehouse = tempfile.TemporaryDirectory()
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("treasury-append-only-silver")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", Path(cls.warehouse.name).as_uri())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.databricks.delta.snapshotPartitions", "2")
        )
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.runtime = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(cls.runtime)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.warehouse.cleanup()

    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.suffix = uuid.uuid4().hex[:12]
        self.table = {
            "name": f"sample_{self.suffix}",
            "keys": ["id"],
            "foreign_keys": [],
            "allow_empty": True,
            "change_hash_exclusions": ["extract_date"],
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {
                    "name": "amount",
                    "data_type": "decimal",
                    "precision": 18,
                    "scale": 2,
                    "nullable": False,
                },
                {"name": "extract_date", "data_type": "datetime", "nullable": False},
            ],
        }
        self.context = {
            "source_code": "bas",
            "lakehouse": "spark_catalog",
            "silver_schema": f"slv_{self.suffix}",
            "quarantine_schema": f"dq_{self.suffix}",
            "bronze_root": Path(self.folder.name).as_posix(),
            "processing_date": date(2026, 11, 3),
            "pipeline_run_id": "batch-1",
            "batch_started_utc": datetime(2026, 11, 3, 8, tzinfo=UTC),
            "contract_hash": "append-only-test",
            "tables": [self.table],
        }
        self.fs = SimpleNamespace(
            exists=lambda p: Path(p).exists(),
            head=lambda p, n: Path(p).read_text(encoding="utf-8")[:n],
            rm=lambda *args: self.fail("Bronze must not be deleted"),
        )
        self.runtime.setup_namespaces(self.spark, self.context)
        self.addCleanup(self._drop_tables)

    def _drop_tables(self) -> None:
        for key in ("silver_schema", "quarantine_schema"):
            self.spark.sql(f"DROP SCHEMA IF EXISTS {self.context[key]} CASCADE")

    def _land(
        self, amount: str, *, batch: str, hour: int, extract_hour: int = 0
    ) -> None:
        self.context.update(
            pipeline_run_id=batch,
            batch_started_utc=datetime(2026, 11, 3, hour, tzinfo=UTC),
        )
        path = Path(self.runtime.source_path(self.context, self.table))
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, escapechar="\\")
            writer.writerow([c["name"] for c in self.table["columns"]])
            writer.writerow((1, amount, f"2026-11-03T{extract_hour:02}:00:00.000"))

    def _land_quality_fixture(self, rows) -> None:
        self._land("10.00", batch="quality-" + self.suffix, hour=8)
        path = Path(self.runtime.source_path(self.context, self.table))
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, escapechar="\\")
            writer.writerow([column["name"] for column in self.table["columns"]])
            writer.writerows(
                (identity, amount, "2026-11-03T00:00:00.000")
                for identity, amount in rows
            )

    def _configure_quality_rules(self) -> None:
        self.table["rule_specs"] = [
            {
                "id": "baseline-" + handler,
                "handler": "legacy." + handler,
                "version": "1",
                "severity": "error",
                "action": "fail",
                "parameters": {},
            }
            for handler in ("prepare_rows", "relationship_checks", "silver_checks")
        ] + [
            {
                "id": "too-large",
                "handler": "value_range",
                "version": "1",
                "severity": "error",
                "action": "quarantine",
                "parameters": {"column": "amount", "maximum": 100},
            },
            {
                "id": "too-negative",
                "handler": "value_range",
                "version": "1",
                "severity": "error",
                "action": "fail",
                "parameters": {"column": "amount", "minimum": -100},
            },
            {
                "id": "negative-warning",
                "handler": "value_range",
                "version": "1",
                "severity": "warning",
                "action": "warn",
                "parameters": {"column": "amount", "minimum": 0},
            },
        ]

    def _load(self, *, reject_publication: bool = False) -> None:
        self.runtime.inventory(self.spark, self.fs, self.context)
        process = self.runtime.process_table
        failures = []

        def capture(*args):
            try:
                return process(*args)
            except self.runtime._operation_errors() as error:
                failures.append(error)
                raise

        try:
            with patch.object(self.runtime, "process_table", side_effect=capture):
                self.runtime.load_silver(self.spark, self.fs, self.context)
        except RuntimeError as error:
            if failures:
                raise error from failures[-1]
            raise
        rules = [("injected_failure", 1)] if reject_publication else []
        with patch.object(self.runtime, "source_business_checks", return_value=rules):
            self.runtime.validate_source(self.spark, self.fs, self.context)

    def _silver(self):
        return self.spark.table(self.runtime.table_path(self.context, self.table))

    def _state(self):
        return self.runtime.latest_silver(self.spark, self.context, self.table)

    def test_projection_order_changes_preserve_physical_schema_and_business_versions(
        self,
    ) -> None:
        self.table["hash_policy"] = {
            "key_columns": ["id"],
            "value_columns": ["id", "amount"],
            "encoding": "spark_json_struct_sha256_v1",
        }
        self._land("10.00", batch="batch-1", hour=8)
        self._load()
        initial = self._silver().first()
        columns = self._silver().columns
        self.table["columns"].reverse()

        def land_reordered(amount, batch, hour):
            self.context.update(
                pipeline_run_id=batch,
                batch_started_utc=datetime(2026, 11, 3, hour, tzinfo=UTC),
            )
            path = Path(self.runtime.source_path(self.context, self.table))
            path.parent.mkdir(parents=True, exist_ok=True)
            values = {
                "id": "1",
                "amount": amount,
                "extract_date": "2026-11-03T01:00:00.000",
            }
            with path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream, escapechar="\\")
                names = [column["name"] for column in self.table["columns"]]
                writer.writerow(names)
                writer.writerow([values[name] for name in names])

        land_reordered("10.00", "batch-2", 9)
        self._load()
        self.assertEqual(self._silver().columns, columns)
        self.assertEqual(self._silver().count(), 1)
        self.assertEqual(self._silver().first().record_hash, initial.record_hash)
        self._load()
        self.assertEqual(
            self.runtime._run_audit(
                self.spark, self.context, "bronze_silver_validation"
            )
            .filter("check_name = 'source_batch_committed' AND status = 'SUCCESS'")
            .count(),
            1,
        )
        commit = self.runtime.source_commit_evidence(self.spark, self.context)
        self.assertEqual(commit["kind"], "source_commit")
        self.assertEqual(
            commit["location"], "spark_catalog.audit.bronze_silver_validation"
        )
        self.assertIsInstance(commit["version"], int)
        self.assertEqual(len(commit["sha256"]), 64)
        land_reordered("12.00", "batch-3", 10)
        self._load()
        self.assertEqual(self._silver().columns, columns)
        self.assertEqual(self._silver().count(), 2)
        self.assertEqual(str(self._state().first().amount), "12.00")
        self.assertEqual(self._state().first().record_version, 2)

    def test_single_silver_store_hash_changes_replay_reversion_and_failed_batches(
        self,
    ) -> None:
        self._land("10.00", batch="batch-1", hour=8)
        self._load()
        first_id = self._silver().first().record_id
        self._load()
        self.assertEqual(self._silver().count(), 1)
        self.assertEqual(self._silver().first().record_id, first_id)

        self._land("20.00", batch="batch-2", hour=9)
        with self.assertRaisesRegex(RuntimeError, "Source validation failed"):
            self._load(reject_publication=True)
        self.assertEqual(self._silver().count(), 2)
        self.assertEqual(str(self._state().first().amount), "10.00")

        self._land("20.00", batch="batch-3", hour=10)
        self._load()
        self.assertEqual(self._silver().count(), 3)
        self.assertEqual(str(self._state().first().amount), "20.00")

        self._land("10.00", batch="batch-4", hour=11)
        self._load()
        committed = (
            self.runtime.eligible_silver(self.spark, self.context, self.table)
            .orderBy("record_version")
            .collect()
        )
        self.assertEqual(
            [str(r.amount) for r in committed], ["10.00", "20.00", "10.00"]
        )
        self.assertEqual([r.record_version for r in committed], [1, 3, 4])
        self.assertEqual(
            [r.record_change_type for r in committed], ["INSERT", "UPDATE", "UPDATE"]
        )
        self.assertEqual(committed[1].record_previous_hash, committed[0].record_hash)
        self.assertEqual(committed[2].record_previous_hash, committed[1].record_hash)

        self._land("10.00", batch="batch-5", hour=12, extract_hour=1)
        self._load()
        self.assertEqual(self._silver().count(), 4)
        self.assertEqual(self._state().first().ingest_batch_run_id, "batch-4")
        self.assertTrue(
            all(
                not count
                for _, count in self.runtime.silver_checks(
                    self.spark, self.context, self.table
                )
            )
        )
        self.runtime.cleanup_snapshots(self.spark, self.fs, self.context)
        self.assertTrue(
            Path(self.runtime.source_path(self.context, self.table)).is_file()
        )
        self.assertFalse(self.spark.catalog.databaseExists(f"hist_{self.suffix}"))
        self.assertEqual(
            len(self.spark.catalog.listTables(self.context["silver_schema"])), 1
        )
        self.assertNotIn("silver_history_observations", self.runtime.AUDIT_SCHEMAS)

        with self.assertRaisesRegex(
            self.runtime._operation_errors(),
            "appendOnly|only allow appends|append-only",
        ):
            self.runtime.delta_table(
                self.spark, self.runtime.table_path(self.context, self.table)
            ).delete("id = 1")
        self.assertEqual(self._silver().count(), 4)

        self._land("99.00", batch="batch-5", hour=12)
        with self.assertRaisesRegex(RuntimeError, "inventory failed"):
            self.runtime.inventory(self.spark, self.fs, self.context)
        self.context.update(
            pipeline_run_id="batch-4",
            batch_started_utc=datetime(2026, 11, 3, 11, tzinfo=UTC),
        )
        with self.assertRaisesRegex(RuntimeError, "Silver failed"):
            self.runtime.load_silver(self.spark, self.fs, self.context)
        self.assertEqual(str(self._state().first().amount), "10.00")
        self.assertEqual(self._silver().count(), 4)

    def test_quarantine_evidence_is_required_and_replay_is_idempotent(self) -> None:
        self._configure_quality_rules()
        self._land_quality_fixture([(1, "10.00"), (2, "500.00"), (3, "-1.00")])
        self.runtime.inventory(self.spark, self.fs, self.context)
        result = self.runtime.load_silver(self.spark, self.fs, self.context)[0]
        self.assertEqual(
            (
                result["status"],
                result["source_rows"],
                result["silver_rows"],
                result["quarantine_rows"],
            ),
            ("SUCCESS", 3, 2, 1),
        )
        quarantine = self.runtime.table_path(self.context, self.table, quarantine=True)
        rejected = self.spark.table(quarantine).collect()
        self.assertEqual([(row.id, row.amount) for row in rejected], [("2", "500.00")])
        self.assertEqual(
            self._silver().filter("id = 3").first().dq_warning, "negative-warning@1"
        )
        warning_counts = (
            self.runtime._run_audit(self.spark, self.context, "silver_dq_rule_summary")
            .filter("dq_rule = 'warning:negative-warning@1'")
            .select("failure_count")
            .collect()
        )
        self.assertEqual([row.failure_count for row in warning_counts], [1])

        self.runtime.delta_table(self.spark, quarantine).delete("id = '2'")
        with (
            patch.object(self.runtime, "source_business_checks", return_value=[]),
            self.assertRaisesRegex(RuntimeError, "Source validation failed"),
        ):
            self.runtime.validate_source(self.spark, self.fs, self.context)
        self.assertEqual(
            self.runtime._run_audit(
                self.spark, self.context, "bronze_silver_validation"
            )
            .filter("check_name = 'source_batch_committed' AND status = 'SUCCESS'")
            .count(),
            0,
        )

        self.runtime.load_silver(self.spark, self.fs, self.context)
        with patch.object(self.runtime, "source_business_checks", return_value=[]):
            self.runtime.validate_source(self.spark, self.fs, self.context)
        self._load()
        self.assertEqual(self._silver().count(), 2)
        self.assertEqual(self._state().count(), 2)
        self.assertEqual(self.spark.table(quarantine).count(), 1)
        self.assertEqual(
            self.runtime._run_audit(self.spark, self.context, "silver_dq_rule_summary")
            .filter("dq_rule = 'warning:negative-warning@1'")
            .count(),
            3,
        )
        self.assertEqual(
            self.runtime._run_audit(
                self.spark, self.context, "bronze_silver_validation"
            )
            .filter("check_name = 'source_batch_committed' AND status = 'SUCCESS'")
            .count(),
            1,
        )

    def test_fail_and_baseline_violations_still_block_source_commit(self) -> None:
        self._configure_quality_rules()
        fail_rule = next(
            rule for rule in self.table["rule_specs"] if rule["id"] == "too-negative"
        )
        self.table["rule_specs"].extend(
            [
                {**fail_rule, "id": "warning:negative-warning"},
                {**fail_rule, "id": "failure:warning:negative-warning"},
            ]
        )
        self._land_quality_fixture(
            [(1, "10.00"), (2, "500.00"), (3, "-101.00"), (4, "invalid")]
        )
        with self.assertRaisesRegex(RuntimeError, "Silver failed"):
            self._load()
        quarantine = self.runtime.table_path(self.context, self.table, quarantine=True)
        rejected = {
            row.id: row.dq_failure_reason
            for row in self.spark.table(quarantine).collect()
        }
        self.assertIn("too-large@1", rejected["2"])
        self.assertIn("too-negative@1", rejected["3"])
        self.assertIn("INVALID_VALUE:amount", rejected["4"])
        summary = {
            row.dq_rule: row.failure_count
            for row in self.runtime._run_audit(
                self.spark, self.context, "silver_dq_rule_summary"
            ).collect()
        }
        self.assertEqual(summary["warning:negative-warning@1"], 2)
        self.assertEqual(summary["failure:warning:negative-warning@1"], 2)
        self.assertEqual(summary["failure:failure:warning:negative-warning@1"], 2)
        self.assertEqual(self._state().count(), 0)
        self.assertEqual(
            self.runtime._run_audit(
                self.spark, self.context, "bronze_silver_validation"
            )
            .filter("check_name = 'source_batch_committed' AND status = 'SUCCESS'")
            .count(),
            0,
        )

    def test_warning_only_unchanged_batch_records_a_new_summary(self) -> None:
        self._configure_quality_rules()
        self._land("-1.00", batch="warning-1-" + self.suffix, hour=8)
        self._load()
        self._land("-1.00", batch="warning-2-" + self.suffix, hour=9)
        self._load()
        self.assertEqual(self._silver().count(), 1)
        summary = (
            self.runtime._run_audit(self.spark, self.context, "silver_dq_rule_summary")
            .filter("dq_rule = 'warning:negative-warning@1'")
            .select("failure_count")
            .collect()
        )
        self.assertEqual([row.failure_count for row in summary], [1])

    def test_legacy_current_state_requires_non_destructive_migration(self) -> None:
        raw = self.spark.createDataFrame(
            [("1", "5.00", "2026-11-02T00:00:00.000")],
            ["id", "amount", "extract_date"],
        )
        old = self.runtime._with_lineage(
            self.runtime.prepare_rows(raw, self.table), self.context, self.table
        ).drop("_raw_record", "_dq_reasons", "ingest_corrupt_record")
        old.write.format("delta").saveAsTable(
            self.runtime.table_path(self.context, self.table)
        )
        original_schema = self._silver().schema
        self._land("10.00", batch="batch-1", hour=8)
        with self.assertRaisesRegex(RuntimeError, "Silver failed"):
            self._load()
        self.assertEqual(self._silver().schema, original_schema)
        self.assertEqual(str(self._silver().first().amount), "5.00")
        self.assertNotIn("record_hash", self._silver().columns)


if __name__ == "__main__":
    unittest.main()
