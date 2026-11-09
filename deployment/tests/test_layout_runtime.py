"""Fake-Spark execution and opt-in real local Delta layout evidence."""

from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from collections import Counter
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from test_layout_contracts import CAPABILITIES, LIQUID, PARTITIONED, SCHEMA, layout


def spark_schema(columns=SCHEMA):
    return SimpleNamespace(
        fields=[
            SimpleNamespace(
                name=c.name,
                dataType=SimpleNamespace(simpleString=lambda t=c.data_type: t),
                nullable=c.nullable,
            )
            for c in columns
        ]
    )


class Frame:
    def __init__(self, rows=(), schema=None):
        self.rows = list(rows)
        self.schema = schema or spark_schema()

    def limit(self, number):
        return Frame(self.rows[:number], self.schema)

    def collect(self):
        if len(self.rows) > 2:
            raise AssertionError("Only bounded metadata may be collected")
        return self.rows

    def select(self, *columns):
        names = [f.name for f in self.schema.fields]
        indices = [names.index(c) for c in columns]
        return Frame([tuple(row[i] for i in indices) for row in self.rows], self.schema)

    def exceptAll(self, other):
        counts = Counter(self.rows) - Counter(other.rows)
        return Frame(list(counts.elements()), self.schema)

    def count(self):
        return len(self.rows)


class FakeSpark:
    version = "3.5.7"

    def __init__(self, *, partition=(), cluster=(), exists=True):
        self.exists = exists
        self.version_number = 7
        self.table_id = "stable-table-id"
        self.partition = list(partition)
        self.cluster = list(cluster)
        self.calls = []
        self.detail_overrides = {}
        self.fail_sql = False
        self.corrupt_after = False
        self.catalog = SimpleNamespace(tableExists=lambda name: self.exists)
        self.rows = [(1, None, "north", "run"), (1, None, "north", "run")]
        self.snapshots = {7: list(self.rows)}
        self.read = self
        self.read_version = None

    def format(self, name):
        assert name == "delta"
        return self

    def option(self, key, value):
        assert key == "versionAsOf"
        self.read_version = value
        return self

    def table(self, name):
        rows = self.snapshots.get(self.read_version, self.rows)
        self.read_version = None
        return Frame(rows)

    def sql(self, sql):
        self.calls.append(sql)
        if sql.startswith("DESCRIBE HISTORY"):
            return Frame([{"version": self.version_number}])
        if sql.startswith("DESCRIBE DETAIL"):
            return Frame(
                [
                    {
                        "format": "delta",
                        "id": self.table_id,
                        "partitionColumns": self.partition,
                        "clusteringColumns": self.cluster,
                        "sizeInBytes": 1024,
                    }
                    | self.detail_overrides
                ]
            )
        if sql.startswith(("OPTIMIZE ", "ALTER TABLE ")):
            if self.fail_sql:
                raise RuntimeError("interrupted native operation")
            self.version_number += 1
            if sql.startswith("ALTER TABLE "):
                self.cluster = ["region"]
            if self.corrupt_after:
                self.rows.pop()
            self.snapshots[self.version_number] = list(self.rows)
            return Frame()
        if sql.startswith("CREATE TABLE "):
            self.exists = True
            self.partition = [] if "CLUSTER BY" in sql else ["day"]
            self.cluster = ["id", "region"] if "CLUSTER BY" in sql else []
            self.rows = []
            self.version_number = 0
            self.snapshots = {0: []}
            return Frame()
        raise AssertionError(f"Unexpected mutation/query: {sql}")

    def mutations(self):
        return [s for s in self.calls if not s.startswith(("DESCRIBE ", "SHOW "))]


class FakeLease:
    def __init__(self):
        self.owned = True
        self.checks = 0

    def assert_owned(self):
        self.checks += 1
        if not self.owned:
            raise RuntimeError("lease lost")


@contextmanager
def owned(lease):
    lease.assert_owned()
    yield lease


class LayoutRuntimeTests(unittest.TestCase):
    def test_no_requested_layout_preserves_all_existing_layouts_without_mutation(self):
        for partition, cluster in (((), ()), (("day",), ()), ((), ("id", "region"))):
            spark = FakeSpark(partition=partition, cluster=cluster)
            plan = layout.prewrite_plan(
                spark, "slv.entries", None, SCHEMA, capabilities=CAPABILITIES
            )
            self.assertEqual(plan.adapter, "preserve")
            self.assertEqual(plan.existing.partition_columns, partition)
            self.assertEqual(plan.existing.cluster_by_columns, cluster)
            self.assertEqual(spark.mutations(), [])
            self.assertEqual(len(spark.rows), 2)

    def test_preserving_existing_liquid_still_needs_a_capable_writer(self):
        spark = FakeSpark(cluster=("id",))
        with self.assertRaisesRegex(layout.LayoutError, "liquid_write"):
            layout.prewrite_plan(spark, "slv.entries", None, SCHEMA)
        self.assertEqual(spark.mutations(), [])

    def test_parent_hook_creates_empty_before_merge_and_checks_existence(self):
        spark = FakeSpark(exists=False)
        layout.ensure_target_layout(
            spark, "slv.entries", Frame(), PARTITIONED, existing_target=False
        )
        self.assertEqual(spark.rows, [])
        self.assertEqual(spark.mutations()[0].split()[0:2], ["CREATE", "TABLE"])
        count = len(spark.mutations())
        layout.ensure_target_layout(
            spark, "slv.entries", Frame(), None, existing_target=True
        )
        self.assertEqual(len(spark.mutations()), count)
        with self.assertRaises(layout.LayoutError):
            layout.ensure_target_layout(
                spark, "slv.entries", Frame(), PARTITIONED, existing_target=False
            )
        with self.assertRaises(layout.LayoutError):
            layout.ensure_target_layout(
                FakeSpark(exists=False),
                "slv.entries",
                Frame(),
                None,
                existing_target=False,
            )

    def test_clustering_absence_requires_authoritative_feature_evidence(self):
        spark = FakeSpark()
        spark.detail_overrides = {"clusteringColumns": None, "tableFeatures": []}
        self.assertEqual(
            layout.inspect_target(spark, "slv.entries").cluster_by_columns, ()
        )
        spark.detail_overrides["tableFeatures"] = ["clustering", "domainMetadata"]
        with self.assertRaisesRegex(layout.LayoutError, "clusteringColumns"):
            layout.inspect_target(spark, "slv.entries")

    def test_clustering_feature_with_no_keys_requires_capable_writer(self):
        spark = FakeSpark()
        spark.detail_overrides = {"tableFeatures": ["clustering", "domainMetadata"]}
        with self.assertRaisesRegex(layout.LayoutError, "liquid_write"):
            layout.prewrite_plan(spark, "slv.entries", None, SCHEMA)
        layout.prewrite_plan(
            spark, "slv.entries", None, SCHEMA, capabilities=CAPABILITIES
        )
        self.assertEqual(spark.mutations(), [])

    def test_inspection_failure_and_history_drift_are_not_missing_targets(self):
        spark = FakeSpark()
        sql = spark.sql

        def drifting_sql(query):
            result = sql(query)
            if query.startswith("DESCRIBE DETAIL"):
                spark.version_number += 1
            return result

        spark.sql = drifting_sql
        with self.assertRaisesRegex(layout.LayoutError, "drift"):
            layout.inspect_target(spark, "slv.entries")
        spark.catalog.tableExists = lambda name: (_ for _ in ()).throw(
            RuntimeError("catalog unavailable")
        )
        with self.assertRaisesRegex(RuntimeError, "catalog unavailable"):
            layout.prewrite_plan(spark, "slv.entries", PARTITIONED, SCHEMA)
        self.assertEqual(spark.mutations(), [])

    def test_exact_compatibility_and_type_mismatch_before_append_or_merge(self):
        spark = FakeSpark(partition=("day",))
        self.assertEqual(
            layout.prewrite_plan(spark, "slv.entries", PARTITIONED, SCHEMA).adapter,
            "preserve",
        )
        for metadata, columns in (
            (LIQUID, SCHEMA),
            (PARTITIONED, (SCHEMA[0], layout.Column("day", "string"), *SCHEMA[2:])),
            (PARTITIONED | {"partition_columns": ["day", "region"]}, SCHEMA),
        ):
            with self.subTest(metadata=metadata), self.assertRaises(layout.LayoutError):
                layout.prewrite_plan(
                    spark, "slv.entries", metadata, columns, capabilities=CAPABILITIES
                )
        spark = FakeSpark(cluster=("region", "id"))
        with self.assertRaisesRegex(layout.LayoutError, "migration"):
            layout.prewrite_plan(
                spark, "gld.entries", LIQUID, SCHEMA, capabilities=CAPABILITIES
            )
        self.assertEqual(spark.mutations(), [])

    def test_unpartitioned_history_is_not_transformed_by_metadata(self):
        spark = FakeSpark()
        with self.assertRaisesRegex(layout.LayoutError, "migration"):
            layout.prewrite_plan(spark, "slv.entries", PARTITIONED, SCHEMA)
        self.assertEqual(spark.mutations(), [])
        self.assertEqual(spark.version_number, 7)

    def test_missing_or_unverifiable_detail_fails_closed(self):
        for overrides in (
            {"format": "parquet"},
            {"id": None},
            {"partitionColumns": None},
            {"clusteringColumns": None},
            {"partitionColumns": ["day"], "clusteringColumns": ["id"]},
            {"partitionColumns": "day"},
        ):
            spark = FakeSpark()
            spark.detail_overrides = overrides
            with (
                self.subTest(overrides=overrides),
                self.assertRaises(layout.LayoutError),
            ):
                layout.inspect_target(spark, "slv.entries")

    def test_new_target_needs_selection_and_writer_uses_partition_by_only_on_creation(
        self,
    ):
        spark = FakeSpark(exists=False)
        with self.assertRaises(layout.LayoutError):
            layout.prewrite_plan(spark, "slv.entries", None, SCHEMA)
        plan = layout.prewrite_plan(spark, "slv.entries", PARTITIONED, SCHEMA)
        writer_calls = []

        class Writer:
            def format(self, value):
                writer_calls.append(("format", value))
                return self

            def mode(self, value):
                writer_calls.append(("mode", value))
                return self

            def partitionBy(self, *values):
                writer_calls.append(("partitionBy", values))
                return self

        layout.prepare_creation_writer(spark, Writer(), plan)
        self.assertEqual(
            writer_calls,
            [("format", "delta"), ("mode", "errorifexists"), ("partitionBy", ("day",))],
        )
        spark.exists = True
        with self.assertRaises(layout.LayoutError):
            layout.prepare_creation_writer(spark, Writer(), plan)
        self.assertEqual(len(writer_calls), 3)

    def test_create_target_and_liquid_sql_adapter_gate(self):
        spark = FakeSpark(exists=False)
        plan = layout.prewrite_plan(spark, "slv.entries", PARTITIONED, SCHEMA)
        result = layout.create_target(spark, plan)
        self.assertEqual(result.partition_columns, ("day",))
        self.assertEqual(len(spark.mutations()), 1)
        spark = FakeSpark(exists=False)
        plan = layout.prewrite_plan(
            spark, "gld.entries", LIQUID, SCHEMA, capabilities=CAPABILITIES
        )
        self.assertEqual(plan.adapter, "create_table_sql")
        with self.assertRaisesRegex(layout.LayoutError, "clusterBy"):
            layout.prepare_creation_writer(spark, object(), plan)
        created = layout.create_target(spark, plan, capabilities=CAPABILITIES)
        self.assertEqual(created.cluster_by_columns, ("id", "region"))
        self.assertEqual(spark.rows, [])
        self.assertEqual(len(spark.mutations()), 1)
        spark = FakeSpark(exists=False)
        spark.version = "4.0.0"
        with self.assertRaisesRegex(layout.LayoutError, "runtime"):
            layout.create_target(spark, plan, capabilities=CAPABILITIES)
        self.assertEqual(spark.mutations(), [])

    def _maintenance(self, spark, operation="optimize", metadata=LIQUID):
        return layout.plan_maintenance(
            spark,
            "gld.entries",
            metadata,
            SCHEMA,
            operation=operation,
            capabilities=CAPABILITIES,
        )

    def _approval(self, plan):
        return layout.OperationApproval(
            plan_token=plan.token,
            operation_id="approved-operation-9",
            configuration_version="config-2",
            worker_version="worker-3",
            data_release="release-4",
            rollback_reference="retain Delta history + prior config/release references",
            consumer_proof="scratch SQL/Direct Lake proof reference",
        )

    def test_configuring_columns_is_not_optimize_and_neither_uses_zorder(self):
        spark = FakeSpark(cluster=("id", "region"))
        plan = self._maintenance(
            spark,
            "configure",
            {"layout_strategy": "liquid", "cluster_by_columns": ["region"]},
        )
        self.assertTrue(plan.sql.startswith("ALTER TABLE "))
        self.assertNotIn("OPTIMIZE", plan.sql)
        self.assertFalse(plan.rewrites_data)
        self.assertEqual(spark.mutations(), [])
        result = layout.execute_maintenance(
            spark,
            plan,
            approval=self._approval(plan),
            ownership=owned(FakeLease()),
            capabilities=CAPABILITIES,
        )
        self.assertTrue(result.comparison.equivalent)
        self.assertEqual(result.before.version, 7)
        self.assertEqual(result.after.version, 8)
        self.assertEqual(spark.mutations(), [plan.sql])
        optimize = self._maintenance(
            spark,
            metadata={"layout_strategy": "liquid", "cluster_by_columns": ["region"]},
        )
        self.assertTrue(optimize.rewrites_data)
        self.assertEqual(optimize.sql, "OPTIMIZE `gld`.`entries`")
        self.assertNotIn("ZORDER", optimize.sql)
        self.assertNotIn("VACUUM", optimize.sql)

    def test_maintenance_rejects_migration_zorder_vacuum_and_unknown_costs_stay_unknown(
        self,
    ):
        spark = FakeSpark()
        with self.assertRaises(layout.MigrationRequired):
            self._maintenance(spark, "configure")
        for operation in ("zorder", "vacuum", "optimize_full", "overwrite"):
            with (
                self.subTest(operation=operation),
                self.assertRaises(layout.LayoutError),
            ):
                self._maintenance(FakeSpark(cluster=("id", "region")), operation)
        plan = self._maintenance(FakeSpark(cluster=("id", "region")))
        self.assertIsNone(plan.estimate.elapsed_seconds)
        self.assertIsNone(plan.estimate.capacity_unit_seconds)
        self.assertIsNone(plan.estimate.cost)

    def test_approval_owner_and_version_or_identity_drift_reject_before_mutation(self):
        for reason in ("approval", "token", "history", "owner", "version", "identity"):
            spark = FakeSpark(cluster=("id", "region"))
            plan = self._maintenance(spark)
            approval = self._approval(plan)
            lease = FakeLease()
            if reason == "approval":
                approval = None
            elif reason == "token":
                approval = replace(approval, plan_token="another-plan")
            elif reason == "history":
                approval = replace(approval, preserve_history=False)
            elif reason == "owner":
                lease.owned = False
            elif reason == "version":
                spark.version_number += 1
            elif reason == "identity":
                spark.table_id = "recreated-table"
            with self.subTest(reason=reason), self.assertRaises(RuntimeError):
                layout.execute_maintenance(
                    spark,
                    plan,
                    approval=approval,
                    ownership=owned(lease),
                    capabilities=CAPABILITIES,
                )
            self.assertEqual(spark.mutations(), [])

    def test_successful_optimize_proves_full_content_and_keeps_history(self):
        spark = FakeSpark(cluster=("id", "region"))
        plan = self._maintenance(spark)
        lease = FakeLease()
        result = layout.execute_maintenance(
            spark,
            plan,
            approval=self._approval(plan),
            ownership=owned(lease),
            capabilities=CAPABILITIES,
        )
        self.assertTrue(result.comparison.equivalent)
        self.assertGreaterEqual(lease.checks, 3)
        self.assertEqual(spark.snapshots[7], spark.snapshots[8])
        self.assertEqual(result.approval.data_release, "release-4")

    def test_lease_loss_after_command_does_not_report_completion_or_retry(self):
        spark = FakeSpark(cluster=("id", "region"))
        plan = self._maintenance(spark)
        lease = FakeLease()
        sql = spark.sql

        def lose_lease(query):
            result = sql(query)
            if query.startswith("OPTIMIZE "):
                lease.owned = False
            return result

        spark.sql = lose_lease
        with self.assertRaisesRegex(layout.OperationIncomplete, "reconcile"):
            layout.execute_maintenance(
                spark,
                plan,
                approval=self._approval(plan),
                ownership=owned(lease),
                capabilities=CAPABILITIES,
            )
        self.assertEqual(spark.mutations(), [plan.sql])
        self.assertIn(7, spark.snapshots)

    def test_interruption_or_content_loss_is_incomplete_not_automatically_retried_or_undone(
        self,
    ):
        for corrupt in (False, True):
            spark = FakeSpark(cluster=("id", "region"))
            spark.corrupt_after = corrupt
            spark.fail_sql = not corrupt
            plan = self._maintenance(spark)
            with (
                self.subTest(corrupt=corrupt),
                self.assertRaisesRegex(layout.OperationIncomplete, "reconcile"),
            ):
                layout.execute_maintenance(
                    spark,
                    plan,
                    approval=self._approval(plan),
                    ownership=owned(FakeLease()),
                    capabilities=CAPABILITIES,
                )
            self.assertEqual(len(spark.mutations()), 1)
            self.assertEqual(len(spark.snapshots[7]), 2)

    def test_migration_preview_is_read_only_estimated_and_execution_explicitly_gated(
        self,
    ):
        spark = FakeSpark()
        current = layout.inspect_target(spark, "slv.entries")
        requested = layout.validate_layout(PARTITIONED, SCHEMA)
        preview = layout.preview_migration(
            current,
            requested,
            input_versions={"slv.entries": 7},
            throughput_bytes_per_second=512,
            capacity_units=8,
            cost_per_cu_second=0.1,
        )
        self.assertEqual(preview.expected_target_version, 7)
        self.assertIsNone(preview.result_version)
        self.assertEqual(preview.input_versions, (("slv.entries", 7),))
        self.assertTrue(preview.rewrites_data)
        self.assertEqual(preview.estimate.read_bytes, 1024)
        self.assertEqual(preview.estimate.capacity_unit_seconds, 16)
        self.assertAlmostEqual(preview.estimate.cost, 1.6)
        self.assertTrue(preview.consumer_requirements)
        self.assertEqual(spark.mutations(), [])
        with self.assertRaisesRegex(layout.MigrationRequired, "isolated target"):
            layout.execute_migration(
                spark,
                preview,
                approval=self._approval(preview),
                ownership=owned(FakeLease()),
            )
        self.assertEqual(spark.mutations(), [])
        self.assertEqual(spark.version_number, 7)

    def test_comparison_is_bidirectional_and_duplicate_and_null_sensitive(self):
        left = Frame([(1, None, "a", "run"), (1, None, "a", "run")])
        right = Frame([(1, None, "a", "run"), (2, None, "a", "run")])
        result = layout.compare_logical_content(left, right)
        self.assertEqual((result.left_only, result.right_only), (1, 1))
        self.assertFalse(result.equivalent)
        self.assertTrue(layout.compare_logical_content(left, left).equivalent)
        different_schema = Frame(
            left.rows, spark_schema((layout.Column("other", "int"),))
        )
        self.assertFalse(
            layout.compare_logical_content(left, different_schema).equivalent
        )


@unittest.skipUnless(
    os.environ.get("TREASURY_DELTA_TESTS") == "1",
    "Opt-in real local Delta tier: set TREASURY_DELTA_TESTS=1; no dependencies installed",
)
class LocalDeltaLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Use already installed packages/JARs only; this suite never fetches Maven artifacts.
        from pyspark.sql import SparkSession

        jars = os.environ.get("TREASURY_DELTA_JARS")
        if not jars or not all(Path(p).is_file() for p in jars.split(",")):
            raise unittest.SkipTest(
                "Set TREASURY_DELTA_JARS to existing local Delta JARs"
            )
        cls.storage = tempfile.TemporaryDirectory()
        try:
            cls.spark = (
                SparkSession.builder.master("local[2]")
                .appName("treasury-layout-local")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.warehouse.dir", Path(cls.storage.name).as_uri())
                .config("spark.jars", jars)
                .config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .getOrCreate()
            )
        except Exception:
            cls.storage.cleanup()
            raise
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.storage.cleanup()

    def test_partitioned_create_append_preserve_and_reject_metadata_change(self):
        table = "layout_" + uuid.uuid4().hex
        schema = (layout.Column("id", "bigint"), layout.Column("day", "string"))
        try:
            plan = layout.prewrite_plan(self.spark, table, PARTITIONED, schema)
            created = layout.create_target(self.spark, plan)
            self.assertEqual(created.partition_columns, ("day",))
            frame = self.spark.createDataFrame(
                [(1, "2026-11-09"), (1, "2026-11-09")], "id long, day string"
            )
            layout.prewrite_plan(self.spark, table, PARTITIONED, frame.schema)
            frame.write.format("delta").mode("append").saveAsTable(table)
            before = layout.inspect_target(self.spark, table)
            preserved = layout.prewrite_plan(self.spark, table, None, frame.schema)
            self.assertEqual(preserved.adapter, "preserve")
            self.assertEqual(before, preserved.existing)
            self.assertTrue(
                layout.compare_logical_content(
                    frame, self.spark.table(table)
                ).equivalent
            )
            with self.assertRaises(layout.LayoutError):
                layout.prewrite_plan(
                    self.spark,
                    table,
                    PARTITIONED | {"partition_columns": ["id"]},
                    frame.schema,
                )
            self.assertEqual(before, layout.inspect_target(self.spark, table))
            self.assertEqual(self.spark.table(table).count(), 2)
        finally:
            self.spark.sql(f"DROP TABLE IF EXISTS {layout.quote_table(table)}")

    @unittest.skip(
        "Local liquid runtime is not validated; requires native Fabric capability/consumer proof"
    )
    def test_liquid_requires_native_acceptance(self):
        pass


if __name__ == "__main__":
    unittest.main()
