# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Focused pure and opt-in Spark/Delta Gold transformation evidence."""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
import tempfile
import unittest
import uuid
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fabric_medallion_deployer.contracts import load_sources
from fabric_medallion_deployer.gold import gold_config
from fabric_medallion_deployer.use_cases import load_case_extension, load_use_case
from test_runtime_controls import case_rule_handlers

ROOT = Path(__file__).resolve().parents[2]


def load_runtime(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gold = load_runtime("test_gold_runtime_module", "artefacts/gold/gold_runtime.py")
silver = load_runtime(
    "test_gold_silver_module", "artefacts/medallion/ingestion.py"
)


class GoldRequestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = gold_config(
            ROOT, load_sources(ROOT, use_case="treasury"), use_case="treasury"
        )
        cls.runs = {
            s["source_code"]: {
                "pipeline_run_id": "run",
                "processing_date": "2026-11-03",
            }
            for s in cls.config["sources"]
        }

    def test_explicit_sources_and_demo_only_guard(self) -> None:
        runs, as_of = gold.gold_request(self.config, json.dumps(self.runs), "gold-run")
        self.assertEqual(runs, self.runs)
        self.assertEqual(as_of, date(2026, 11, 3))
        for config, runs, release in (
            (self.config | {"execution_mode": "production"}, self.runs, "run"),
            (self.config, {"bas": self.runs["bas"]}, "run"),
            (self.config, self.runs, "../escape"),
            (
                self.config,
                self.runs
                | {"bas": {"pipeline_run_id": None, "processing_date": "2026-11-03"}},
                "run",
            ),
            (
                self.config,
                self.runs
                | {"csd": {"pipeline_run_id": "run", "processing_date": "2026-11-04"}},
                "run",
            ),
        ):
            with self.subTest(release=release), self.assertRaises(ValueError):
                gold.gold_request(config, json.dumps(runs), release)

    def test_sealed_utc_observation_uses_spark_row_local_time_conversion(self) -> None:
        from pyspark.sql.types import TimestampType

        observed = datetime(2026, 11, 3, 8, tzinfo=UTC)
        row_value = TimestampType().fromInternal(int(observed.timestamp() * 1_000_000))
        self.assertEqual(gold._gold_row_observed_utc(row_value), observed)
        self.assertEqual(gold._gold_row_observed_utc(observed), observed)


@unittest.skipUnless(
    os.environ.get("TREASURY_GOLD_SPARK_TESTS") == "1"
    or (
        os.environ.get("TREASURY_SPARK_TESTS") == "1"
        and os.environ.get("TREASURY_DELTA_TESTS") == "1"
    ),
    "Opt-in Gold Spark/Delta tier",
)
class GoldSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        cls.storage = tempfile.TemporaryDirectory()
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("treasury-gold-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", Path(cls.storage.name).as_uri())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.databricks.delta.snapshotPartitions", "2")
        )
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.sources = load_sources(ROOT, use_case="treasury")
        cls.config = gold_config(ROOT, cls.sources, use_case="treasury")
        gold._CASE_GOLD = load_case_extension(
            load_use_case(ROOT, "treasury"), "gold_rules"
        )
        silver._MEDALLION_RULE_HANDLERS = case_rule_handlers("treasury")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.storage.cleanup()

    def test_scd2_replay_reversion_conflicts_and_half_open_lookup(self) -> None:
        from pyspark.sql import functions as F

        rows = [
            ("1", "A", datetime(2026, 1, 1), "one"),
            ("1", "A", datetime(2026, 2, 1), "two"),
            ("1", "B", datetime(2026, 3, 1), "three"),
            ("1", "A", datetime(2026, 4, 1), "four"),
        ]
        frame = self.spark.createDataFrame(
            rows,
            "id string, label string, record_observed_utc timestamp, record_id string",
        )
        spec = {"name": "dim_example", "keys": ["id"], "attributes": ["label"]}
        output = gold.build_scd2(frame, spec)
        actual = output.filter("dimension_sk <> 0").orderBy("effective_from").collect()
        self.assertEqual([r.label for r in actual], ["A", "B", "A"])
        self.assertEqual(actual[0].effective_to, datetime(2026, 3, 1))
        self.assertIsNone(actual[-1].effective_to)
        self.assertTrue(actual[-1].is_current)
        self.assertEqual(output.filter("dimension_sk = 0").count(), 1)
        self.assertEqual(
            {r.dimension_sk for r in output.collect()},
            {r.dimension_sk for r in gold.build_scd2(frame, spec).collect()},
        )
        event = self.spark.createDataFrame(
            [("1", datetime(2026, 3, 1), "event"), ("1", datetime(2025, 1, 1), "old")],
            "id string, record_observed_utc timestamp, record_id string",
        )
        joined = gold.attach_dimension(event, output, spec, "member_sk").collect()
        self.assertEqual(
            {r.record_id: r.member_sk for r in joined},
            {
                "event": actual[1].dimension_sk,
                "old": 0,
            },
        )
        conflict = frame.unionByName(frame.limit(1).withColumn("label", F.lit("OTHER")))
        with self.assertRaisesRegex(RuntimeError, "conflicting dimension"):
            gold.build_scd2(conflict, spec)

    def _demo_frame(self, source, table):
        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        with source.seed_file(table["name"]).open(
            encoding="utf-8", newline=""
        ) as stream:
            records = list(csv.DictReader(stream))[:3]
        raw = self.spark.createDataFrame(
            records,
            T.StructType(
                [
                    T.StructField(c["name"], T.StringType(), True)
                    for c in table["columns"]
                ]
            ),
        )
        context = source.runtime_config() | {
            "processing_date": date(2026, 11, 3),
            "pipeline_run_id": "source-run",
        }
        frame = silver._with_lineage(
            silver.prepare_rows(raw, table), context, table
        ).drop("_dq_reasons", "_raw_record", "ingest_corrupt_record")
        frame = (
            frame.withColumn(
                "record_id", gold._gold_hash(*[F.col(k) for k in table["keys"]])
            )
            .withColumn("record_version", F.lit(1).cast("long"))
            .withColumn("record_hash", F.lit("sha"))
            .withColumn("record_observed_utc", F.lit(datetime(2026, 11, 3, tzinfo=UTC)))
        )
        return gold._department(frame, source.code, self.config)

    def test_all_source_backed_fact_projections_and_exact_money(self) -> None:
        from pyspark.sql import functions as F

        frames = {}
        for source in self.sources:
            for table in source.tables:
                if (source.code, table["name"]) not in {
                    (f["source"], f["table"]) for f in self.config["facts"]
                } | {(d["source"], d["table"]) for d in self.config["dimensions"]}:
                    continue
                frames[(source.code, table["name"])] = self._demo_frame(
                    source, table
                ).cache()
        dimensions = {
            spec["name"]: gold.build_scd2(
                frames[(spec["source"], spec["table"])], spec
            ).cache()
            for spec in self.config["dimensions"]
        }
        for spec in self.config["facts"]:
            with self.subTest(fact=spec["name"]):
                result = gold.build_fact(
                    frames[(spec["source"], spec["table"])],
                    spec,
                    dimensions,
                    self.config,
                    date(2026, 11, 3),
                    "gold-run",
                )
                self.assertLessEqual(result.count(), 3)
                self.assertTrue(
                    result.schema["fact_sk"].dataType.simpleString() == "bigint"
                )
                for measure in spec["measures"]:
                    self.assertEqual(
                        result.schema[measure].dataType.simpleString(),
                        spec["measure_types"][measure],
                    )
                result.createOrReplaceTempView(spec["name"])
        prefix = f"{self.config['target_lakehouse']}.{self.config['schema']}."
        for name, statement in self.config["mlvs"].items():
            query = "SELECT " + statement.split("\nAS SELECT ", 1)[1]
            self.assertGreaterEqual(
                self.spark.sql(query.replace(prefix, "")).count(), 0, name
            )
        bands = (
            self.spark.createDataFrame([(30,), (31,), (None,)], "days int")
            .select(gold.source_age_band(F.col("days")).alias("band"))
            .collect()
        )
        self.assertEqual([r.band for r in bands], ["0_30", "31_60", "UNKNOWN"])
        payroll = next(
            f for f in self.config["facts"] if f["name"] == "fact_payroll_component"
        )
        salary = (
            frames[("persal", "t_allow")]
            .limit(1)
            .withColumn("allow_amt", F.lit("12345.6789").cast("decimal(19,4)"))
        )
        self.assertEqual(
            str(
                gold.build_fact(
                    salary, payroll, {}, self.config, date(2026, 11, 3), "x"
                )
                .first()
                .allow_amt
            ),
            "12345.6789",
        )
        for frame in [*frames.values(), *dimensions.values()]:
            frame.unpersist()

    def test_supplier_identity_without_backdating_and_aggregate_queries(self) -> None:
        from pyspark.sql import functions as F

        supplier_spec = next(
            d for d in self.config["dimensions"] if d["name"] == "dim_supplier"
        )
        csd = self.sources[1]
        supplier_table = next(
            t for t in csd.tables if t["name"] == "supplier_identification"
        )
        later = self._demo_frame(csd, supplier_table).withColumn(
            "record_observed_utc", F.lit(datetime(2026, 11, 3, 1, tzinfo=UTC))
        )
        supplier = gold.build_scd2(later, supplier_spec).cache()
        names = {
            "fact_disbursement": "mlv_disbursement_monthly",
            "fact_payment_cycle": "mlv_payment_cycle_monthly",
            "fact_outstanding_payment_snapshot": "mlv_recorded_outstanding_payments",
        }
        for spec in (f for f in self.config["facts"] if f["name"] in names):
            table = next(
                t for t in self.sources[0].tables if t["name"] == spec["table"]
            )
            result = gold.build_fact(
                self._demo_frame(self.sources[0], table),
                spec,
                {"dim_supplier": supplier},
                self.config,
                date(2026, 11, 3),
                "identity-test",
            ).withColumn("entity_sk", F.lit(0).cast("long"))
            self.assertEqual(result.filter("supplier_sk <> 0").count(), 0)
            self.assertEqual(
                result.filter("supplier_business_key IS NOT NULL").count(), 3
            )
            result.createOrReplaceTempView(spec["name"])
            statement = self.config["mlvs"][names[spec["name"]]]
            query = "SELECT " + statement.split("\nAS SELECT ", 1)[1]
            query = query.replace(
                f"{self.config['target_lakehouse']}.{self.config['schema']}.", ""
            )
            self.assertEqual(self.spark.sql(query).count(), 3)
        supplier.unpersist()

    def test_manifest_multi_file_gold_reopens_sealed_source_membership(self) -> None:
        from test_layout_contracts import layout

        self.enterContext(patch.object(silver, "_MEDALLION_LAYOUT", layout, create=True))
        """Real local Delta and file I/O, not FTP/SFTP transport certification."""
        suffix = uuid.uuid4().hex[:10]
        table_name = "mf_sample_" + suffix
        schema = "gld_mf_" + suffix
        feed_id, run_id = "bas-local-file-fixture", "file-fixture-" + suffix
        batch = gold._gold_digest({"feed_id": feed_id, "run_id": run_id})
        table = {
            "name": table_name,
            "keys": ["id"],
            "foreign_keys": [],
            "allow_empty": False,
            "change_hash_exclusions": [],
            "hash_policy": {
                "key_columns": ["id"],
                "value_columns": ["id", "amount", "event_date"],
                "encoding": "spark_json_struct_sha256_v1",
            },
            "rule_specs": [
                {
                    "id": "local-" + handler.rsplit(".", 1)[1],
                    "handler": handler,
                    "version": "1",
                    "severity": "error",
                    "action": "fail",
                    "parameters": {},
                }
                for handler in (
                    "legacy.prepare_rows",
                    "legacy.relationship_checks",
                    "legacy.silver_checks",
                )
            ]
            + [
                {
                    "id": "local-quarantine",
                    "handler": "value_range",
                    "version": "1",
                    "severity": "error",
                    "action": "quarantine",
                    "parameters": {"column": "amount", "maximum": 100},
                }
            ],
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {
                    "name": "amount",
                    "data_type": "decimal",
                    "precision": 18,
                    "scale": 4,
                    "nullable": False,
                },
                {
                    "name": "event_date",
                    "data_type": "int",
                    "nullable": False,
                    "format": "YYYYMMDD",
                },
            ],
        }
        prefix = f"Files/bronze/bas_finance_sftp/20261103/{batch}/{table_name}"
        paths = [
            prefix + "/original part B.csv",
            prefix + "/original part A.csv",
            prefix + "/quarantined part.csv",
        ]
        for path, row in zip(
            paths,
            [
                (1, "12.3456", 20261103),
                (2, "98.7654", 20261103),
                (3, "500.0000", 20261103),
            ],
            strict=True,
        ):
            physical = Path(self.storage.name) / path
            physical.parent.mkdir(parents=True, exist_ok=True)
            with physical.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["id", "amount", "event_date"])
                writer.writerow(row)
        payload = {
            "source_code": "bas",
            "lakehouse": "spark_catalog",
            "silver_schema": "slv_bas",
            "quarantine_schema": "quarantine_bas",
            "bronze_root": "Files/bronze/bas_finance_sftp",
            "contract_hash": hashlib.sha256(b"local-source-contract").hexdigest(),
            "seed_snapshot_date": "2026-11-03",
            "tables": [table],
            "metadata_version": "local-file-version",
            "metadata_run_id": run_id,
            "source_feed_id": feed_id,
            "source_batch_id": batch,
            "source_type": "sftp",
            "description_slug": "finance",
            "observed_utc": "2026-11-03T08:00:00Z",
            "processing_date": "2026-11-03",
            "pipeline_run_id": batch,
            "raw_source_files": {table_name: paths},
            "raw_source_sizes": {
                table_name: [
                    (Path(self.storage.name) / path).stat().st_size for path in paths
                ]
            },
            "raw_targets": {table_name: prefix},
            "source_selection_complete": True,
            "required_source_tables": [table_name],
            "selected_tables": [table_name],
            "bronze_receipt_sha256": "d" * 64,
            "source_rule_specs": [
                {
                    "id": "local-source-checks",
                    "handler": "legacy.source_business_checks",
                    "version": "1",
                    "severity": "error",
                    "action": "fail",
                    "parameters": {"scope": "local-file-fixture"},
                }
            ],
        }
        table["runtime_layout"] = {
            "layout": {
                "layout_strategy": "partitioned",
                "partition_columns": ["event_date"],
                "cluster_by_columns": [],
            },
            "capabilities": None,
        }
        context = silver.manifest_processing_context(
            gold._gold_json(payload),
            gold._gold_digest(payload),
            expected_lakehouse="spark_catalog",
        )
        config = deepcopy(self.config)
        config.update(
            target_lakehouse="spark_catalog",
            schema=schema,
            sources=[
                {
                    "source_code": "bas",
                    "source_instance": "local_demo_bas",
                    "source_feed_id": feed_id,
                    "source_type": "sftp",
                    "description_slug": "finance",
                    "lakehouse": "spark_catalog",
                    "silver_schema": "slv_bas",
                    "quarantine_schema": "quarantine_bas",
                    "bronze_root": payload["bronze_root"],
                    "contract_hash": payload["contract_hash"],
                    "synthetic_only": True,
                    "tables": [table],
                }
            ],
            dimensions=[
                {
                    "name": "dim_file_sample",
                    "source": "bas",
                    "source_instance": "local_demo_bas",
                    "table": table_name,
                    "keys": ["id"],
                    "attributes": ["amount"],
                }
            ],
            supplier_crosswalk=[],
            facts=[
                {
                    "name": name,
                    "source": "bas",
                    "table": table_name,
                    "keys": ["id"],
                    "measures": ["amount"],
                    "attributes": ["id"],
                    "date_field": "event_date",
                    "period_field": "",
                    "snapshot": snapshot,
                }
                for name, snapshot in (
                    ("fact_file_event", False),
                    ("fact_file_position", True),
                )
            ],
        )

        def physical(path):
            return (
                Path(self.storage.name) / path
                if path.startswith("Files/")
                else Path(path)
            )

        fs = SimpleNamespace(
            exists=lambda path: physical(path).exists(),
            head=lambda path, limit: physical(path).read_text(encoding="utf-8")[:limit],
            ls=lambda path: [
                SimpleNamespace(name=entry.name, size=entry.stat().st_size)
                for entry in physical(path).iterdir()
            ],
        )
        base = (Path(self.storage.name) / "Files/bronze").as_posix()
        source_reader = silver.read_raw

        def rooted_read(spark, filesystem, current, spec):
            # Local equivalent of a Fabric source-Lakehouse default storage root.
            if current["raw_source_files"][spec["name"]][0].startswith("Files/"):
                filesystem, current = gold._gold_bronze_reader(
                    filesystem, current, base
                )
            return source_reader(spark, filesystem, current, spec)

        source_exists = self.spark.catalog.databaseExists("slv_bas")
        quarantine_exists = self.spark.catalog.databaseExists("quarantine_bas")
        silver.setup_namespaces(self.spark, context)
        try:
            with patch.object(silver, "read_raw", side_effect=rooted_read):
                silver.inventory(self.spark, fs, context)
                silver.load_silver(self.spark, fs, context)
                with patch.object(silver, "source_business_checks", return_value=[]):
                    silver.validate_source(self.spark, fs, context)
            runs = json.dumps(
                {"bas": {"pipeline_run_id": batch, "processing_date": "2026-11-03"}}
            )
            preparation = gold.prepare_gold(
                self.spark,
                fs,
                config,
                runs,
                "mf-release-" + suffix,
                silver,
                base,
                source_contexts={
                    "bas": {
                        "context_json": gold._gold_json(payload),
                        "sha256": gold._gold_digest(payload),
                    }
                },
            )
            inputs = gold.load_gold_preparation(self.spark, config, preparation)
            self.assertEqual(inputs["_sealed"]["bas." + table_name]["source_rows"], 3)
            self.assertEqual(
                json.loads(inputs["_contexts"]["bas"]["context_json"])[
                    "raw_source_files"
                ][table_name],
                paths,
            )
            results = {}
            pending = set(inputs["_tasks"])
            while pending:
                ready = [
                    name
                    for name in pending
                    if set(inputs["_tasks"][name]["dependencies"]) <= set(results)
                ]
                self.assertTrue(ready, "The local object DAG must be acyclic")
                for name in ready:
                    task = inputs["_tasks"][name]
                    results[name] = gold.build_gold_object(
                        self.spark,
                        fs,
                        config,
                        preparation,
                        name,
                        silver,
                        attempt_id="local-file-attempt",
                        fence=1,
                        job_id="local-file-job",
                        code_sha256=config["runtime_sha256"],
                        dependency_results={
                            parent: results[parent] for parent in task["dependencies"]
                        },
                    )
                    pending.remove(name)
            outputs = gold.finalize_gold(self.spark, config, preparation, results)
            self.assertEqual(outputs["dim_file_sample"]["row_count"], 3)
            self.assertEqual(outputs["fact_file_event"]["row_count"], 2)
            self.assertEqual(outputs["fact_file_position"]["row_count"], 2)
            total = (
                self.spark.table(f"{schema}.fact_file_event")
                .selectExpr("SUM(amount)")
                .first()[0]
            )
            self.assertEqual(str(total), "111.1110")
            for name in ("fact_file_event", "fact_file_position"):
                self.assertEqual(
                    self.spark.table(f"{schema}.{name}")
                    .filter("dim_file_sample_sk = 0")
                    .count(),
                    0,
                )
            with self.assertRaisesRegex(RuntimeError, "prepared attempt/fence/job"):
                gold.gold_result_references(
                    self.spark,
                    config,
                    preparation,
                    {
                        "dim_file_sample": {
                            "attempt_id": "local-file-attempt",
                            "fence": 2,
                            "job_id": "local-file-job",
                        }
                    },
                )
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                gold.load_gold_result(
                    self.spark,
                    config,
                    preparation,
                    "dim_file_sample",
                    results["dim_file_sample"] | {"fence": 2},
                )
            changed = Path(self.storage.name) / paths[0]
            changed.write_bytes(changed.read_bytes().replace(b"12.3456", b"88.8888"))
            with self.assertRaisesRegex(
                RuntimeError, "immutable Bronze content changed"
            ):
                gold.prepare_gold(
                    self.spark,
                    fs,
                    config,
                    runs,
                    "mf-release-" + suffix,
                    silver,
                    base,
                    source_contexts={
                        "bas": {
                            "context_json": gold._gold_json(payload),
                            "sha256": gold._gold_digest(payload),
                        }
                    },
                )
        finally:
            self.spark.sql(f"DROP TABLE IF EXISTS slv_bas.{table_name}")
            self.spark.sql(f"DROP TABLE IF EXISTS quarantine_bas.{table_name}")
            if not source_exists:
                self.spark.sql("DROP SCHEMA IF EXISTS slv_bas")
            if not quarantine_exists:
                self.spark.sql("DROP SCHEMA IF EXISTS quarantine_bas")
            self.spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

    def test_delta_release_fact_correction_and_snapshot_membership(self) -> None:
        from pyspark.sql import functions as F

        suffix = uuid.uuid4().hex[:10]
        source_schema, target_schema = f"slv_{suffix}", f"gld_{suffix}"
        context = {
            "source_code": "bas",
            "source_instance": "demo_bas",
            "lakehouse": "spark_catalog",
            "silver_schema": source_schema,
            "quarantine_schema": f"dq_{suffix}",
            "bronze_root": Path(self.storage.name).as_posix() + "/bas",
            "contract_hash": "source-contract",
            "synthetic_only": True,
            "processing_date": date(2026, 11, 3),
            "pipeline_run_id": f"source_{suffix}_1",
            "batch_started_utc": datetime(2026, 11, 3, 8, tzinfo=UTC),
        }
        table = {
            "name": f"sample_{suffix}",
            "keys": ["id"],
            "foreign_keys": [],
            "allow_empty": True,
            "change_hash_exclusions": [],
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {
                    "name": "amount",
                    "data_type": "decimal",
                    "precision": 18,
                    "scale": 2,
                    "nullable": False,
                },
                {
                    "name": "event_date",
                    "data_type": "int",
                    "nullable": False,
                    "format": "YYYYMMDD",
                },
            ],
        }
        context["tables"] = [table]
        config = deepcopy(self.config)
        config.update(
            target_lakehouse="spark_catalog",
            schema=target_schema,
            sources=[
                {
                    k: v
                    for k, v in context.items()
                    if k
                    not in {"processing_date", "pipeline_run_id", "batch_started_utc"}
                }
            ],
            dimensions=[
                {
                    "name": "dim_sample",
                    "source": "bas",
                    "source_instance": "demo_bas",
                    "table": table["name"],
                    "keys": ["id"],
                    "attributes": ["amount"],
                }
            ],
            supplier_crosswalk=[],
            facts=[
                {
                    "name": "fact_sample",
                    "source": "bas",
                    "table": table["name"],
                    "keys": ["id"],
                    "measures": ["amount"],
                    "attributes": ["id"],
                    "date_field": "event_date",
                    "period_field": "",
                    "snapshot": False,
                },
                {
                    "name": "fact_position",
                    "source": "bas",
                    "table": table["name"],
                    "keys": ["id"],
                    "measures": ["amount"],
                    "attributes": ["id"],
                    "date_field": "event_date",
                    "period_field": "",
                    "snapshot": True,
                },
            ],
        )
        fs = SimpleNamespace(
            exists=lambda p: Path(p).exists(),
            head=lambda p, n: Path(p).read_text(encoding="utf-8")[:n],
        )
        silver.setup_namespaces(self.spark, context)
        try:

            def source_run(rows):
                path = Path(silver.source_path(context, table))
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(["id", "amount", "event_date"])
                    writer.writerows(rows)
                silver.inventory(self.spark, fs, context)
                silver.load_silver(self.spark, fs, context)
                silver._audit(
                    self.spark,
                    context,
                    "bronze_silver_validation",
                    [
                        {
                            "table_name": "",
                            "check_name": "source_batch_committed",
                            "failed_count": 0,
                            "status": "SUCCESS",
                        }
                    ],
                )
                return json.dumps(
                    {
                        "bas": {
                            "pipeline_run_id": context["pipeline_run_id"],
                            "processing_date": "2026-11-03",
                        }
                    }
                )

            runs = source_run([(1, "10.00", 20261103), (2, "20.00", 20261103)])
            base = Path(self.storage.name).as_posix()
            first = gold.run_gold(
                self.spark, fs, config, runs, f"gold_{suffix}_1", silver, base
            )
            self.assertEqual(first["fact_sample"]["row_count"], 2)
            self.assertEqual(first["dim_sample"]["row_count"], 3)
            self.assertEqual(
                self.spark.table(f"{target_schema}.dim_sample")
                .filter("dimension_sk = 0")
                .count(),
                1,
            )
            original_event = (
                self.spark.table(f"{target_schema}.fact_sample")
                .filter("id = 1")
                .first()
                .business_event_id
            )
            self.assertEqual(
                first,
                gold.run_gold(
                    self.spark, fs, config, runs, f"gold_{suffix}_1", silver, base
                ),
            )
            context.update(
                pipeline_run_id=f"source_{suffix}_2",
                batch_started_utc=datetime(2026, 11, 3, 9, tzinfo=UTC),
            )
            runs = source_run([(1, "15.00", 20261103)])
            release = f"gold_{suffix}_2"
            preparation = gold.prepare_gold(
                self.spark, fs, config, runs, release, silver, base
            )
            pinned = gold.load_gold_preparation(self.spark, config, preparation)
            source_version = pinned[f"spark_catalog.{source_schema}.{table['name']}"][
                "version"
            ]
            context.update(
                pipeline_run_id=f"source_{suffix}_3",
                batch_started_utc=datetime(2026, 11, 3, 10, tzinfo=UTC),
            )
            source_run([(1, "999.00", 20261103)])
            self.assertGreater(
                silver.delta_version(
                    self.spark, f"spark_catalog.{source_schema}.{table['name']}"
                ),
                source_version,
            )

            def build(name, attempt, dependencies=None):
                return gold.build_gold_object(
                    self.spark,
                    fs,
                    config,
                    preparation,
                    name,
                    silver,
                    attempt_id=attempt,
                    fence=2,
                    job_id="local-gold-job",
                    code_sha256=config["runtime_sha256"],
                    dependency_results=dependencies,
                )

            results = {}
            results["dim_sample"] = build("dim_sample", "dimension-attempt")
            dimension_ref = {"dim_sample": results["dim_sample"]}
            results["fact_sample"] = build(
                "fact_sample", "event-attempt", dimension_ref
            )
            actual_merge = gold._merge_gold

            def fail_position(spark, target, frame, key, **kwargs):
                if target.endswith(".fact_position"):
                    raise RuntimeError("injected failure before write")
                return actual_merge(spark, target, frame, key, **kwargs)

            with (
                patch.object(gold, "_merge_gold", side_effect=fail_position),
                self.assertRaisesRegex(RuntimeError, "injected failure"),
            ):
                build("fact_position", "failed-position-attempt", dimension_ref)
            with self.assertRaisesRegex(RuntimeError, "Partial"):
                gold.finalize_gold(self.spark, config, preparation, results)
            self.assertIsNone(gold._ready_release(self.spark, config, release))
            with self.assertRaisesRegex(RuntimeError, "cannot be rebound"):
                gold.prepare_gold(
                    self.spark,
                    fs,
                    config | {"runtime_sha256": "0" * 64},
                    runs,
                    release,
                    silver,
                    base,
                )
            preparation = gold.prepare_gold(
                self.spark, fs, config, runs, release, silver, base
            )
            self.assertEqual(
                gold.load_gold_preparation(self.spark, config, preparation)[
                    f"spark_catalog.{source_schema}.{table['name']}"
                ]["version"],
                source_version,
            )
            self.assertEqual(
                results["dim_sample"]["result_sha256"],
                gold.gold_result_references(
                    self.spark, config, preparation, {"dim_sample": "dimension-attempt"}
                )["dim_sample"]["result_sha256"],
            )
            with self.assertRaisesRegex(RuntimeError, "new-attempt recovery"):
                build("fact_position", "failed-position-attempt", dimension_ref)
            results["fact_position"] = build(
                "fact_position", "position-retry", dimension_ref
            )
            facts = {
                name: ref for name, ref in results.items() if name.startswith("fact_")
            }
            results["dim_date"] = build("dim_date", "date-attempt", facts)
            results["dim_fiscal_period"] = build(
                "dim_fiscal_period", "period-attempt", facts
            )
            second = gold.finalize_gold(self.spark, config, preparation, results)
            self.assertEqual(second["fact_sample"]["row_count"], 2)
            self.assertEqual(second["fact_position"]["row_count"], 1)
            amounts = (
                self.spark.table(f"{target_schema}.fact_sample")
                .agg(F.sum("amount"))
                .first()[0]
            )
            self.assertEqual(str(amounts), "35.0000")
            position = self.spark.table(f"{target_schema}.fact_position").first()
            self.assertEqual(position.id, 1)
            self.assertEqual(str(position.amount), "15.0000")
            self.assertEqual(
                self.spark.table(f"{target_schema}.fact_sample")
                .filter("id = 1")
                .first()
                .business_event_id,
                original_event,
            )
            self.assertEqual(
                str(
                    self.spark.table(f"{target_schema}.dim_sample")
                    .filter("id = 1 AND is_current")
                    .first()
                    .amount
                ),
                "15.00",
            )
            self.assertEqual(
                second,
                gold.finalize_gold(self.spark, config, preparation, results),
            )
            invalid = json.dumps(
                {
                    "bas": {
                        "pipeline_run_id": "not-committed",
                        "processing_date": "2026-11-03",
                    }
                }
            )
            with self.assertRaisesRegex(RuntimeError, "not committed"):
                gold.run_gold(
                    self.spark, fs, config, invalid, f"failed_{suffix}", silver, base
                )
            gold._gold_delta(
                self.spark, f"spark_catalog.{target_schema}.fact_sample"
            ).update(
                condition="id = 1", set={"amount": "amount + CAST(1 AS DECIMAL(28,4))"}
            )
            with self.assertRaisesRegex(RuntimeError, "output drift"):
                gold.finalize_gold(self.spark, config, preparation, results)
            self.assertIsNone(gold._ready_release(self.spark, config, release))
        finally:
            for schema in (source_schema, context["quarantine_schema"], target_schema):
                self.spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


if __name__ == "__main__":
    unittest.main()
