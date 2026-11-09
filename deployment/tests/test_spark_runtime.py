"""Executable Spark tests, enabled explicitly with TREASURY_SPARK_TESTS=1."""

from __future__ import annotations

import csv
import importlib.util
import os
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "treasury_ingestion", ROOT / "artefacts" / "medallion" / "ingestion.py"
)


@unittest.skipUnless(
    os.environ.get("TREASURY_SPARK_TESTS") == "1",
    "Opt-in Spark tier: set TREASURY_SPARK_TESTS=1",
)
class SparkRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("treasury-local-contracts")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.runtime = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(cls.runtime)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_copy_reconciliation_seals_real_spark_rows_before_sql_callbacks(self):
        import io
        import json

        import test_pipeline_reconciliation as reconciliation

        fixture = reconciliation.PipelineReconciliationTests(
            "test_activity_evidence_is_terminal_exact_and_metadata_only"
        )
        fixture.setUpClass()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.fs.put(
            fixture.task["target_path"], "\ufeffid,amount\n1,123.4500\n2,-12.3400\n"
        )
        fs = fixture.fs
        original_files = self.runtime.source_files

        def local_paths(context, table):
            return tuple(
                (fs.root / path).as_posix() for path in original_files(context, table)
            )

        class Client:
            def request(_self, method, location, *, body, expected):
                return SimpleNamespace(
                    content=json.dumps([fixture.activity()]).encode("utf-8")
                )

        failures = []

        class LocalIngestion:
            def read_raw(_self, spark, fs_adapter, context, table):
                context = dict(context)
                context["raw_source_files"] = {
                    table["name"]: list(local_paths(context, table))
                }
                adapter = SimpleNamespace(
                    exists=lambda path: Path(path).exists(),
                    head=lambda path, count: (
                        Path(path).read_bytes()[:count].decode("utf-8")
                    ),
                    ls=lambda path: [
                        SimpleNamespace(name=file.name, size=file.stat().st_size)
                        for file in Path(path).iterdir()
                        if file.is_file()
                    ],
                )
                try:
                    return self.runtime.read_raw(spark, adapter, context, table)
                except self.runtime._operation_errors() as error:
                    failures.append(error)
                    raise

            snapshot_fingerprint = staticmethod(self.runtime.snapshot_fingerprint)
            _operation_errors = staticmethod(self.runtime._operation_errors)

        result = fixture.runtime.reconcile_bronze_pipeline(
            self.spark,
            fs,
            fixture.manifest,
            fixture.dispatch(),
            client=Client(),
            observed_utc="2026-11-06T09:01:00Z",
            ingestion=LocalIngestion(),
        )
        if failures:
            raise failures[0]
        self.assertFalse(result["has_failed"])
        row = next(
            csv.DictReader(
                io.StringIO(fs.head(result["callbacks"][0]["path"], 10000)),
                escapechar="\\",
            )
        )
        value = json.loads(row["result_json"])
        self.assertEqual(
            value["metrics"],
            {
                "rows_read": 2,
                "rows_written": 2,
                "rows_quarantined": 0,
            },
        )
        self.assertEqual(value["status"], "SUCCEEDED")
        self.assertEqual(len(value["artifact_refs"][0]["sha256"]), 64)
        fixture.runtime.verify_pipeline_callbacks(
            fs,
            fixture.manifest,
            result["callbacks"][0]["path"],
            result["callbacks"][0]["sha256"],
        )

    def test_manifest_multifile_csv_preserves_values_filenames_and_strict_headers(
        self,
    ) -> None:
        from pyspark.sql import functions as F

        table = {
            "name": "manifest_sample",
            "keys": ["id"],
            "foreign_keys": [],
            "allow_empty": False,
            "change_hash_exclusions": [],
            "columns": [
                {
                    "name": "id",
                    "data_type": "varchar",
                    "nullable": False,
                    "max_length": 20,
                },
                {"name": "sequence", "data_type": "bigint", "nullable": False},
                {
                    "name": "amount",
                    "data_type": "decimal",
                    "nullable": False,
                    "precision": 28,
                    "scale": 4,
                },
                {
                    "name": "note",
                    "data_type": "varchar",
                    "nullable": True,
                    "max_length": 200,
                },
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / "part_01.csv", Path(directory) / "part_02.csv"]
            for index, path in enumerate(paths):
                with path.open(
                    "w", encoding="utf-8-sig" if index == 0 else "utf-8", newline=""
                ) as stream:
                    writer = csv.writer(stream, escapechar="\\")
                    writer.writerow([column["name"] for column in table["columns"]])
                    writer.writerow(
                        (
                            f"0000{index + 1}",
                            "9007199254740993",
                            "12345.6789" if index == 0 else "-12.3400",
                            "first\nline" if index == 0 else "second",
                        )
                    )
            fs = SimpleNamespace(
                exists=lambda path: Path(path).is_file(),
                head=lambda path, count: (
                    Path(path).read_bytes()[:count].decode("utf-8")
                ),
                ls=lambda path: [
                    SimpleNamespace(name=p.name, size=p.stat().st_size)
                    for p in Path(path).iterdir()
                    if p.is_file()
                ],
            )
            context = {
                "metadata_mode": "manifest_v1",
                "raw_source_files": {
                    table["name"]: [path.as_posix() for path in paths]
                },
                "raw_source_sizes": {
                    table["name"]: [path.stat().st_size for path in paths]
                },
                "raw_targets": {table["name"]: Path(directory).as_posix()},
                "pipeline_run_id": "source-batch",
                "source_code": "bas",
                "processing_date": date(2026, 11, 6),
                "contract_hash": "manifest-test",
            }
            raw = self.runtime.read_raw(self.spark, fs, context, table).cache()
            try:
                before = self.runtime.snapshot_fingerprint(raw)
                changed_path = raw.withColumn(
                    "_source_file_path", F.lit("different transport path")
                )
                self.assertEqual(
                    before, self.runtime.snapshot_fingerprint(changed_path)
                )
                typed = (
                    self.runtime._with_lineage(
                        self.runtime.prepare_rows(raw, table), context, table
                    )
                    .orderBy("id")
                    .collect()
                )
                self.assertEqual([row["id"] for row in typed], ["00001", "00002"])
                self.assertEqual(
                    [row["sequence"] for row in typed], [9007199254740993] * 2
                )
                self.assertEqual(
                    [str(row["amount"]) for row in typed], ["12345.6789", "-12.3400"]
                )
                self.assertEqual(typed[0]["note"], "first\nline")
                self.assertEqual(
                    [row["ingest_source_file_name"] for row in typed],
                    ["part_01.csv", "part_02.csv"],
                )
                self.assertTrue(all(row["dq_passed"] for row in typed))
            finally:
                raw.unpersist()
            paths[1].write_text("id,sequence,WRONG,note\n", encoding="utf-8")
            context["raw_source_sizes"][table["name"]][1] = paths[1].stat().st_size
            with self.assertRaisesRegex(ValueError, "header/order"):
                self.runtime.read_raw(self.spark, fs, context, table)

    def test_explicit_hash_order_survives_projection_reordering_without_false_changes(
        self,
    ) -> None:
        from copy import deepcopy

        table = {
            "name": "ordered_hash",
            "keys": ["id"],
            "foreign_keys": [],
            "change_hash_exclusions": ["extract_date"],
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {"name": "amount", "data_type": "money", "nullable": False},
                {"name": "extract_date", "data_type": "datetime", "nullable": False},
            ],
            "hash_policy": {
                "key_columns": ["id"],
                "value_columns": ["id", "amount"],
                "encoding": "spark_json_struct_sha256_v1",
            },
        }
        raw = self.spark.createDataFrame(
            [("1", "12345.6789", "2026-11-06T00:00:00.000")],
            ["id", "amount", "extract_date"],
        )
        typed = self.runtime.prepare_rows(raw, table)
        first = self.runtime.with_record_hashes(typed, table).first()
        reordered = deepcopy(table)
        reordered["columns"].reverse()
        second = self.runtime.with_record_hashes(typed, reordered).first()
        self.assertEqual(first["_record_key_hash"], second["_record_key_hash"])
        self.assertEqual(first["_record_hash"], second["_record_hash"])
        reordered["hash_policy"]["value_columns"] = ["amount"]
        with self.assertRaisesRegex(ValueError, "hash policy"):
            self.runtime.with_record_hashes(typed, reordered)

    def test_metadata_rule_actions_preserve_warnings_and_block_errors(self) -> None:
        baseline = [
            {
                "id": f"baseline-{name}",
                "handler": f"legacy.{name}",
                "version": "1",
                "severity": "error",
                "action": "fail",
                "parameters": {"scope": "object"},
            }
            for name in ("prepare_rows", "relationship_checks", "silver_checks")
        ]
        table = {
            "name": "rule_sample",
            "keys": ["id"],
            "foreign_keys": [],
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {"name": "amount", "data_type": "money", "nullable": False},
            ],
            "rule_specs": baseline
            + [
                {
                    "id": "negative-warning",
                    "handler": "value_range",
                    "version": "1",
                    "severity": "warning",
                    "action": "warn",
                    "parameters": {"column": "amount", "minimum": 0},
                },
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
            ],
        }
        raw = self.spark.createDataFrame(
            [("1", "-1.0000"), ("2", "500.0000"), ("3", "-101.0000")],
            ["id", "amount"],
        )
        context = {
            "source_code": "bas",
            "pipeline_run_id": "source-batch",
            "processing_date": date(2026, 11, 6),
            "contract_hash": "rule-test",
            "bronze_root": "Files/bronze/bas",
        }
        rows = (
            self.runtime._with_lineage(
                self.runtime.prepare_rows(raw, table), context, table
            )
            .orderBy("id")
            .collect()
        )
        self.assertTrue(rows[0]["dq_passed"])
        self.assertEqual(rows[0]["dq_warning"], "negative-warning@1")
        self.assertFalse(rows[1]["dq_passed"])
        self.assertIn("too-large@1", rows[1]["dq_failure_reason"])
        self.assertIn("too-negative@1", rows[2]["dq_failure_reason"])

        from pyspark.sql import functions as F

        typed = self.runtime.prepare_rows(raw, table)
        blocking = {
            row["id"]
            for row in typed.filter(
                F.size(self.runtime.blocking_dq_reasons(table)) > 0
            ).collect()
        }
        self.assertEqual(blocking, {3})

    def test_preserves_source_extract_date_and_exact_money(self) -> None:
        table = {
            "name": "t_employee",
            "keys": ["employee_id"],
            "foreign_keys": [],
            "columns": [
                {"name": "employee_id", "data_type": "bigint", "nullable": False},
                {"name": "extract_date", "data_type": "datetime", "nullable": False},
                {"name": "gross_salary", "data_type": "money", "nullable": False},
            ],
        }
        raw = self.spark.createDataFrame(
            [("9007199254740993", "2026-11-02T00:00:00.000", "12345.6789")],
            ["employee_id", "extract_date", "gross_salary"],
        )
        out = self.runtime.prepare_rows(raw, table)
        row = out.first()
        self.assertEqual(row["employee_id"], 9007199254740993)
        self.assertEqual(str(row["gross_salary"]), "12345.6789")
        self.assertEqual(row["extract_date"].isoformat(), "2026-11-02T00:00:00")
        self.assertEqual(row["_dq_reasons"], [])
        dates = {
            "name": "dates",
            "keys": ["id"],
            "foreign_keys": [],
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {
                    "name": "event_date",
                    "data_type": "varchar",
                    "nullable": False,
                    "format": "YYYYMMDD",
                },
                {
                    "name": "event_month",
                    "data_type": "varchar",
                    "nullable": False,
                    "format": "YYYYMM",
                },
                {"name": "when_seen", "data_type": "datetime", "nullable": False},
            ],
        }
        invalid = self.spark.createDataFrame(
            [
                ("1", "00000101", "000001", "0000-01-01T00:00:00.000"),
                ("2", "20260230", "202613", "2026-02-30T00:00:00.000"),
            ],
            ["id", "event_date", "event_month", "when_seen"],
        )
        for result in (
            self.runtime.prepare_rows(invalid, dates).select("_dq_reasons").collect()
        ):
            self.assertEqual(
                set(result["_dq_reasons"]),
                {
                    "INVALID_VALUE:event_date",
                    "INVALID_VALUE:event_month",
                    "INVALID_VALUE:when_seen",
                },
            )

    def test_duplicate_invalid_and_overprecise_rows_have_reasons(self) -> None:
        table = {
            "name": "money_rows",
            "keys": ["id"],
            "foreign_keys": [],
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {
                    "name": "amount",
                    "data_type": "decimal",
                    "nullable": False,
                    "precision": 5,
                    "scale": 2,
                },
            ],
        }
        raw = self.spark.createDataFrame(
            [("1", "12.34"), ("1", "12.35"), ("x", "1.234"), ("2", "9999.99")],
            ["id", "amount"],
        )
        rows = self.runtime.prepare_rows(raw, table).collect()
        self.assertTrue(all(r["_dq_reasons"] for r in rows))
        self.assertTrue(any("DUPLICATE_BUSINESS_KEY" in r["_dq_reasons"] for r in rows))
        self.assertTrue(any("INVALID_VALUE:amount" in r["_dq_reasons"] for r in rows))

    def test_every_source_field_can_be_parsed_without_quarantine(self) -> None:
        from fabric_medallion_deployer.contracts import load_sources
        from pyspark.sql import types as T

        for source in load_sources(ROOT):
            for table in source.tables:
                with self.subTest(source=source.code, table=table["name"]):
                    with source.seed_file(table["name"]).open(
                        encoding="utf-8", newline=""
                    ) as stream:
                        reader = csv.DictReader(stream)
                        rows = [row for _, row in zip(range(3), reader)]
                    schema = T.StructType(
                        [
                            T.StructField(c["name"], T.StringType(), True)
                            for c in table["columns"]
                        ]
                    )
                    raw = self.spark.createDataFrame(rows, schema=schema)
                    result = (
                        self.runtime.prepare_rows(raw, table)
                        .select("_dq_reasons")
                        .collect()
                    )
                    self.assertTrue(
                        all(not row["_dq_reasons"] for row in result),
                        [row["_dq_reasons"] for row in result],
                    )

    def test_csv_structural_errors_and_valid_empty_file(self) -> None:
        table = {
            "name": "sample",
            "keys": ["id"],
            "foreign_keys": [],
            "allow_empty": True,
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {
                    "name": "amount",
                    "data_type": "decimal",
                    "nullable": True,
                    "precision": 5,
                    "scale": 2,
                },
            ],
        }
        with tempfile.TemporaryDirectory() as folder:
            context = {
                "bronze_root": Path(folder).as_posix(),
                "processing_date": date(2026, 11, 3),
                "pipeline_run_id": "csv-test",
            }
            file = Path(self.runtime.source_path(context, table))
            file.parent.mkdir(parents=True)
            fs = SimpleNamespace(
                exists=lambda p: Path(p).exists(),
                head=lambda p, n: Path(p).read_text(encoding="utf-8")[:n],
            )
            file.write_text("id,amount\n1,12.34,unexpected\n", encoding="utf-8")
            raw = self.runtime.read_raw(self.spark, fs, context, table)
            row = self.runtime.prepare_rows(raw, table).first()
            self.assertIn("MALFORMED_CSV_RECORD", row["_dq_reasons"])
            file.write_text("id,amount\n", encoding="utf-8")
            raw = self.runtime.read_raw(self.spark, fs, context, table)
            self.assertEqual(self.runtime.prepare_rows(raw, table).count(), 0)
            for prefix in ("", "\ufeff"):
                for header in ("id,amount\n", '"id","amount"\r\n'):
                    with self.subTest(bom=bool(prefix), header=header):
                        file.write_text(
                            prefix + header + '"1","12.34"\n', encoding="utf-8"
                        )
                        raw = self.runtime.read_raw(self.spark, fs, context, table)
                        row = self.runtime.prepare_rows(raw, table).first()
                        self.assertEqual(row["id"], 1)
                        self.assertEqual(str(row["amount"]), "12.34")
                        self.assertEqual(row["_dq_reasons"], [])
            file.write_text("amount,id\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "header/order"):
                self.runtime.read_raw(self.spark, fs, context, table)

    def test_money_range_is_exact_at_four_decimal_places(self) -> None:
        table = {
            "name": "limits",
            "keys": ["id"],
            "foreign_keys": [],
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False},
                {"name": "amount", "data_type": "money", "nullable": False},
            ],
        }
        raw = self.spark.createDataFrame(
            [
                ("1", "922337203685477.5807"),
                ("2", "922337203685477.5808"),
                ("3", "-922337203685477.5808"),
                ("4", "-922337203685477.5809"),
            ],
            ["id", "amount"],
        )
        actual = {
            r["id"]: not r["_dq_reasons"]
            for r in self.runtime.prepare_rows(raw, table).collect()
        }
        self.assertEqual(actual, {1: True, 2: False, 3: True, 4: False})

    def test_csd_funding_checks_use_the_complete_supplier_bank_key(self) -> None:
        from fabric_medallion_deployer.contracts import load_sources
        from pyspark.sql import types as T

        from use_cases.treasury.fixtures.generate import csd_rows
        source = load_sources(ROOT)[1]
        data = {}
        for table in source.tables:
            rows = csd_rows(table, 4, date(2026, 11, 2), 42)
            if table["name"] == "bank_accounts":
                local_key = next(
                    row["BankAccountID"]
                    for row in rows
                    if row["IsForeignBankAccount"] == "false"
                )
                for row in rows:
                    if row["IsForeignBankAccount"] == "true":
                        row["BankAccountID"] = local_key
            schema = T.StructType(
                [
                    T.StructField(c["name"], T.StringType(), True)
                    for c in table["columns"]
                ]
            )
            data[table["name"]] = self.runtime.prepare_rows(
                self.spark.createDataFrame(rows, schema=schema), table
            )
        checks = dict(self.runtime.source_business_checks(data, "csd"))
        self.assertEqual(checks["funding_bank_state"], 0)

    def test_native_source_business_rules_match_synthetic_controls(self) -> None:
        from fabric_medallion_deployer.contracts import load_sources
        from pyspark.sql import types as T

        from use_cases.treasury.fixtures.generate import bas_rows, csd_rows, persal_rows

        builders = {"bas": bas_rows, "csd": csd_rows, "persal": persal_rows}
        for source in load_sources(ROOT):
            data = {}
            try:
                for table in source.tables:
                    schema = T.StructType(
                        [
                            T.StructField(c["name"], T.StringType(), True)
                            for c in table["columns"]
                        ]
                    )
                    rows = builders[source.code](table, 40, date(2026, 11, 2), 42)
                    raw = self.spark.createDataFrame(rows, schema=schema)
                    data[table["name"]] = (
                        self.runtime.prepare_rows(raw, table)
                        .select(*[c["name"] for c in table["columns"]])
                        .cache()
                    )
                checks = self.runtime.source_business_checks(data, source.code)
                self.assertTrue(checks)
                self.assertEqual(
                    [(name, count) for name, count in checks if count], [], source.code
                )
            finally:
                for frame in data.values():
                    frame.unpersist()

    def test_history_hash_ignores_ledger_trailer_changes_but_tracks_amounts(
        self,
    ) -> None:
        from fabric_medallion_deployer.contracts import load_sources
        from pyspark.sql import functions as F

        ledger = load_sources(ROOT)[0].tables[0]
        names = [
            "demo_record_id",
            "debit_amount",
            "total_debit",
            "total_credit",
            "total_transactions",
        ]
        table = {
            **ledger,
            "columns": [c for c in ledger["columns"] if c["name"] in names],
        }
        raw = self.spark.createDataFrame(
            [
                (1, "10.00", "100.00", "100.00", 10),
                (1, "10.00", "200.00", "200.00", 20),
                (1, "20.00", "200.00", "200.00", 20),
            ],
            names,
        )
        frame = raw.select(
            "demo_record_id",
            *[F.col(n).cast("decimal(18,2)").alias(n) for n in names[1:4]],
            "total_transactions",
        )
        hashes = [
            r["_record_hash"]
            for r in self.runtime.with_record_hashes(frame, table)
            .select("_record_hash")
            .collect()
        ]
        self.assertEqual(hashes[0], hashes[1])
        self.assertNotEqual(hashes[1], hashes[2])

    def test_persal_observation_dates_can_advance_independently_of_payroll_period(
        self,
    ) -> None:
        from pyspark.sql import functions as F

        employees = self.spark.createDataFrame(
            [(1, 101, "dept", "person", 1, "S", None, 20261130, 202611)],
            "persal_num int, employee_id int, dept string, surname string, id_num long, "
            "initial string, term_date_id int, pay_date int, post_month int",
        )
        leave = self.spark.createDataFrame(
            [(1, 20261102, 20261104, 3.0, 202611)],
            "persal_num int, leave_start_date int, leave_end_date int, total_days double, post_month int",
        )
        terminated = self.spark.createDataFrame([], "persal_num int, post_month int")
        data = {
            name: frame.withColumn(
                "extract_date", F.lit("2026-12-03T12:00:00.000").cast("timestamp_ntz")
            ).withColumn("source_filename", F.lit(f"{name}_20261203.csv"))
            for name, frame in (
                ("t_employee", employees),
                ("t_leave", leave),
                ("t_termi", terminated),
            )
        }
        checks = self.runtime.source_business_checks(data, "persal")
        self.assertEqual([(name, count) for name, count in checks if count], [])

    def test_retention_is_source_scoped_and_requires_current_validation(self) -> None:
        from fabric_medallion_deployer.contracts import load_sources

        config = load_sources(ROOT)[0].runtime_config()
        config["tables"] = [config["tables"][0]]
        context = self.runtime.processing_context(config, "2026-11-03", "cleanup-test")
        table = config["tables"][0]["name"]
        current = self.runtime.source_path(context, config["tables"][0])
        old = f"Files/bronze/bas/20261102/old/{table}.csv"
        future = f"Files/bronze/bas/20261104/future/{table}.csv"
        unrelated = "Files/bronze/csd/20261101/other/supplier_identification.csv"
        files = {old, current, future, unrelated}
        deleted = []
        fs = SimpleNamespace(
            exists=lambda p: p in files,
            ls=lambda p: [
                SimpleNamespace(path=f, size=1, isDir=False)
                for f in sorted(files)
                if f.startswith(p + "/")
            ],
            rm=lambda p, recursive: (deleted.append(p), files.remove(p)),
        )
        now = datetime.now(UTC)
        validation = self.spark.createDataFrame(
            [("source_ready_for_cleanup", "SUCCESS", now)],
            "check_name string, status string, event_utc timestamp",
        )
        processing = self.spark.createDataFrame(
            [(now - timedelta(seconds=1),)], "event_utc timestamp"
        )
        audit = lambda spark, ctx, name: (
            validation if name == "bronze_silver_validation" else processing
        )

        def verify(spark, files, ctx, spec):
            if not files.exists(self.runtime.source_path(ctx, spec)):
                raise FileNotFoundError("Missing sealed batch")
            return None, None

        with (
            patch.object(self.runtime, "_run_audit", side_effect=audit),
            patch.object(self.runtime, "_audit") as writer,
            patch.object(self.runtime, "verify_batch", side_effect=verify),
            patch.object(self.runtime, "guard_batch_order"),
            patch.object(self.runtime, "silver_checks", return_value=[]),
        ):
            self.runtime.cleanup_snapshots(self.spark, fs, context)
        self.assertEqual(deleted, [])
        self.assertEqual(files, {old, current, future, unrelated})
        self.assertEqual(writer.call_args.args[2], "bronze_cleanup")

        files.remove(current)
        deleted.clear()
        with (
            patch.object(self.runtime, "_run_audit", side_effect=audit),
            patch.object(self.runtime, "_audit"),
            patch.object(self.runtime, "verify_batch", side_effect=verify),
            patch.object(self.runtime, "guard_batch_order"),
            patch.object(self.runtime, "silver_checks", return_value=[]),
            self.assertRaisesRegex(RuntimeError, "no cleanup"),
        ):
            self.runtime.cleanup_snapshots(self.spark, fs, context)
        self.assertEqual(deleted, [])

    def test_foreign_keys_quarantine_all_orphan_children_without_multiplying_rows(
        self,
    ) -> None:
        from pyspark.sql import functions as F

        parent = {
            "name": "parents",
            "columns": [{"name": "id", "data_type": "int", "nullable": False}],
        }
        child = {
            "name": "children",
            "foreign_keys": [
                {"table": "parents", "columns": ["parent_id"], "target_columns": ["id"]}
            ],
        }
        context = {"tables": [parent, child]}
        parent_rows = self.spark.createDataFrame([("1",), ("1",)], ["id"])
        children = self.spark.createDataFrame(
            [("a", 1), ("b", 9)], ["child_id", "parent_id"]
        ).withColumn("_dq_reasons", F.array().cast("array<string>"))
        with patch.object(self.runtime, "read_raw", return_value=parent_rows):
            output = self.runtime.add_foreign_key_rules(
                self.spark, None, context, child, children
            )
        records = output.collect()
        self.assertEqual(len(records), 2)
        rows = {r["child_id"]: r["_dq_reasons"] for r in records}
        self.assertEqual(rows["a"], [])
        self.assertEqual(rows["b"], ["ORPHAN_FOREIGN_KEY:0:parents"])
        typed_parent = parent_rows.select(F.col("id").cast("int").alias("id"))
        proxy = SimpleNamespace(table=lambda name: typed_parent)
        context.update(lakehouse="lh_brz_slv_treasury", silver_schema="slv_csd")
        with patch.object(self.runtime, "latest_silver", return_value=typed_parent):
            self.assertEqual(
                dict(self.runtime.relationship_checks(proxy, context, child, children)),
                {"foreign_key_0_parent_unique": 1, "foreign_key_0_resolved": 1},
            )


if __name__ == "__main__":
    unittest.main()
