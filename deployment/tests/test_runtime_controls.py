"""Pure notebook scope guards; no Spark, cloud credentials or remote writes."""

from __future__ import annotations

import importlib.util
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

from fabric_medallion_deployer.contracts import load_sources
from fabric_medallion_deployer.use_cases import load_case_extension, load_use_case

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "treasury_ingestion", ROOT / "artefacts" / "medallion" / "ingestion.py"
)
runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime)


def case_rule_handlers(use_case: str) -> dict:
    spec = importlib.util.spec_from_file_location(
        "runtime_control_rules", ROOT / "artefacts/medallion/rules.py"
    )
    rules = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rules)
    rules._CASE_RULES = load_case_extension(
        load_use_case(ROOT, use_case), "source_rules"
    )
    return rules._MEDALLION_RULE_HANDLERS


class RuntimeControlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = load_sources(ROOT, use_case="treasury")[2]
        runtime._MEDALLION_RULE_HANDLERS = case_rule_handlers("treasury")

    def test_processing_date_and_source_filename(self) -> None:
        config = self.source.runtime_config()
        context = runtime.processing_context(config, "2026-11-03", "pipeline-test")
        self.assertEqual(context["processing_date"], date(2026, 11, 3))
        table = next(t for t in self.source.tables if t["name"] == "t_employee")
        self.assertEqual(
            runtime.source_path(context, table),
            "Files/bronze/persal/20261103/pipeline-test/t_employee.csv",
        )
        self.assertEqual(
            runtime.table_path(context, table), "lh_brz_slv_treasury.slv_psl.t_employee"
        )
        self.assertEqual(config["seed_snapshot_date"], "2026-11-02")
        self.assertNotIn("history_schema", config)
        self.assertFalse(hasattr(runtime, "merge_current"))
        self.assertFalse(hasattr(runtime, "history_path"))
        self.assertEqual(
            table["change_hash_exclusions"], ["extract_date", "source_filename"]
        )

    def test_batch_timestamp_and_same_day_paths_are_explicit(self) -> None:
        first = runtime.processing_context(
            self.source.runtime_config(),
            "2026-11-03",
            "first",
            "2026-11-03T08:30:00.123Z",
        )
        second = runtime.processing_context(
            self.source.runtime_config(),
            "2026-11-03",
            "second",
            "2026-11-03T09:30:00.123Z",
        )
        self.assertEqual(
            first["batch_started_utc"], datetime(2026, 11, 3, 8, 30, 0, 123000, UTC)
        )
        self.assertNotEqual(
            runtime.source_path(first, self.source.tables[0]),
            runtime.source_path(second, self.source.tables[0]),
        )
        for value in ("2026-11-03", "2026-11-03T08:30:00+02:00", "../batch"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                runtime.processing_context(
                    self.source.runtime_config(), "2026-11-03", "run", value
                )

    def test_scope_and_invalid_date_fail_explicitly(self) -> None:
        for change in (
            {"bronze_root": "Files/bronze/bas"},
            {"silver_schema": "slv_persal"},
            {"lakehouse": "other"},
            {"quarantine_schema": "audit"},
            {"source_code": "other"},
            {"history_schema": "slv_psl"},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                runtime.processing_context(
                    self.source.runtime_config() | change, "2026-11-03", "run"
                )
        for value in ("20261103", "2026-02-30", "../2026-11-03"):
            with self.subTest(date=value), self.assertRaises(ValueError):
                runtime.processing_context(self.source.runtime_config(), value, "run")

    def test_cleanup_requires_explicit_date_and_correct_attachment(self) -> None:
        with self.assertRaisesRegex(ValueError, "explicit"):
            runtime.run_step(None, None, self.source.runtime_config(), "cleanup")
        utilities = SimpleNamespace(
            runtime=SimpleNamespace(context={"defaultLakehouseName": "other"})
        )
        with self.assertRaisesRegex(RuntimeError, "default Lakehouse"):
            runtime.run_step(
                None,
                utilities,
                self.source.runtime_config(),
                "inventory",
                processing_date="2026-11-03",
                pipeline_run_id="run",
            )


if __name__ == "__main__":
    unittest.main()
