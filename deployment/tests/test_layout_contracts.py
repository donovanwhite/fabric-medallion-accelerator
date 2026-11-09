"""Offline physical-layout contracts; no Fabric access or optional dependencies."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "treasury_layout_contracts",
    Path(__file__).resolve().parents[2]
    / "artefacts"
    / "orchestrator"
    / "layout.py",
)
layout = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = layout
SPEC.loader.exec_module(layout)

SCHEMA = (
    layout.Column("id", "bigint", False),
    layout.Column("day", "date"),
    layout.Column("region", "string"),
    layout.Column("_source_run_id", "string", False),
)
PARTITIONED = {"layout_strategy": "partitioned", "partition_columns": ["day"]}
LIQUID = {"layout_strategy": "liquid", "cluster_by_columns": ["id", "region"]}
CAPABILITIES = layout.RuntimeCapabilities(
    engine_id="validated-scratch-runtime",
    spark_version="3.5.7",
    evidence="scratch-run-42: create, append, inspect, alter and optimize verified",
    operations=(
        "liquid_create",
        "liquid_write",
        "liquid_configure",
        "optimize",
        "mlv_liquid",
    ),
    statistics_columns=("id", "day", "region"),
)


class LayoutContractTests(unittest.TestCase):
    def test_notebook_embedding_does_not_require_module_registry_or_spark_imports(self):
        namespace = {"__name__": "layout_unregistered_notebook_module"}
        exec(  # noqa: S102 - Test trusted repository source embedding, never metadata.
            compile(
                Path(SPEC.origin).read_text(encoding="utf-8"),
                SPEC.origin,
                "exec",
                dont_inherit=True,
            ),
            namespace,
        )
        self.assertTrue(callable(namespace["ensure_target_layout"]))
        self.assertNotIn("layout_unregistered_notebook_module", sys.modules)

    def test_one_selector_and_ordered_active_columns(self):
        partitioned = layout.validate_layout(PARTITIONED, SCHEMA)
        liquid = layout.validate_layout(LIQUID, SCHEMA, capabilities=CAPABILITIES)
        self.assertEqual(partitioned.columns, ("day",))
        self.assertEqual(liquid.columns, ("id", "region"))
        self.assertIsNone(layout.validate_layout(None, SCHEMA))
        self.assertIsNone(layout.validate_layout({}, SCHEMA))
        self.assertIsNone(
            layout.validate_layout(
                {"layout_strategy": None, "partition_columns": []}, SCHEMA
            )
        )

    def test_invalid_missing_conflicting_or_unordered_settings(self):
        invalid = (
            {"layout_strategy": "other"},
            {"layout_strategy": ""},
            {"layout_strategy": "partitioned"},
            {"layout_strategy": "liquid"},
            {"partition_columns": ["day"]},
            PARTITIONED | {"cluster_by_columns": ["id"]},
            LIQUID | {"partition_columns": ["day"]},
            PARTITIONED | {"partition_columns": "day"},
            PARTITIONED | {"partition_columns": {"day"}},
            PARTITIONED | {"partition_columns": ["day", "day"]},
            PARTITIONED | {"partition_columns": ["missing"]},
            PARTITIONED | {"partition_columns": ["Day"]},
            PARTITIONED | {"partition_columns": ["day; DROP TABLE x"]},
            PARTITIONED | {"partition_columns": [""]},
            LIQUID | {"zorder_columns": ["id"]},
            PARTITIONED | {"partition_enabled": True},
            PARTITIONED | {"cluster_by_columns": None},
        )
        for metadata in invalid:
            with self.subTest(metadata=metadata), self.assertRaises(layout.LayoutError):
                layout.validate_layout(metadata, SCHEMA, capabilities=CAPABILITIES)

    def test_identifiers_schema_types_and_nullability_are_validated(self):
        source_schema = SCHEMA + (layout.Column("business_time", "timestamp_ntz"),)
        spec = layout.validate_layout(PARTITIONED, source_schema)
        self.assertIn(
            "TIMESTAMP_NTZ", layout.creation_sql("slv.entries", source_schema, spec)
        )
        for schema in (
            (),
            (layout.Column("day", "date"), layout.Column("DAY", "date")),
            (layout.Column("bad-name", "date"),),
            (layout.Column("day", "array<string>"),),
            (layout.Column("day", "date; DROP TABLE x"),),
            (layout.Column("day", "decimal(39,2)"),),
            (layout.Column("day", "decimal(12,13)"),),
            (layout.Column("day", "date", "false"),),
        ):
            with self.subTest(schema=schema), self.assertRaises(layout.LayoutError):
                layout.validate_layout(PARTITIONED, schema)
        self.assertEqual(
            layout.quote_table("lake.slv.entries"), "`lake`.`slv`.`entries`"
        )
        for table in ("", "a..b", "a/b", "a; DROP TABLE x", "a.`b`", "a.b.c.d"):
            with self.subTest(table=table), self.assertRaises(layout.LayoutError):
                layout.quote_table(table)

    def test_liquid_needs_operation_specific_runtime_evidence_and_statistics(self):
        for capabilities in (
            None,
            layout.RuntimeCapabilities("engine", "3.5.7", "", ("liquid_write",)),
            layout.RuntimeCapabilities("engine", "3.5.7", "proof", ("liquid_create",)),
            layout.RuntimeCapabilities("engine", "3.5.7", "proof", ("liquid_write",)),
        ):
            with (
                self.subTest(capabilities=capabilities),
                self.assertRaises(layout.LayoutError),
            ):
                layout.validate_layout(LIQUID, SCHEMA, capabilities=capabilities)
        with self.assertRaises(layout.LayoutError):
            layout.validate_layout(
                {"layout_strategy": "liquid", "cluster_by_columns": ["flag"]},
                (layout.Column("flag", "boolean"),),
                capabilities=CAPABILITIES,
            )
        many = tuple(layout.Column(f"c{i}", "int") for i in range(5))
        with self.assertRaises(layout.LayoutError):
            layout.validate_layout(
                {
                    "layout_strategy": "liquid",
                    "cluster_by_columns": [c.name for c in many],
                },
                many,
                capabilities=CAPABILITIES,
            )

    def test_raw_and_mlv_do_not_inherit_delta_table_capability(self):
        for materialization in ("raw_files", "view", "mlv"):
            with (
                self.subTest(materialization=materialization),
                self.assertRaises(layout.LayoutError),
            ):
                layout.validate_layout(
                    PARTITIONED,
                    SCHEMA,
                    materialization=materialization,
                    capabilities=CAPABILITIES,
                )
        spec = layout.validate_layout(
            LIQUID, SCHEMA, materialization="mlv", capabilities=CAPABILITIES
        )
        self.assertEqual(spec.materialization, "mlv")
        # Validation only: the table creation adapter must never emit MLV DDL.
        with self.assertRaisesRegex(layout.LayoutError, "MLV.*adapter"):
            layout.creation_sql("dbo.aggregate", SCHEMA, spec, CAPABILITIES)

    def test_explicit_schema_creation_uses_documented_delta_sql(self):
        partitioned = layout.validate_layout(PARTITIONED, SCHEMA)
        sql = layout.creation_sql("slv.entries", SCHEMA, partitioned)
        self.assertIn("CREATE TABLE `slv`.`entries` (", sql)
        self.assertIn("`_source_run_id` STRING NOT NULL", sql)
        self.assertTrue(sql.endswith("USING DELTA PARTITIONED BY (`day`)"))
        liquid = layout.validate_layout(LIQUID, SCHEMA, capabilities=CAPABILITIES)
        sql = layout.creation_sql("gld.entries", SCHEMA, liquid, CAPABILITIES)
        self.assertTrue(sql.endswith("USING DELTA CLUSTER BY (`id`, `region`)"))
        for forbidden in ("REPLACE", "OVERWRITE", "IF NOT EXISTS", "ZORDER"):
            self.assertNotIn(forbidden, sql.upper())
        with self.assertRaises(layout.LayoutError):
            layout.creation_sql("slv.entries", SCHEMA, None)


if __name__ == "__main__":
    unittest.main()
