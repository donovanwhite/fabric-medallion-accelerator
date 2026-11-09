"""Gold runtime constraints preserve rows and distinguish warnings from blockers."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from test_gold_runtime import gold, load_runtime

rules = load_runtime("test_gold_quality_rules", "artefacts/medallion/rules.py")


@unittest.skipUnless(
    os.environ.get("TREASURY_SPARK_TESTS") == "1", "Opt-in real Spark quality tier"
)
class GoldQualitySparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("gold-runtime-quality")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def config(self, *, constraints=(), row_rules=()):
        return {
            "_metadata": {
                "tasks": {"fact_sample": {"object_id": "gold.fact_sample"}},
                "rules": list(row_rules),
                "constraints": list(constraints),
            }
        }

    def test_warning_keeps_rows_and_fail_rule_withholds_output(self):
        frame = self.spark.createDataFrame([(1, 10), (2, -1)], "id int, amount int")
        rule = {
            "id": "nonnegative",
            "object_id": "gold.fact_sample",
            "handler": "value_range",
            "version": "1",
            "severity": "warning",
            "action": "warn",
            "parameters": {"column": "amount", "minimum": 0},
        }
        with patch.object(
            gold, "_MEDALLION_RULE_HANDLERS", rules._MEDALLION_RULE_HANDLERS, create=True
        ):
            evidence = gold.validate_gold_quality(
                None, frame, self.config(row_rules=[rule]), "fact_sample", {}
            )
            self.assertEqual(evidence[0]["violations"], 1)
            self.assertEqual(frame.count(), 2)
            with self.assertRaisesRegex(RuntimeError, "blocking quality"):
                gold.validate_gold_quality(
                    None,
                    frame,
                    self.config(
                        row_rules=[rule | {"action": "fail", "severity": "error"}]
                    ),
                    "fact_sample",
                    {},
                )
            with self.assertRaisesRegex(RuntimeError, "discard rows"):
                gold.validate_gold_quality(
                    None,
                    frame,
                    self.config(row_rules=[rule | {"action": "quarantine"}]),
                    "fact_sample",
                    {},
                )

    def test_unique_and_relationship_constraints_use_pinned_inputs(self):
        frame = self.spark.createDataFrame([(1,), (2,)], "id int")
        duplicate = self.spark.createDataFrame([(1,), (1,)], "id int")
        constraint = {
            "id": "unique-id",
            "object_id": "gold.fact_sample",
            "kind": "unique",
            "enforcement": "runtime",
            "definition": {"columns": ["id"]},
        }
        config = self.config(constraints=[constraint])
        self.assertEqual(
            gold.validate_gold_quality(None, frame, config, "fact_sample", {})[0][
                "violations"
            ],
            0,
        )
        with self.assertRaisesRegex(RuntimeError, "constraint failed"):
            gold.validate_gold_quality(None, duplicate, config, "fact_sample", {})
        relationship = constraint | {
            "kind": "relationship",
            "definition": {
                "columns": ["id"],
                "reference_table": "lh.gld.dim_sample",
                "reference_columns": ["parent_id"],
            },
        }
        with self.assertRaisesRegex(RuntimeError, "pinned upstream"):
            gold.validate_gold_quality(
                None, frame, self.config(constraints=[relationship]), "fact_sample", {}
            )
        parent = self.spark.createDataFrame([(1,), (2,)], "parent_id int")
        with patch.object(gold, "_reload_gold_table", return_value=parent) as read:
            result = gold.validate_gold_quality(
                None,
                frame,
                self.config(constraints=[relationship]),
                "fact_sample",
                {
                    "dimension": {
                        "table": "lh.gld.dim_sample",
                        "location": "local",
                        "version": 9,
                    }
                },
            )
        self.assertEqual(result[0]["violations"], 0)
        self.assertEqual(read.call_args.args[1]["version"], 9)

    def test_exact_reconciliation_preserves_duplicate_multiplicity(self):
        frame = self.spark.createDataFrame([(1,), (1,)], "id int")
        parent = self.spark.createDataFrame([(1,)], "parent_id int")
        constraint = {
            "id": "same-population",
            "object_id": "gold.fact_sample",
            "kind": "reconciliation",
            "enforcement": "runtime",
            "definition": {
                "columns": ["id"],
                "reference_table": "lh.gld.parent",
                "reference_columns": ["parent_id"],
            },
        }
        with (
            patch.object(gold, "_reload_gold_table", return_value=parent),
            self.assertRaisesRegex(RuntimeError, "constraint failed"),
        ):
            gold.validate_gold_quality(
                None,
                frame,
                self.config(constraints=[constraint]),
                "fact_sample",
                {
                    "parent": {
                        "table": "lh.gld.parent",
                        "location": "local",
                        "version": 0,
                    }
                },
            )
        with self.assertRaisesRegex(RuntimeError, "Engine-enforced"):
            gold.validate_gold_quality(
                None,
                frame,
                self.config(constraints=[constraint | {"enforcement": "engine"}]),
                "fact_sample",
                {},
            )


if __name__ == "__main__":
    unittest.main()
