# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Gold inventory, source fidelity, unsupported-measure and MLV contracts."""

from __future__ import annotations

import base64
import hashlib
import json
import unittest
from pathlib import Path

from fabric_medallion_deployer.contracts import load_sources
from fabric_medallion_deployer.gold import (
    GOLD_NOTEBOOKS,
    bootstrap_gold_config,
    gold_config,
    gold_notebook_definition,
    gold_notebook_document,
    gold_pipeline_definition,
    mlv_definitions,
)
from fabric_medallion_deployer.orchestrator import metadata_notebook_document

ROOT = Path(__file__).resolve().parents[2]


class GoldContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sources = load_sources(ROOT)
        cls.config = gold_config(ROOT, cls.sources)

    def test_legacy_compiler_is_only_a_bootstrap_alias(self) -> None:
        self.assertEqual(
            bootstrap_gold_config(ROOT, self.sources),
            gold_config(ROOT, self.sources),
        )

    def test_matches_frozen_gold_semantics(self) -> None:
        expected = json.loads(
            (
                ROOT / "deployment" / "tests" / "fixtures" / "medallion_baseline.json"
            ).read_text(encoding="utf-8")
        )["gold"]
        for field in (
            "execution_mode",
            "target_lakehouse",
            "schema",
            "dimensions",
            "facts",
            "blocked",
        ):
            self.assertEqual(self.config[field], expected[field], field)
        semantic = {
            key: value
            for key, value in self.config.items()
            if key not in {"runtime_sha256", "contract_hash", "sources"}
        }
        digest = hashlib.sha256(
            json.dumps(
                semantic,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        ).hexdigest()
        self.assertEqual(digest, expected["semantic_sha256"])
        self.assertEqual(set(self.config["mlvs"]), set(expected["mlvs"]))

    def test_only_source_backed_facts_and_explicit_demo_gate(self) -> None:
        self.assertEqual(self.config["execution_mode"], "synthetic_demo")
        self.assertTrue(all(s["synthetic_only"] for s in self.config["sources"]))
        facts = {s["name"] for s in self.config["facts"]}
        self.assertTrue(
            {
                "fact_gl_posting",
                "fact_disbursement",
                "fact_receipt",
                "fact_payroll_component",
                "fact_post_snapshot",
                "fact_outstanding_payment_snapshot",
            }
            <= facts
        )
        self.assertFalse(any("budget" in name or "forecast" in name for name in facts))
        self.assertIn("fact_budget_allocation", self.config["blocked"])
        self.assertIn("fact_debt_transaction", self.config["blocked"])
        self.assertEqual(self.config["target_lakehouse"], "lh_gld_treasury")
        self.assertEqual(self.config["schema"], "gld_treasury")
        self.assertEqual(
            self.config["runtime_sha256"],
            hashlib.sha256(
                (ROOT / "artefacts" / "gold" / "gold_runtime.py")
                .read_text(encoding="utf-8")
                .encode("utf-8")
            ).hexdigest(),
        )
        for dimension in self.config["dimensions"][:8]:
            self.assertEqual(len(dimension["keys"]), 3)
            self.assertEqual(
                dimension["semantic_basis"], "UNCONFIRMED_SOURCE_CODE_PATH"
            )

    def test_fact_fields_exist_and_no_private_source_values_are_projected(self) -> None:
        tables = {(s.code, t["name"]): t for s in self.sources for t in s.tables}
        forbidden = {
            "id_num",
            "SAIDNumber",
            "bank_account_number",
            "AccountNumber",
            "surname",
            "first_name",
            "LegalName",
            "Name",
            "acc_num",
        }
        for fact in self.config["facts"]:
            with self.subTest(fact=fact["name"]):
                source = tables[(fact["source"], fact["table"])]
                fields = {c["name"] for c in source["columns"]}
                self.assertTrue(set(fact["keys"]) <= fields)
                self.assertTrue(set(fact["measures"]) <= fields)
                self.assertTrue(set(fact["attributes"]) <= fields)
                self.assertTrue(
                    fact["date_field"] in fields
                    or (not fact["date_field"] and fact["snapshot"])
                )
                self.assertFalse(
                    forbidden & (set(fact["attributes"]) | set(fact["measures"]))
                )
                self.assertTrue(fact["grain"])

    def test_sql_mlvs_are_deterministic_source_backed_and_not_fake_budgets(
        self,
    ) -> None:
        definitions = mlv_definitions(self.config)
        self.assertIn("mlv_finance_monthly", definitions)
        self.assertIn("mlv_recorded_outstanding_payments", definitions)
        for name, sql in definitions.items():
            with self.subTest(mlv=name):
                self.assertIn("CREATE OR REPLACE MATERIALIZED LAKE VIEW", sql)
                self.assertIn("GROUP BY", sql)
                self.assertNotIn("current_date", sql.lower())
                self.assertNotIn("current_timestamp", sql.lower())
                self.assertNotIn("ROW_NUMBER", sql.upper())
                self.assertNotIn("budget", name)
        self.assertNotIn("actual_expenditure", definitions["mlv_finance_monthly"])

    def test_generated_assets_are_gold_bound_and_do_not_launch_sources(self) -> None:
        for step in GOLD_NOTEBOOKS:
            definition = gold_notebook_definition(
                ROOT,
                self.sources,
                step,
                workspace_id="workspace-test",
                gold_lakehouse_id="gold-test",
                source_lakehouse_id="silver-test",
                document=gold_notebook_document(ROOT, self.sources, step),
            )
            nb = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
            self.assertEqual(
                nb["metadata"]["dependencies"]["lakehouse"]["default_lakehouse_name"],
                "lh_gld_treasury",
            )
            for cell in nb["cells"]:
                if cell["cell_type"] == "code":
                    compile("".join(cell["source"]), f"gold/{step}", "exec")
                    self.assertEqual(cell["outputs"], [])
        with self.assertRaisesRegex(ValueError, "wrapper pipelines are retired"):
            gold_pipeline_definition(
                self.sources,
                workspace_id="workspace-test",
                notebook_id="build-gold-test",
            )

    def test_in_memory_generation_does_not_bypass_stale_notebook_validation(
        self,
    ) -> None:
        document = gold_notebook_document(ROOT, self.sources, "mlvs")
        document["cells"][0]["source"] = ["unreviewed replacement"]
        with self.assertRaisesRegex(ValueError, "Stale generated Gold notebook"):
            gold_notebook_definition(
                ROOT,
                self.sources,
                "mlvs",
                workspace_id="local-workspace",
                gold_lakehouse_id="local-gold",
                source_lakehouse_id="local-silver",
                document=document,
            )

    def test_gold_build_passes_quality_policy_to_the_embedded_ingestion_adapter(self):
        document = metadata_notebook_document(ROOT, "gold_worker")
        namespace = {}
        for cell in document["cells"]:
            if cell["cell_type"] == "code" and cell["id"] != "execute":
                exec(  # noqa: S102 - trusted locally generated notebook code
                    compile("".join(cell["source"]), "gold-worker-bundle", "exec"),
                    namespace,
                )
        handlers = namespace["RULES"]._MEDALLION_RULE_HANDLERS
        self.assertIs(namespace["GOLD"]._MEDALLION_RULE_HANDLERS, handlers)
        self.assertIs(namespace["INGESTION"]._MEDALLION_RULE_HANDLERS, handlers)
        self.assertIs(handlers["blocking_reasons"], namespace["RULES"].blocking_reasons)


if __name__ == "__main__":
    unittest.main()
