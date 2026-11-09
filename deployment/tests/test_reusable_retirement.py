"""Normal runtime artefacts must not regenerate superseded source-bound wrappers."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from fabric_medallion_deployer.contracts import load_sources, owned_inventory
from fabric_medallion_deployer.deployer import inventory_manifest
from fabric_medallion_deployer.inventory import MLV_NOTEBOOK
from fabric_medallion_deployer.orchestrator import METADATA_NOTEBOOKS
from fabric_medallion_deployer.use_cases import load_use_case

ROOT = Path(__file__).resolve().parents[2]


class ReusableRetirementTests(unittest.TestCase):
    def test_six_generated_notebooks_have_no_embedded_active_contract(self):
        paths = [
            load_use_case(ROOT, use_case="treasury").generated
            / "notebooks"
            / f"{name}.ipynb"
            for name in METADATA_NOTEBOOKS.values()
        ] + [
            load_use_case(ROOT, use_case="treasury").generated
            / "notebooks"
            / f"{MLV_NOTEBOOK}.ipynb"
        ]
        self.assertEqual(len(paths), 6)
        for path in paths:
            with self.subTest(path=path):
                document = json.loads(path.read_text(encoding="utf-8"))
                for cell in document["cells"]:
                    if cell["cell_type"] == "code":
                        code = "".join(cell["source"])
                        self.assertNotIn("SOURCE_CONFIG =", code)
                        self.assertNotIn("GOLD_CONFIG =", code)
                        self.assertEqual(cell["outputs"], [])
                        compile(code, str(path), "exec")

    def test_superseded_files_are_absent_but_shared_logic_is_retained(self):
        sources = load_sources(ROOT, use_case="treasury")
        for source in sources:
            for name in source.notebooks.values():
                self.assertFalse(
                    (
                        ROOT / "artefacts" / "medallion" / source.code / f"{name}.ipynb"
                    ).exists()
                )
        self.assertFalse(
            (ROOT / "artefacts" / "gold" / "nb_treasury_silver_to_gold.ipynb").exists()
        )
        for relative in (
            "artefacts\\medallion\\ingestion.py",
            "artefacts\\medallion\\rules.py",
            "artefacts\\gold\\gold_runtime.py",
            "use_cases\\treasury\\contracts\\full_snapshot_pipeline.json",
            "deployment\\tests\\fixtures\\medallion_baseline.json",
        ):
            self.assertTrue((ROOT / relative).is_file(), relative)

    def test_normal_manifest_matches_reusable_inventory_not_source_count(self):
        sources = load_sources(ROOT, use_case="treasury")
        profile = load_use_case(ROOT, use_case="treasury")
        manifest = json.loads(
            (profile.generated / "deployment" / "manifest.json").read_text()
        )
        self.assertEqual(manifest, inventory_manifest(profile=profile))
        inventory = owned_inventory(sources)
        self.assertEqual(sum(kind == "Notebook" for kind in inventory.values()), 6)
        self.assertEqual(sum(kind == "DataPipeline" for kind in inventory.values()), 2)
        for source in sources:
            self.assertNotIn(source.database, inventory)
            self.assertNotIn(source.pipeline, inventory)
            self.assertTrue(set(source.notebooks.values()).isdisjoint(inventory))
        self.assertNotIn("pl_silver_to_gold_treasury", inventory)
        self.assertIn("db_orchestrator", inventory)


if __name__ == "__main__":
    unittest.main()
