"""Shared artefacts have neutral names and no proof-case populations."""

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class SharedArtefactTests(unittest.TestCase):
    def test_shared_runtime_names_and_namespaces_are_domain_neutral(self):
        artefacts = ROOT / "artefacts"
        modules = list(artefacts.rglob("*.py"))
        self.assertTrue(modules)
        for path in modules:
            with self.subTest(path=path):
                self.assertNotIn("treasury", str(path.relative_to(artefacts)).lower())
                self.assertNotIn("treasury", path.read_text(encoding="utf-8").lower())
        self.assertFalse(
            (ROOT / "deployment" / "src" / "fabric_treasury_deployer").exists()
        )

    def test_case_fixtures_and_historical_pipeline_are_not_shared_artefacts(self):
        artefacts = ROOT / "artefacts"
        self.assertFalse(list(artefacts.rglob("*.csv")))
        self.assertFalse((artefacts / "synthetic_data").exists())
        self.assertFalse(
            (artefacts / "pipeline" / "full_snapshot_pipeline.json").exists()
        )
        case = ROOT / "use_cases" / "treasury"
        self.assertEqual(len(list((case / "fixtures").rglob("*.csv"))), 44)
        self.assertTrue((case / "contracts" / "full_snapshot_pipeline.json").is_file())


if __name__ == "__main__":
    unittest.main()
