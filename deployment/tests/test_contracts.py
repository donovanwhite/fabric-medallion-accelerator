"""Contract compilation tests for the three Treasury source feeds."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fabric_medallion_deployer.asset_paths import source_registry_path
from fabric_medallion_deployer.contracts import (
    _validate_lineage,
    digest,
    load_sources,
    owned_inventory,
    source_specifications_root,
    validate_seed_data,
)
from test_asset_paths import declare_case

ROOT = Path(__file__).resolve().parents[2]


class ContractTests(unittest.TestCase):
    def test_source_provenance_supports_relocation_without_ambiguous_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            canonical = "use_cases/sample/docling_specifications"
            declare_case(
                root,
                source_specifications=canonical,
                asset_aliases={
                    "specifications": canonical,
                    "docs/specifications": canonical,
                },
            )
            legacy = root / "specifications"
            legacy.mkdir()
            self.assertEqual(source_specifications_root(root), legacy)
            relocated = root / "docs" / "specifications"
            relocated.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "Ambiguous"):
                source_specifications_root(root)
            legacy.rmdir()
            self.assertEqual(source_specifications_root(root), relocated)

    def test_seed_validation_uses_the_same_resolved_source_provenance(self):
        for location in (
            ("specifications",),
            ("docs", "specifications"),
            ("use_cases", "sample", "docling_specifications"),
        ):
            with (
                self.subTest(location=location),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory).resolve()
                canonical = "use_cases/sample/docling_specifications"
                declare_case(
                    root,
                    source_specifications=canonical,
                    synthetic_data="fixtures/sample",
                    seed_validator="fixture_code.generate",
                    contract_builder_module="fixture_code.contracts.source_contracts",
                    asset_aliases={
                        "specifications": canonical,
                        "docs/specifications": canonical,
                    },
                )
                extracted = root.joinpath(*location, "extracted")
                extracted.mkdir(parents=True)
                builders = root / "fixture_code"
                builders.mkdir(parents=True)
                contracts = builders / "contracts"
                contracts.mkdir(parents=True)
                (contracts / "source_contracts.py").write_text(
                    "from pathlib import Path\n"
                    "ROOT = Path(__file__).resolve().parents[2]\n"
                    "EXTRACTED = ROOT / 'specifications' / 'extracted'\n",
                    encoding="utf-8",
                )
                (builders / "generate.py").write_text(
                    "import sys\n"
                    "from pathlib import Path\n"
                    "from fixture_code.contracts import source_contracts\n"
                    "def main():\n"
                    "    assert sys.argv[1] == '--validate-only'\n"
                    f"    assert sys.argv[2:] == ['--output-root', {str(root / 'fixtures/sample')!r}]\n"
                    f"    assert source_contracts.ROOT == Path({str(root)!r})\n"
                    f"    assert source_contracts.EXTRACTED == Path({str(extracted)!r})\n"
                    "    return 0\n"
                    "if __name__ == '__main__':\n"
                    "    raise SystemExit(main())\n",
                    encoding="utf-8",
                )
                validate_seed_data(root)

    def test_case_without_synthetic_generator_does_not_run_treasury(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            declare_case(root)
            with patch("fabric_medallion_deployer.contracts.subprocess.run") as run:
                validate_seed_data(root)
            run.assert_not_called()

    def test_declared_validator_need_not_use_docling_or_treasury_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            declare_case(
                root, seed_validator="fixture_validator", synthetic_data="fixtures/sample"
            )
            (root / "fixture_validator.py").write_text(
                "import sys\n"
                "def main():\n"
                f"    assert sys.argv[1:] == ['--validate-only', '--output-root', {str(root / 'fixtures/sample')!r}]\n"
                "    return 0\n",
                encoding="utf-8",
            )
            validate_seed_data(root)

    def test_arbitrary_case_source_names_and_schema_mappings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            declare_case(
                root,
                change_hash_exclusions={"sensor": {"events": ["observed"]}},
            )
            assets = root / "fixtures/sample"
            csv_path = assets / "sensor/events/events_20261102.csv"
            csv_path.parent.mkdir(parents=True)
            csv_path.write_text("id,observed\n1,now\n", encoding="utf-8")
            schema = {
                "source": "sensor", "synthetic_only": True,
                "tables": [{
                    "name": "events", "keys": ["id"], "foreign_keys": [],
                    "columns": [
                        {"name": "id", "data_type": "int", "nullable": False},
                        {"name": "observed", "data_type": "varchar", "nullable": False},
                    ],
                }],
            }
            schema_path = assets / "schema.json"
            schema_path.write_text(json.dumps(schema), encoding="utf-8")
            manifest = {
                "source": "sensor", "synthetic_only": True, "snapshot_date": "2026-11-02",
                "schema_sha256": digest(schema_path),
                "tables": [{
                    "name": "events", "file": "sensor/events/events_20261102.csv",
                    "sha256": digest(csv_path), "rows": 1, "columns": 2,
                }],
            }
            (assets / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            registry = {
                "version": 1,
                "lakehouses": {"bronze_silver": "lh_sample", "gold": "lh_results"},
                "sources": [{
                    "code": "sensor", "database": "db_telemetry",
                    "source_schema": "incoming", "silver_schema": "accepted",
                    "quarantine_schema": "rejected", "connection": "conn_events",
                    "pipeline": "pl_events", "schema_path": "fixtures/sample/schema.json",
                    "manifest_path": "fixtures/sample/manifest.json", "allow_empty_tables": [],
                }],
            }
            registry_path = root / "use_cases/sample/contracts/source_registry.json"
            registry_path.parent.mkdir(parents=True, exist_ok=True)
            registry_path.write_text(json.dumps(registry), encoding="utf-8")
            source, = load_sources(root)
            self.assertEqual(source.code, "sensor")
            self.assertEqual(source.lakehouse, "lh_sample")
            self.assertEqual(source.source_schema, "incoming")
            self.assertEqual(source.silver_schema, "accepted")
            self.assertEqual(source.tables[0]["change_hash_exclusions"], ["observed"])
            self.assertEqual(load_sources(root, "sample"), (source,))
            registry["sources"].append(registry["sources"][0])
            registry_path.write_text(json.dumps(registry), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unique source codes"):
                load_sources(root)

    def test_matches_frozen_pre_metadata_contracts(self) -> None:
        baseline = json.loads(
            (
                ROOT / "deployment" / "tests" / "fixtures" / "medallion_baseline.json"
            ).read_text(encoding="utf-8")
        )
        for source, expected in zip(
            load_sources(ROOT), baseline["sources"], strict=True
        ):
            with self.subTest(source=source.code):
                self.assertEqual(source.code, expected["source_code"])
                self.assertEqual(source.contract_hash, expected["legacy_contract_hash"])
                self.assertEqual(source.silver_schema, expected["silver_schema"])
                self.assertEqual(
                    source.quarantine_schema, expected["quarantine_schema"]
                )
                self.assertEqual(source.bronze_root, expected["legacy_bronze_root"])
                for table, frozen in zip(
                    source.tables, expected["tables"], strict=True
                ):
                    self.assertEqual(table["name"], frozen["name"])
                    self.assertEqual(table["keys"], frozen["keys"])
                    self.assertEqual(
                        table["change_hash_exclusions"],
                        frozen["change_hash_exclusions"],
                    )
                    self.assertEqual(
                        [
                            [c["name"], c["data_type"], c["nullable"]]
                            for c in table["columns"]
                        ],
                        frozen["columns"],
                    )
                    digest = hashlib.sha256(
                        json.dumps(
                            table,
                            sort_keys=True,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            allow_nan=False,
                        ).encode()
                    ).hexdigest()
                    self.assertEqual(digest, frozen["contract_sha256"])

    def test_explicit_rebaseline_covers_every_unchanged_business_asset(self) -> None:
        registry_path = source_registry_path(ROOT)
        sidecar = json.loads(
            registry_path.with_name("provenance_rebaseline.json").read_text(
                encoding="utf-8"
            )
        )

        def canonical_hash(value: object) -> str:
            return hashlib.sha256(
                json.dumps(
                    value, sort_keys=True, ensure_ascii=False,
                    separators=(",", ":"), allow_nan=False,
                ).encode("utf-8")
            ).hexdigest()

        self.assertEqual(
            digest(registry_path), sidecar["fingerprints"]["source_registry_sha256"]["old"]
        )
        totals = {"tables": 0, "rows": 0, "columns": 0}
        for source in load_sources(ROOT):
            expected = sidecar["sources"][source.code]
            schema = json.loads(source.schema_path.read_text(encoding="utf-8"))
            business = {
                key: value for key, value in schema.items()
                if key not in {"source_pdf", "extraction_manifest_sha256"}
            }
            self.assertEqual(
                canonical_hash(business), expected["business_contract_sha256"]
            )
            self.assertEqual(
                canonical_hash(source.manifest["tables"]),
                expected["csv_inventory_sha256"],
            )
            self.assertEqual(digest(source.schema_path), expected["schema_sha256"]["new"])
            self.assertEqual(digest(source.manifest_path), expected["manifest_sha256"]["new"])
            for key in (
                "generator_sha256", "validator_sha256", "contract_builder_sha256",
                "department_profile_sha256",
            ):
                self.assertEqual(source.manifest[key], sidecar["fingerprints"][key]["new"])
            for key in totals:
                totals[key] += expected[key]
            stale = {**source.manifest, "generator_sha256": "0" * 64}
            with self.assertRaisesRegex(ValueError, "generate.py changed"):
                _validate_lineage(ROOT, schema, stale, source.schema_path)
        self.assertEqual(
            totals, {key: sidecar["verification"][key] for key in totals}
        )
        self.assertEqual(totals["tables"], sidecar["verification"]["csv_files"])

    def test_complete_source_inventory(self) -> None:
        sources = load_sources(ROOT)
        self.assertEqual([source.code for source in sources], ["bas", "csd", "persal"])
        self.assertEqual([len(s.tables) for s in sources], [17, 21, 6])
        self.assertEqual(
            sum(len(t["columns"]) for s in sources for t in s.tables), 1619
        )
        inventory = owned_inventory(sources)
        self.assertEqual(sum(kind == "Notebook" for kind in inventory.values()), 6)
        self.assertIn("pl_bronze_silver_gold", inventory)
        self.assertFalse(
            any(name.startswith("pl_full_medallion") for name in inventory)
        )
        self.assertTrue(
            all("history_schema" not in source.runtime_config() for source in sources)
        )

    def test_preserves_source_fields_and_schema_abbreviation(self) -> None:
        sources = {source.code: source for source in load_sources(ROOT)}
        self.assertEqual(sources["persal"].silver_schema, "slv_psl")
        employee = next(
            t for t in sources["persal"].tables if t["name"] == "t_employee"
        )
        self.assertIn("extract_date", [c["name"] for c in employee["columns"]])
        self.assertEqual(
            employee["change_hash_exclusions"], ["extract_date", "source_filename"]
        )
        self.assertFalse(
            set(employee["keys"]) & set(employee["change_hash_exclusions"])
        )
        supplier = sources["csd"].tables[0]
        self.assertIn("SupplierNumber", [c["name"] for c in supplier["columns"]])
        self.assertEqual(supplier["change_hash_exclusions"], [])
        self.assertEqual(
            sources["bas"].tables[0]["change_hash_exclusions"],
            ["total_debit", "total_credit", "total_transactions"],
        )
        # Original names remain historical contract metadata, not owned items.
        self.assertTrue(
            set(sources["bas"].notebooks.values()).isdisjoint(
                owned_inventory(tuple(sources.values()))
            )
        )
        self.assertEqual(
            sources["bas"].notebooks["silver"], "nb_bas_bronze_csv_to_silver_delta"
        )

    def test_runtime_contract_has_no_local_paths(self) -> None:
        for source in load_sources(ROOT):
            config = source.runtime_config()
            self.assertEqual(config["source_code"], source.code)
            self.assertNotIn("schema_path", config)
            self.assertEqual(len(config["contract_hash"]), 64)
            self.assertTrue(
                all(
                    t["allow_empty"] for t in config["tables"] if t["name"] == "t_termi"
                )
            )


if __name__ == "__main__":
    unittest.main()
