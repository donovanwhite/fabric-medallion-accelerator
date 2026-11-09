"""Declared case selection, historical aliases and competing-authority checks."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

from fabric_medallion_deployer.asset_paths import (
    department_profile_path,
    local_asset_path,
    resolve_asset_path,
    source_registry_path,
    source_registry_root,
    source_specifications_root,
    use_case_descriptor,
)


def declare_case(root: Path, case: str = "sample", **fields: object) -> dict:
    """Declare one compact case without relying on the repository's Treasury profile."""
    catalog = root / "use_cases"
    case_root = catalog / case
    case_root.mkdir(parents=True, exist_ok=True)
    (catalog / "catalog.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "default_use_case": None,
                "use_cases": [case],
            }
        ),
        encoding="utf-8",
    )
    descriptor = {
        "schema_version": 1, "id": case, "adapter": "snapshot_bootstrap",
        "baseline_version": "1.1.0",
        "resources": {
            "bronze_silver": "lh_sample", "gold": "lh_results",
            "control_database": "db_control", "item_prefix": case,
        },
        "inputs": {
            "source_registry": "contracts/source_registry.json",
            "gold_profile": None, "source_rules": None,
            "gold_rules": None, "compiled_metadata": None,
        },
        "documents": {
            "requirements": "requirements.md", "sources": "sources.md",
            "gold": "gold.md", "plan": "plan.md",
        },
    }
    (case_root / "use_case.json").write_text(json.dumps(descriptor), encoding="utf-8")
    contracts = case_root / "contracts"
    contracts.mkdir()
    (contracts / "provenance_rebaseline.json").write_text(
        json.dumps({"schema_version": 1, "asset_bindings": fields}), encoding="utf-8"
    )
    return descriptor


class AssetPathTests(unittest.TestCase):
    def test_selected_registry_and_profile_have_no_treasury_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            declare_case(root, department_profile="fixtures/sample/roster.json")
            registry = root / "use_cases/sample/contracts/source_registry.json"
            self.assertEqual(source_registry_path(root, "sample"), registry)
            self.assertEqual(source_registry_root(root, "sample"), registry.parent)
            self.assertEqual(
                department_profile_path(root, "sample"),
                root / "fixtures/sample/roster.json",
            )
            with self.assertRaisesRegex(ValueError, "Unknown use case"):
                source_registry_path(root, "treasury")

    def test_missing_catalog_does_not_infer_a_treasury_authority(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaises(FileNotFoundError),
        ):
            source_registry_path(Path(directory), "sample")

    def test_evidence_supports_only_declared_historical_locations(self) -> None:
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
            self.assertEqual(source_specifications_root(root, "sample"), legacy)
            moved = root / canonical
            moved.mkdir()
            with self.assertRaisesRegex(ValueError, "Ambiguous"):
                source_specifications_root(root, "sample")
            legacy.rmdir()
            self.assertEqual(source_specifications_root(root, "sample"), moved)
            another = root / "docs/specifications"
            another.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "Ambiguous"):
                source_specifications_root(root, "sample")

    def test_aliases_preserve_logical_spelling_without_multiple_authorities(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            declare_case(
                root, asset_aliases={"old/data": "fixtures/sample"}
            )
            canonical = root / "fixtures/sample/schema.json"
            canonical.parent.mkdir(parents=True)
            canonical.write_text("{}")
            self.assertEqual(
                resolve_asset_path(root, "old\\data\\schema.json", "sample"), canonical
            )
            legacy = root / "old/data/schema.json"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("{}")
            for spelling in ("old/data/schema.json", "fixtures/sample/schema.json"):
                with self.assertRaisesRegex(ValueError, "Ambiguous"):
                    resolve_asset_path(root, spelling, "sample")

    def test_paths_and_declared_alias_targets_cannot_escape_repository(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            declare_case(root, asset_aliases={"old": "../outside"})
            with self.assertRaisesRegex(ValueError, "escapes"):
                resolve_asset_path(root, "old/schema.json", "sample")
            with self.assertRaisesRegex(ValueError, "escapes"):
                local_asset_path(root, "../outside")
            for absolute in ("/outside", "C:\\outside"):
                with self.assertRaisesRegex(ValueError, "must be relative"):
                    local_asset_path(root, absolute)

    def test_ambiguous_alias_mapping_is_not_selected_by_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            declare_case(root, asset_aliases={"old": "a", "old/data": "b"})
            with self.assertRaisesRegex(ValueError, "Ambiguous"):
                resolve_asset_path(root, "old/data/schema.json", "sample")

    def test_descriptor_identity_and_explicit_selection_are_checked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            declare_case(root)
            with self.assertRaisesRegex(ValueError, "Unknown use case"):
                use_case_descriptor(root, "../sample")
            path = root / "use_cases/sample/use_case.json"
            descriptor = json.loads(path.read_text())
            descriptor["id"] = "different"
            path.write_text(json.dumps(descriptor))
            with self.assertRaisesRegex(ValueError, "identity"):
                use_case_descriptor(root, "sample")

    def test_fixture_wrappers_reject_implicit_named_imports(self) -> None:
        from deployment import generate_fixtures as generate
        from deployment import validate_fixtures as validate

        for module, name in (
            (generate, "bas_rows"),
            (generate, "csd_rows"),
            (generate, "persal_rows"),
            (validate, "validate_data"),
            (validate, "validate_output"),
        ):
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(ValueError, "explicit.*--use-case"),
            ):
                getattr(module, name)
        self.assertNotIn("bas_rows", vars(generate))
        self.assertNotIn("validate_data", vars(validate))

    def test_compiled_case_without_fixtures_never_imports_a_treasury_adapter(self) -> None:
        from deployment.generate_fixtures import load_fixture_module

        with patch("deployment.generate_fixtures.importlib.import_module") as imported:
            for role in ("builder", "validator"):
                with self.assertRaisesRegex(ValueError, "does not declare a local fixture"):
                    load_fixture_module(role, use_case="inventory_example")
            imported.assert_not_called()

    def test_fixture_modules_come_from_selected_catalog_and_declared_assets(self) -> None:
        from deployment.generate_fixtures import load_fixture_module

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            declare_case(
                root,
                fixture_builder_module="sample_fixture_builder",
                fixture_validator_module="sample_fixture_validator",
                provenance_assets={
                    "generator_sha256": "fixtures/sample/builder.py",
                    "validator_sha256": "fixtures/sample/validator.py",
                },
            )
            builder = ModuleType("sample_fixture_builder")
            builder.__file__ = str(root / "fixtures/sample/builder.py")
            validator = ModuleType("sample_fixture_validator")
            validator.__file__ = str(root / "fixtures/sample/validator.py")
            with patch.dict(
                sys.modules,
                {"sample_fixture_builder": builder, "sample_fixture_validator": validator},
            ):
                self.assertIs(
                    load_fixture_module(root=root, use_case="sample"), builder
                )
                self.assertIs(
                    load_fixture_module("validator", root=root, use_case="sample"),
                    validator,
                )
                validator.__file__ = builder.__file__
                with self.assertRaisesRegex(ValueError, "differs from its declared asset"):
                    load_fixture_module("validator", root=root, use_case="sample")
            with self.assertRaisesRegex(ValueError, "Unknown fixture adapter role"):
                load_fixture_module("unknown", root=root)

    def test_fixture_launchers_forward_case_options_and_restore_process_arguments(self) -> None:
        from deployment import generate_fixtures as generate
        from deployment import validate_fixtures as validate

        original = sys.argv
        captured = []
        module = ModuleType("sample_fixture_builder")
        module.__file__ = "sample_fixture_builder.py"

        def run_fixture() -> int:
            captured.extend(sys.argv)
            return 0

        module.main = run_fixture
        with patch.object(generate, "load_fixture_module", return_value=module) as selected:
            self.assertEqual(
                generate.main(["--use-case", "sample", "--validate-only", "--seed", "42"]),
                0,
            )
            selected.assert_called_once_with(use_case="sample")
        self.assertEqual(
            captured, ["sample_fixture_builder.py", "--validate-only", "--seed", "42"]
        )
        self.assertIs(sys.argv, original)
        with patch.object(validate, "builder_main", return_value=0) as launched:
            self.assertEqual(validate.main(["--use-case", "sample"]), 0)
            launched.assert_called_once_with(["--validate-only", "--use-case", "sample"])


if __name__ == "__main__":
    unittest.main()
