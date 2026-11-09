"""Baseline inheritance, typed contracts and deterministic multi-case compilation."""

from __future__ import annotations

import json
import re
import shutil
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from fabric_medallion_deployer.metadata import load_configuration
from fabric_medallion_deployer.spec_compiler import (
    SpecificationError,
    _compile_documents,
    compile_use_case,
    validate_case_documents,
)

ROOT = Path(__file__).resolve().parents[2]


class SpecificationCompilerTests(unittest.TestCase):
    def test_worked_example_reaches_generic_gold_planning_without_treasury_rules(self):
        from fabric_medallion_deployer.configuration import bind_configuration
        from fabric_medallion_deployer.deployer import validation_id
        from fabric_medallion_deployer.metadata import compile_run_plan
        from fabric_medallion_deployer.orchestrator import (
            bootstrap_configuration,
            metadata_notebook_document,
            metadata_worker_sha256,
        )

        namespace = {}
        notebook = metadata_notebook_document(ROOT, "gold", "inventory_example")
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code" and cell["id"] != "execute":
                exec("".join(cell["source"]), namespace)  # noqa: S102 - trusted generated bundle
        gold = namespace["GOLD"]

        config = bootstrap_configuration(ROOT, use_case="inventory_example")
        bound = bind_configuration(
            config,
            workspace_id=validation_id("inventory-workspace"),
            source_database_ids={"inventory": validation_id("inventory-source")},
            connection_ids={"inventory": validation_id("inventory-connection")},
        )
        document = bound.to_document()
        for capability in document["capabilities"]:
            capability.update(verified=True, evidence_sha256="e" * 64)
        for obj in document["objects"]:
            obj["enabled"] = obj["materialization"] != "mlv"
        fixture = load_configuration(document)
        plan = compile_run_plan(
            fixture,
            "inventory-offline-only",
            "2026-11-09T08:00:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=fixture.definition_sha256,
            require_ready=True,
        )
        restored = gold.gold_config_from_manifest(
            plan.to_document(),
            snapshot_sha256=plan.snapshot_sha256,
            code_sha256=metadata_worker_sha256(
                ROOT, layer="gold", use_case="inventory_example"
            ),
            lakehouse="lh_gld_inventory_example",
        )
        self.assertEqual(restored["calendar"]["fiscal_year_start_month"], 1)
        self.assertEqual(restored["facts"][0]["units"]["quantity"], "units")
        self.assertEqual(restored["dimensions"][0]["name"], "dim_product")
        self.assertEqual(restored["sources"][0]["source_code"], "inventory")

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for relative in (
            "use_cases/catalog.json",
            "use_cases/inventory_example/use_case.json",
            "baselines/requirements/policy.json",
            "use_cases/inventory_example/requirements/requirements.md",
            "use_cases/inventory_example/specifications/source_contract.md",
            "use_cases/inventory_example/specifications/gold_contract.md",
            "use_cases/inventory_example/plans/plan.md",
        ):
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, target)
        engine = self.root / "artefacts/medallion/rules.py"
        engine.parent.mkdir(parents=True)
        shutil.copyfile(ROOT / "artefacts/medallion/rules.py", engine)
        policy = json.loads((ROOT / "baselines/requirements/policy.json").read_text())
        for relative in policy["governing_documents"]:
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, target)

    def documents(self):
        return validate_case_documents(self.root, "inventory_example")

    def mutate(self, key, callback):
        profile, *_ = self.documents()
        path = profile.path(profile.documents[key])
        text = path.read_text(encoding="utf-8")
        block = re.search(r"^```json\n(.*?)^```", text, re.MULTILINE | re.DOTALL)
        document = json.loads(block[1])
        callback(document)
        path.write_text(
            text[: block.start(1)]
            + json.dumps(document, indent=2)
            + "\n"
            + text[block.end(1) :],
            encoding="utf-8",
        )

    def test_explicit_non_treasury_inputs_compile_to_valid_unapproved_metadata(self):
        profile, policy, requirements, sources, gold = self.documents()
        document = _compile_documents(
            profile, policy, sources, gold, {"silver": "a" * 64, "gold": "b" * 64}
        )
        config = load_configuration(document)
        self.assertEqual(len(config.feeds), 1)
        self.assertEqual(config.feeds[0].source_code, "inventory")
        self.assertEqual(sum(obj.layer == "silver" for obj in config.objects), 2)
        self.assertTrue(
            all(not capability.verified for capability in config.capabilities)
        )
        self.assertTrue(
            all(
                connection.fabric_connection_id is None
                for connection in config.connections
            )
        )
        self.assertFalse(
            any(obj.enabled for obj in config.objects if obj.layer == "gold")
        )
        self.assertEqual(
            document["extensions"]["gold_contract"]["calendar"][
                "fiscal_year_start_month"
            ],
            1,
        )
        self.assertNotIn("treasury", json.dumps(document).lower())
        self.assertEqual(requirements["use_case_id"], profile.case_id)

    def test_sql_server_contract_compiles_with_a_distinct_unverified_copy_capability(
        self,
    ):
        self.mutate(
            "sources",
            lambda doc: doc["sources"][0].update(
                connector_type="SqlServer", authentication_type="Basic"
            ),
        )
        profile, policy, _, sources, gold = self.documents()
        document = _compile_documents(
            profile, policy, sources, gold, {"silver": "a" * 64, "gold": "b" * 64}
        )
        config = load_configuration(document)
        self.assertEqual(config.connections[0].connector_type, "SqlServer")
        self.assertEqual(config.connections[0].authentication_type, "Basic")
        self.assertIsNone(config.connections[0].fabric_connection_id)
        self.assertIsNone(config.feeds[0].geometry)
        required = next(
            obj.required_capabilities for obj in config.objects if obj.layer == "bronze"
        )
        self.assertIn("sql-server-copy-basic", required)
        self.assertNotIn("dynamic-fabric-sql", required)
        self.assertFalse(
            next(
                cap for cap in config.capabilities if cap.id == "sql-server-copy-basic"
            ).verified
        )

    def test_unknown_sql_connector_and_authentication_combinations_are_rejected(self):
        for connector, authentication in (
            ("SqlServer", "Anonymous"),
            ("FabricSql", "Basic"),
            ("UnknownSql", "Basic"),
            ([], "Basic"),
        ):
            _, policy, _, sources, gold = self.documents()
            sources["sources"][0].update(
                connector_type=connector, authentication_type=authentication
            )
            profile, *_ = self.documents()
            with (
                self.subTest(connector=connector),
                self.assertRaises(SpecificationError),
            ):
                _compile_documents(
                    profile,
                    policy,
                    sources,
                    gold,
                    {"silver": "a" * 64, "gold": "b" * 64},
                )

    def test_baseline_omission_version_drift_and_case_redefinition_are_blocked(self):
        for mutation in (
            lambda doc: doc["baseline_requirements"].pop(),
            lambda doc: doc.update(baseline_version="2.0.0"),
            lambda doc: doc["requirements"][0].update(id="BASE-001"),
            lambda doc: doc.update(overrides={"silver_storage": "mutable"}),
        ):
            with self.subTest(mutation=mutation):
                original = (
                    self.root
                    / "use_cases/inventory_example/requirements/requirements.md"
                )
                saved = original.read_text()
                self.mutate("requirements", mutation)
                with self.assertRaises(SpecificationError):
                    self.documents()
                original.write_text(saved)

    def test_missing_business_keys_units_and_unknown_handlers_are_not_inferred(self):
        for kind, mutation in (
            ("sources", lambda doc: doc["sources"][0]["tables"][0].update(keys=[])),
            (
                "sources",
                lambda doc: doc["sources"][0]["tables"][0]["dq_rules"][0].update(
                    handler="run_python"
                ),
            ),
            ("gold", lambda doc: doc["facts"][0]["measures"][0].update(unit="")),
            ("gold", lambda doc: doc["facts"][0].update(grain="TBD")),
            (
                "gold",
                lambda doc: doc["facts"][0]["dimension_lookups"][0].update(
                    source_columns=["guessed"]
                ),
            ),
        ):
            with self.subTest(kind=kind, mutation=mutation):
                profile, *_ = self.documents()
                path = profile.path(profile.documents[kind])
                saved = path.read_text()
                self.mutate(kind, mutation)
                with self.assertRaises(ValueError):
                    profile, policy, _, sources, gold = self.documents()
                    _compile_documents(
                        profile,
                        policy,
                        sources,
                        gold,
                        {"silver": "a" * 64, "gold": "b" * 64},
                    )
                path.write_text(saved)

    def test_compile_is_deterministic_case_scoped_and_preserves_noop_build_products(
        self,
    ):
        with patch(
            "fabric_medallion_deployer.orchestrator.metadata_worker_sha256",
            side_effect=lambda _root, *, layer, **_kwargs: (
                ("a" if layer == "silver" else "b") * 64
            ),
        ):
            first = compile_use_case(self.root, "inventory_example")
            directory = self.root / "use_cases/inventory_example/generated"
            manifest = directory / "deployment/manifest.json"
            manifest.parent.mkdir()
            manifest.write_text('{"local_fixture":true}\n')
            self.assertEqual(first, compile_use_case(self.root, "inventory_example"))
            self.assertTrue(manifest.is_file())
            self.assertEqual(
                first, compile_use_case(self.root, "inventory_example", check=True)
            )
            self.assertEqual(
                list((self.root / "artefacts").rglob("*.py")),
                [self.root / "artefacts/medallion/rules.py"],
            )
            self.assertFalse((self.root / "use_cases/treasury/generated").exists())
            definition = directory / "metadata/definition.json"
            definition.write_text("{}")
            with self.assertRaisesRegex(SpecificationError, "stale generated"):
                compile_use_case(self.root, "inventory_example", check=True)
            with self.assertRaisesRegex(SpecificationError, "hand-edited"):
                compile_use_case(self.root, "inventory_example")

    def test_raw_prose_or_multiple_contract_blocks_cannot_become_executable_configuration(
        self,
    ):
        profile, *_ = self.documents()
        path = profile.path(profile.documents["sources"])
        text = path.read_text()
        path.write_text(text + "\n```json\n{}\n```\n")
        with self.assertRaisesRegex(SpecificationError, "exactly one"):
            self.documents()

    def test_invalid_gold_does_not_replace_previous_generated_tree(self):
        profile, policy, _, sources, gold = self.documents()
        changed = deepcopy(gold)
        changed["aggregates"][0]["sum_columns"] = ["missing"]
        with self.assertRaisesRegex(SpecificationError, "Aggregate"):
            _compile_documents(
                profile,
                policy,
                sources,
                changed,
                {"silver": "a" * 64, "gold": "b" * 64},
            )

    def test_explicit_source_sentinel_values_are_not_unresolved_document_placeholders(
        self,
    ):
        self.mutate(
            "sources",
            lambda doc: doc["sources"][0]["tables"][0]["dq_rules"][0].update(
                handler="allowed_values",
                parameters={
                    "column": "category",
                    "values": ["UNKNOWN", "REQUIRED"],
                    "allow_null": False,
                },
            ),
        )
        profile, policy, _, sources, gold = self.documents()
        config = _compile_documents(
            profile, policy, sources, gold, {"silver": "a" * 64, "gold": "b" * 64}
        )
        self.assertIn("UNKNOWN", json.dumps(config["rules"]))


if __name__ == "__main__":
    unittest.main()
