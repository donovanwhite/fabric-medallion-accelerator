"""Local multi-case proof: no snapshot Source importer, cloud, or shared mutations."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import Mock, patch

from fabric_medallion_deployer.deployer import (
    AcceleratorDeployer,
    DeploymentOptions,
    inventory_manifest,
    validate_local,
)
from fabric_medallion_deployer.gold import (
    dimension_specs,
    gold_config,
    gold_notebook_document,
    mlv_definitions,
)
from fabric_medallion_deployer.inventory import (
    reusable_inventory,
    runtime_source_bindings,
)
from fabric_medallion_deployer.orchestrator import (
    bootstrap_configuration,
    metadata_notebook_definition,
    metadata_notebook_document,
    metadata_worker_sha256,
)
from fabric_medallion_deployer.use_cases import load_use_case
from metadata_fixtures import metadata_document

ROOT = Path(__file__).resolve().parents[2]
SOURCE_RULES = "def source_business_checks(data, source):\n    return []\ndef row_rules(table):\n    return []\n"
GOLD_RULES = "# Explicit generic-only Gold adapter: all DQ is pinned metadata.\n"


class MultiCaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for directory in (
            "artefacts/orchestrator",
            "artefacts/medallion",
            "artefacts/gold",
            "artefacts/pipeline",
        ):
            shutil.copytree(ROOT / directory, self.root / directory)
        package = self.root / "deployment/src/fabric_medallion_deployer"
        package.mkdir(parents=True)
        for name in ("metadata.py", "fabric.py"):
            shutil.copyfile(
                ROOT / "deployment/src/fabric_medallion_deployer" / name, package / name
            )
        cases = self.root / "use_cases"
        cases.mkdir()
        (cases / "catalog.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "default_use_case": "alpha",
                    "use_cases": ["alpha", "beta"],
                }
            ),
            encoding="utf-8",
        )
        for case in ("alpha", "beta"):
            directory = cases / case
            extensions = directory / "generated/extensions"
            extensions.mkdir(parents=True)
            (extensions / "source_rules.py").write_text(
                SOURCE_RULES + f"# {case}\n", encoding="utf-8"
            )
            (extensions / "gold_rules.py").write_text(
                GOLD_RULES + f"# {case}\n", encoding="utf-8"
            )
            resources = {
                "bronze_silver": f"lh_brz_slv_{case}",
                "gold": f"lh_gld_{case}",
                "control_database": f"db_control_{case}",
                "item_prefix": case,
            }
            descriptor = {
                "schema_version": 1,
                "id": case,
                "adapter": "compiled",
                "baseline_version": "1.1.0",
                "resources": resources,
                "inputs": {
                    "source_registry": None,
                    "gold_profile": None,
                    "source_rules": "generated/extensions/source_rules.py",
                    "gold_rules": "generated/extensions/gold_rules.py",
                    "compiled_metadata": "generated/metadata/definition.json",
                },
                "documents": {
                    name: f"specifications/{name}.md"
                    for name in ("requirements", "sources", "gold", "plan")
                },
            }
            (directory / "use_case.json").write_text(
                json.dumps(descriptor), encoding="utf-8"
            )
            document = metadata_document()
            for obj in document["objects"]:
                obj["lakehouse"] = resources["bronze_silver"]
            document["workers"][0]["lakehouse"] = resources["bronze_silver"]
            document["workers"].append(
                {
                    "id": "gold-worker",
                    "notebook_name": "nb_gold_worker",
                    "lakehouse": resources["gold"],
                    "code_sha256": "c" * 64,
                }
            )
            document["capabilities"][0].update(verified=True, evidence_sha256="d" * 64)
            document["extensions"].update(
                {
                    "use_case_id": case,
                    "synthetic_only": True,
                    "legacy_gold_contract_hash": "e" * 64,
                    "gold_contract": {
                        "execution_mode": "synthetic_demo",
                        "target_lakehouse": resources["gold"],
                        "schema": "gld_inventory",
                        "dimensions": [],
                        "facts": [],
                        "blocked": {},
                        "department_mapping": [],
                        "supplier_crosswalk": [],
                        "calendar": {"fiscal_year_start_month": 1},
                    },
                }
            )
            path = directory / "generated/metadata/definition.json"
            path.parent.mkdir()
            path.write_text(json.dumps(document), encoding="utf-8")
        # These deliberately minimal unit fixtures are trusted model inputs, not
        # MD compiler outputs. Bypass only receipt I/O for these exact temp cases.
        verifier = patch(
            "fabric_medallion_deployer.spec_compiler.validate_compilation_receipt",
            side_effect=self.fixture_receipt,
            create=True,
        )
        self.receipt_verifier = verifier.start()
        self.addCleanup(verifier.stop)

    def fixture_receipt(self, root, use_case):
        self.assertEqual(root, self.root)
        self.assertIn(use_case, {"alpha", "beta"})
        return {"trusted_test_fixture": use_case}

    def test_receipt_rejection_precedes_metadata_parsing_and_hash_refresh(self):
        for reason in (
            "missing compilation receipt",
            "source MD changed",
            "generated rule file changed",
            "baseline version changed",
            "baseline policy changed",
            "metadata definition changed",
            "receipt path escapes its case",
        ):
            self.receipt_verifier.reset_mock()
            self.receipt_verifier.side_effect = ValueError(reason)
            with (
                self.subTest(reason=reason),
                patch(
                    "fabric_medallion_deployer.orchestrator.load_configuration",
                    side_effect=AssertionError("unverified metadata was consumed"),
                ) as parse,
                patch(
                    "fabric_medallion_deployer.orchestrator.metadata_worker_sha256",
                    side_effect=AssertionError("stale worker bundle was re-hashed"),
                ) as refresh,
                self.assertRaisesRegex(ValueError, reason),
            ):
                bootstrap_configuration(self.root, use_case="alpha")
            self.receipt_verifier.assert_called_once_with(self.root, "alpha")
            parse.assert_not_called()
            refresh.assert_not_called()

    def generate(self, case):
        profile = load_use_case(self.root, case)
        directory = profile.generated / "notebooks"
        directory.mkdir()
        for role, name in profile.notebook_names().items():
            (directory / f"{name}.ipynb").write_text(
                json.dumps(metadata_notebook_document(self.root, role, case)),
                encoding="utf-8",
            )
        (directory / f"{profile.mlv_notebook}.ipynb").write_text(
            json.dumps(gold_notebook_document(self.root, (), "mlvs", case)),
            encoding="utf-8",
        )
        manifest = profile.generated / "deployment/manifest.json"
        manifest.parent.mkdir()
        manifest.write_text(
            json.dumps(inventory_manifest(profile=profile)), encoding="utf-8"
        )

    def test_selection_inventory_and_generated_paths_are_case_isolated(self):
        self.assertEqual(load_use_case(self.root).case_id, "alpha")
        alpha, beta = [load_use_case(self.root, name) for name in ("alpha", "beta")]
        self.assertFalse(
            set(reusable_inventory(profile=alpha))
            & set(reusable_inventory(profile=beta))
        )
        self.assertNotEqual(alpha.generated, beta.generated)
        with self.assertRaisesRegex(ValueError, "Unknown use case"):
            bootstrap_configuration(self.root, use_case="missing")
        beta.input_path("compiled_metadata").unlink()
        with self.assertRaisesRegex(ValueError, "missing generated metadata"):
            bootstrap_configuration(self.root, use_case="beta")

    def test_drafts_refresh_bundle_hashes_without_reusing_approvals(self):
        config = bootstrap_configuration(self.root)
        other = bootstrap_configuration(self.root, use_case="beta")
        self.assertEqual(
            config.definition_sha256,
            bootstrap_configuration(self.root).definition_sha256,
        )
        self.assertNotEqual(config.version_id, other.version_id)
        self.assertTrue(config.version_id.startswith("alpha-compiled-"))
        document = config.to_document()
        self.assertFalse(any(item["verified"] for item in document["capabilities"]))
        self.assertEqual(document["extensions"]["bootstrap_state"], "draft_unbound")
        for worker in document["workers"]:
            layer = "gold" if worker["id"] == "gold-worker" else "silver"
            self.assertEqual(
                worker["code_sha256"], metadata_worker_sha256(self.root, layer=layer)
            )
            self.assertIn("alpha", worker["notebook_name"])
        before = {
            layer: metadata_worker_sha256(self.root, layer=layer)
            for layer in ("silver", "gold")
        }
        load_use_case(self.root).input_path("gold_rules").write_text(
            GOLD_RULES + "# new reviewed code\n", encoding="utf-8"
        )
        self.assertEqual(before["silver"], metadata_worker_sha256(self.root))
        self.assertNotEqual(
            before["gold"], metadata_worker_sha256(self.root, layer="gold")
        )
        self.assertNotEqual(
            config.version_id, bootstrap_configuration(self.root).version_id
        )

    def test_staged_rule_code_hashes_match_disk_and_reject_partial_overrides(self):
        profile = load_use_case(self.root)
        sources = {
            name: profile.input_path(name).read_text(encoding="utf-8")
            for name in ("source_rules", "gold_rules")
        }
        for layer in ("silver", "gold"):
            self.assertEqual(
                metadata_worker_sha256(self.root, layer=layer),
                metadata_worker_sha256(self.root, layer=layer, case_sources=sources),
            )
        with self.assertRaises(ValueError):
            metadata_worker_sha256(
                self.root, case_sources={"source_rules": SOURCE_RULES}
            )

    def test_compiled_gold_configuration_never_loads_treasury_profile(self):
        config = gold_config(self.root, use_case="beta")
        self.assertEqual(config["use_case_id"], "beta")
        self.assertEqual(config["target_lakehouse"], "lh_gld_beta")
        self.assertEqual(config["calendar"]["fiscal_year_start_month"], 1)
        self.assertEqual(dimension_specs(self.root, "beta"), [])
        self.assertEqual(mlv_definitions(config, root=self.root), {})

    def test_compiled_configuration_rejects_cross_case_targets_and_missing_calendars(
        self,
    ):
        path = load_use_case(self.root).input_path("compiled_metadata")
        original = json.loads(path.read_text(encoding="utf-8"))
        for field in ("lakehouse", "calendar", "identity"):
            document = json.loads(json.dumps(original))
            if field == "lakehouse":
                document["objects"][0]["lakehouse"] = "lh_brz_slv_beta"
            elif field == "calendar":
                del document["extensions"]["gold_contract"]["calendar"]
            else:
                document["extensions"]["use_case_id"] = "beta"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.subTest(field=field), self.assertRaises(ValueError):
                bootstrap_configuration(self.root)

    def test_clean_bundles_do_not_inherit_treasury_rules_or_active_source_arrays(self):
        for case in ("alpha", "beta"):
            namespace = {}
            for cell in metadata_notebook_document(self.root, "gold_worker", case)[
                "cells"
            ]:
                if cell["cell_type"] == "code" and cell["id"] != "execute":
                    exec(  # noqa: S102 - trusted locally generated notebook code
                        compile(
                            "".join(cell["source"]), "reviewed-case-bundle", "exec"
                        ),
                        namespace,
                    )
            self.assertEqual(namespace["RULES"].source_business_checks({}, "bas"), [])
            self.assertEqual(namespace["RULES"].row_rules({"name": "t_leave"}), [])
            self.assertFalse(hasattr(namespace["CASE_GOLD"], "_department"))
            self.assertNotIn("SOURCE_CONFIG", namespace)
            self.assertNotIn("GOLD_CONFIG", namespace)

    def test_validation_never_imports_snapshot_sources_or_mutates_shared_assets(self):
        before = {
            path.relative_to(self.root): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (self.root / "artefacts").rglob("*")
            if path.is_file()
        }
        with (
            patch(
                "fabric_medallion_deployer.contracts.load_sources",
                side_effect=AssertionError("snapshot importer"),
            ),
            patch(
                "fabric_medallion_deployer.deployer.validate_seed_data",
                side_effect=AssertionError("seed validation"),
            ),
        ):
            for case in ("alpha", "beta"):
                self.generate(case)
                self.assertEqual(
                    runtime_source_bindings(self.root, case)[0].database,
                    "db_bas_finance",
                )
                validate_local(self.root, use_case=case)
        after = {
            path.relative_to(self.root): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (self.root / "artefacts").rglob("*")
            if path.is_file()
        }
        self.assertEqual(before, after)
        self.assertEqual(
            len(
                list(
                    (self.root / "use_cases/alpha/generated/notebooks").glob("*.ipynb")
                )
            ),
            6,
        )

    def test_default_bindings_and_stale_outputs_fail_closed(self):
        self.generate("alpha")
        kwargs = {
            "workspace_id": "11111111-1111-4111-8111-111111111111",
            "lakehouse_id": "22222222-2222-4222-8222-222222222222",
            "lakehouse_name": "lh_brz_slv_alpha",
        }
        definition = metadata_notebook_definition(self.root, "worker", **kwargs)
        notebook = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        self.assertEqual(
            notebook["metadata"]["dependencies"]["lakehouse"]["default_lakehouse_name"],
            "lh_brz_slv_alpha",
        )
        with self.assertRaisesRegex(ValueError, "selected case resource"):
            metadata_notebook_definition(
                self.root, "worker", **(kwargs | {"lakehouse_name": "lh_brz_slv_beta"})
            )
        profile = load_use_case(self.root)
        path = (
            profile.generated
            / "notebooks"
            / f"{profile.notebook_names()['worker']}.ipynb"
        )
        path.write_text('{"cells":[]}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Stale generated"):
            metadata_notebook_definition(self.root, "worker", **kwargs)
        profile.input_path("gold_rules").write_text(
            GOLD_RULES + "# changed\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "Stale generated"):
            metadata_notebook_definition(
                self.root,
                "gold_worker",
                **(
                    kwargs
                    | {
                        "lakehouse_name": "lh_gld_alpha",
                        "source_workspace_id": kwargs["workspace_id"],
                        "source_lakehouse_id": "33333333-3333-4333-8333-333333333333",
                    }
                ),
            )


class CompiledReceiptIntegrationTests(unittest.TestCase):
    """Use the real MD compiler/verifier; mutate only a disposable local case."""

    def test_normal_build_and_deploy_reject_all_compilation_drift_before_cloud(self):
        from fabric_medallion_deployer.spec_compiler import compile_use_case

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for directory in (
                "artefacts/orchestrator",
                "artefacts/medallion",
                "artefacts/gold",
            ):
                shutil.copytree(ROOT / directory, root / directory)
            for relative in (
                "use_cases/catalog.json",
                "use_cases/inventory_example/use_case.json",
                "use_cases/inventory_example/requirements/requirements.md",
                "use_cases/inventory_example/specifications/source_contract.md",
                "use_cases/inventory_example/specifications/gold_contract.md",
                "use_cases/inventory_example/plans/plan.md",
                "baselines/requirements/policy.json",
                "deployment/src/fabric_medallion_deployer/fabric.py",
                "deployment/src/fabric_medallion_deployer/metadata.py",
            ):
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / relative, target)
            policy = json.loads(
                (root / "baselines/requirements/policy.json").read_text()
            )
            for relative in policy["governing_documents"]:
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / relative, target)
            compile_use_case(root, "inventory_example")
            profile = load_use_case(root, "inventory_example")
            bootstrap_configuration(root, use_case=profile.case_id)
            spec = importlib.util.spec_from_file_location(
                "case_receipt_build_entrypoint", ROOT / "deployment/build.py"
            )
            build = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(build)
            build.ROOT = root
            mutations = (
                (
                    profile.path(profile.documents["sources"]),
                    lambda text: text + "\nReviewed source prose changed.\n",
                ),
                (
                    profile.input_path("source_rules"),
                    lambda text: text + "\n# hand-edited generated rules\n",
                ),
                (
                    profile.directory / "use_case.json",
                    lambda text: text.replace(
                        '"baseline_version": "1.1.0"', '"baseline_version": "2.0.0"'
                    ),
                ),
                (
                    root / "baselines/requirements/policy.json",
                    lambda text: text.replace(
                        "immutable_no_automatic_deletion", "changed_retention_policy"
                    ),
                ),
                (
                    profile.input_path("compiled_metadata"),
                    lambda text: json.dumps(
                        json.loads(text) | {"owner": "changed_owner"}
                    ),
                ),
                (
                    profile.generated / "compilation.json",
                    lambda text: text.replace(
                        '"use_case_id":"inventory_example"',
                        '"use_case_id":"wrong_case"',
                    ),
                ),
                (
                    profile.generated / "compilation.json",
                    lambda text: text.replace(
                        '"inputs":{', '"inputs":{"../outside":"a",'
                    ),
                ),
                (profile.generated / "compilation.json", lambda text: None),
            )
            for path, mutate in mutations:
                original = path.read_text(encoding="utf-8")
                changed = mutate(original)
                self.assertNotEqual(original, changed, str(path))
                try:
                    if changed is None:
                        path.unlink()
                    else:
                        path.write_text(changed, encoding="utf-8")
                    client = Mock()
                    with (
                        self.subTest(path=path),
                        redirect_stderr(io.StringIO()),
                        patch("sys.argv", ["build.py", "--use-case", profile.case_id]),
                    ):
                        self.assertEqual(build.main(), 2)
                    with self.assertRaises(ValueError):
                        AcceleratorDeployer(
                            root,
                            DeploymentOptions(
                                workspace="unused-offline",
                                use_case=profile.case_id,
                                metadata_assets=True,
                                dry_run=True,
                            ),
                            client=client,
                        ).deploy()
                    client.resolve_workspace.assert_not_called()
                    client.request.assert_not_called()
                finally:
                    path.write_text(original, encoding="utf-8")
