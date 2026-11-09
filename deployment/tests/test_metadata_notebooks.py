"""Self-contained generic notebook generation and clean-kernel execution contracts."""

from __future__ import annotations

import base64
import json
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nbformat
import test_manifest_ingestion as ingestion_fixtures
import test_run_manifests as manifests
from fabric_medallion_deployer.orchestrator import (
    METADATA_NOTEBOOKS,
    metadata_notebook_definition,
    metadata_notebook_document,
    metadata_worker_sha256,
)
from fabric_medallion_deployer.use_cases import load_use_case

ROOT = Path(__file__).resolve().parents[2]


class MetadataNotebookTests(manifests.ManifestFixture):
    def test_changed_case_targets_require_rebinding_before_bronze_dispatch(self):
        from unittest.mock import Mock

        namespace = {
            "notebookutils": SimpleNamespace(
                fs=Mock(),
                runtime=SimpleNamespace(
                    context={"defaultLakehouseName": "lh_brz_slv_treasury"}
                ),
            ),
            "spark": Mock(),
        }
        document = metadata_notebook_document(ROOT, "prepare")
        wrong = SimpleNamespace(
            to_document=lambda: {
                "configuration": {
                    "extensions": {"use_case_id": namespace["CASE_ID"]},
                    "objects": [
                        {
                            "layer": "bronze",
                            "lakehouse": "lh_other_case",
                            "enabled": True,
                        }
                    ],
                }
            }
        )
        with self.assertRaisesRegex(RuntimeError, "target resources"):
            for cell in document["cells"]:
                if cell["cell_type"] == "code":
                    exec("".join(cell["source"]), namespace)  # noqa: S102 - trusted generated bundle
                    if cell["id"] == "packaged-runtime":
                        namespace["ORCHESTRATION"].read_run_manifest = (
                            lambda *_args, **_kwargs: wrong
                        )
        self.assertEqual(namespace["spark"].method_calls, [])
        namespace["notebookutils"].fs.put.assert_not_called()

    def test_wrong_case_snapshot_is_rejected_before_dispatch_or_data_writes(self):
        wrong = SimpleNamespace(
            to_document=lambda: {
                "configuration": {
                    "extensions": {"use_case_id": "another_case"},
                    "feeds": [],
                },
                "tasks": [],
            }
        )
        for step in METADATA_NOTEBOOKS:
            is_gold = step in {"gold", "gold_worker"}
            from unittest.mock import Mock

            utilities = SimpleNamespace(
                fs=Mock(),
                runtime=SimpleNamespace(
                    context={
                        "defaultLakehouseName": "lh_gld_treasury"
                        if is_gold
                        else "lh_brz_slv_treasury"
                    }
                ),
            )
            namespace = {"notebookutils": utilities, "spark": Mock()}
            document = metadata_notebook_document(ROOT, step)
            with (
                self.subTest(step=step),
                self.assertRaisesRegex(RuntimeError, "another use case"),
            ):
                for cell in document["cells"]:
                    if cell["cell_type"] != "code":
                        continue
                    exec("".join(cell["source"]), namespace)  # noqa: S102 - trusted generated bundle
                    if cell["id"] == "parameters" and is_gold:
                        namespace.update(
                            source_workspace_id=str(uuid.UUID(int=31)),
                            source_lakehouse_id=str(uuid.UUID(int=32)),
                        )
                    elif cell["id"] == "packaged-runtime":
                        namespace["ORCHESTRATION"].read_run_manifest = (
                            lambda *_args, **_kwargs: wrong
                        )
            self.assertEqual(namespace["spark"].method_calls, [])
            utilities.fs.put.assert_not_called()

    def test_file_prepare_requires_persisted_arrivals_in_a_clean_namespace(self):
        fixture = ingestion_fixtures.ManifestIngestionTests(
            "test_file_receipts_bind_arrival_policy_to_pinned_object_metadata"
        )
        fixture.setUpClass()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        task, arrival, _ = fixture.file_source()
        reference = fixture.runtime.write_file_arrival_manifest(
            fixture.fs, fixture.manifest, {task["object_id"]: arrival}
        )
        returned = []

        class NotebookExit(Exception):
            pass

        def exit_value(value):
            returned.append(json.loads(value))
            raise NotebookExit

        utilities = SimpleNamespace(
            fs=fixture.fs,
            runtime=SimpleNamespace(
                context={"defaultLakehouseName": "lh_brz_slv_treasury"}
            ),
            notebook=SimpleNamespace(exit=exit_value),
        )
        document = metadata_notebook_document(ROOT, "prepare")
        for supplied in (False, True):
            namespace = {"notebookutils": utilities}
            with (
                self.subTest(supplied=supplied),
                self.assertRaises(NotebookExit if supplied else RuntimeError),
            ):
                for cell in document["cells"]:
                    if cell["cell_type"] != "code":
                        continue
                    exec(  # noqa: S102 - trusted generated cells
                        compile("".join(cell["source"]), "file-prepare", "exec"),
                        namespace,
                    )
                    if cell["id"] == "parameters":
                        namespace.update(
                            run_id=fixture.plan.run_id,
                            version_id=fixture.config.version_id,
                            manifest_path=f"Files/orchestration/{fixture.plan.run_id}/file-manifest.json",
                            snapshot_sha256=fixture.plan.snapshot_sha256,
                            file_arrival_path=reference["path"] if supplied else "",
                            file_arrival_sha256=reference["sha256"] if supplied else "",
                        )
        self.assertEqual(len(returned), 1)
        self.assertEqual(returned[0]["run_id"], fixture.plan.run_id)

    def test_all_generic_notebooks_compile_with_no_active_source_contracts(self):
        for step, name in METADATA_NOTEBOOKS.items():
            document = metadata_notebook_document(ROOT, step)
            nbformat.validate(nbformat.from_dict(document))
            namespace = {}
            for cell in document["cells"]:
                if cell["cell_type"] != "code":
                    continue
                self.assertEqual(cell["outputs"], [])
                code = "".join(cell["source"])
                compile(code, name, "exec")
                self.assertNotIn("SOURCE_CONFIG =", code)
                if cell["id"] != "execute":
                    exec(compile(code, name, "exec"), namespace)  # noqa: S102 - trusted generated cells
            self.assertEqual(
                namespace["WORKER_CODE_SHA256"],
                metadata_worker_sha256(
                    ROOT, layer="gold" if step in {"gold", "gold_worker"} else "silver"
                ),
            )
            self.assertTrue(callable(namespace["INGESTION"].load_silver))
            self.assertTrue(callable(namespace["ORCHESTRATION"].build_silver_dag))
            self.assertTrue(callable(namespace["RULES"].validate_rule_specs))

    def test_prepare_runs_top_to_bottom_from_a_clean_namespace(self):
        path = self.runtime.write_run_manifest(self.fs, self.plan)
        returned = []

        class NotebookExit(Exception):
            pass

        def exit_value(value):
            returned.append(json.loads(value))
            raise NotebookExit

        utilities = SimpleNamespace(
            fs=self.fs,
            runtime=SimpleNamespace(
                context={"defaultLakehouseName": "lh_brz_slv_treasury"}
            ),
            notebook=SimpleNamespace(exit=exit_value),
        )
        namespace = {"notebookutils": utilities}
        document = metadata_notebook_document(ROOT, "prepare")
        with self.assertRaises(NotebookExit):
            for cell in document["cells"]:
                if cell["cell_type"] != "code":
                    continue
                exec(  # noqa: S102 - trusted generated cells
                    compile("".join(cell["source"]), "prepare-notebook", "exec"),
                    namespace,
                )
                if cell["id"] == "parameters":
                    namespace.update(
                        run_id=self.plan.run_id,
                        version_id=self.config.version_id,
                        manifest_path=path,
                        snapshot_sha256=self.plan.snapshot_sha256,
                    )
        self.assertEqual(returned[0]["run_id"], self.plan.run_id)
        self.assertEqual(returned[0]["pages"], [{"after_ordinal": 0, "page_size": 1}])
        self.assertEqual(returned[0]["manifest_path"], path)

    def test_silver_parent_checks_acknowledgements_against_pinned_dispatch(self):
        manifest_path = self.runtime.write_run_manifest(self.fs, self.plan)
        manifest = self.read(manifest_path)
        pipeline, dispatch, dag = object(), object(), {"activities": []}
        calls, returned = [], []

        class NotebookExit(Exception):
            pass

        def exit_value(value):
            returned.append(json.loads(value))
            raise NotebookExit

        notebook = SimpleNamespace(exit=exit_value)
        utilities = SimpleNamespace(
            fs=self.fs,
            runtime=SimpleNamespace(
                context={"defaultLakehouseName": "lh_brz_slv_treasury"}
            ),
            notebook=notebook,
        )
        namespace = {"notebookutils": utilities, "spark": None}
        document = metadata_notebook_document(ROOT, "silver")
        with self.assertRaises(NotebookExit):
            for cell in document["cells"]:
                if cell["cell_type"] != "code":
                    continue
                exec(  # noqa: S102 - trusted generated cells
                    compile("".join(cell["source"]), "silver-parent", "exec"),
                    namespace,
                )
                if cell["id"] == "parameters":
                    namespace.update(
                        run_id=self.plan.run_id,
                        version_id=self.config.version_id,
                        manifest_path=manifest_path,
                        snapshot_sha256=self.plan.snapshot_sha256,
                    )
                elif cell["id"] == "packaged-runtime":
                    runtime = namespace["ORCHESTRATION"]
                    runtime.read_run_manifest = lambda *_args, **_kwargs: manifest
                    runtime.read_pipeline_dispatch = lambda *_args, **_kwargs: pipeline
                    runtime.notebook_layer_dispatch = lambda *_args: dispatch
                    runtime.write_layer_dispatch = lambda *_args: "dispatch.json"
                    runtime.prepare_silver_contexts = lambda *_args, **_kwargs: {}
                    runtime.initialize_silver_sources = lambda *_args, **_kwargs: None
                    runtime.build_silver_dag = lambda *_args, **_kwargs: dag
                    runtime.run_notebook_dag = lambda *args: calls.append(args)
                    runtime.finalize_silver_sources = lambda *_args, **_kwargs: {
                        "status": "SOURCE_COMMITTED"
                    }
        self.assertEqual(calls, [(notebook, dag, dispatch)])
        self.assertEqual(returned, [{"status": "SOURCE_COMMITTED"}])

    def test_worker_requires_dispatch_proof_before_any_data_write(self):
        path = self.runtime.write_run_manifest(self.fs, self.plan)
        utilities = SimpleNamespace(
            fs=self.fs,
            runtime=SimpleNamespace(
                context={"defaultLakehouseName": "lh_brz_slv_treasury"}
            ),
        )
        namespace = {"notebookutils": utilities, "spark": None}
        document = metadata_notebook_document(ROOT, "worker")
        with self.assertRaises(RuntimeError):
            for cell in document["cells"]:
                if cell["cell_type"] != "code":
                    continue
                exec(  # noqa: S102 - trusted generated cells
                    compile("".join(cell["source"]), "worker-notebook", "exec"),
                    namespace,
                )
                if cell["id"] == "parameters":
                    namespace.update(
                        run_id=self.plan.run_id,
                        version_id=self.config.version_id,
                        manifest_path=path,
                        snapshot_sha256=self.plan.snapshot_sha256,
                    )

    def test_definition_binds_only_the_resolved_default_lakehouse(self):
        workspace, lakehouse = str(uuid.UUID(int=31)), str(uuid.UUID(int=32))
        definition = metadata_notebook_definition(
            ROOT,
            "worker",
            workspace_id=workspace,
            lakehouse_id=lakehouse,
            lakehouse_name="lh_brz_slv_treasury",
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        binding = document["metadata"]["dependencies"]["lakehouse"]
        self.assertEqual(binding["default_lakehouse"], lakehouse)
        self.assertEqual(binding["default_lakehouse_workspace_id"], workspace)
        with self.assertRaises(ValueError):
            metadata_notebook_definition(
                ROOT,
                "worker",
                workspace_id=workspace,
                lakehouse_id="not-a-guid",
                lakehouse_name="lh_brz_slv_treasury",
            )

    def test_generic_publication_rejects_stale_generated_notebook(self):
        original = Path.read_text
        target = (
            load_use_case(ROOT).generated
            / "notebooks"
            / f"{METADATA_NOTEBOOKS['worker']}.ipynb"
        )

        def read(path, *args, **kwargs):
            return '{"cells":[]}' if path == target else original(path, *args, **kwargs)

        with (
            patch.object(Path, "read_text", read),
            self.assertRaisesRegex(ValueError, "Stale generated"),
        ):
            metadata_notebook_definition(
                ROOT,
                "worker",
                workspace_id=str(uuid.UUID(int=31)),
                lakehouse_id=str(uuid.UUID(int=32)),
                lakehouse_name="lh_brz_slv_treasury",
            )


if __name__ == "__main__":
    unittest.main()
