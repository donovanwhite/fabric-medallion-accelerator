"""Immutable notebook manifest transport with real local files and bounded reads."""

from __future__ import annotations

import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from fabric_medallion_deployer.metadata import compile_run_plan, load_configuration
from metadata_fixtures import metadata_document

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "artefacts" / "orchestrator" / "silver_orchestration.py"


class LocalFs:
    def __init__(self, root: Path) -> None:
        self.root = root

    def exists(self, path: str) -> bool:
        return (self.root / path).exists()

    def ls(self, path: str):
        return [
            SimpleNamespace(name=p.name, size=p.stat().st_size, isDir=p.is_dir())
            for p in (self.root / path).iterdir()
        ]

    def head(self, path: str, size: int) -> str:
        return (self.root / path).read_bytes()[:size].decode("utf-8")

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open(
            "w" if overwrite else "x", encoding="utf-8", newline=""
        ) as stream:
            stream.write(contents)
        return True

    def mkdirs(self, path: str) -> bool:
        (self.root / path).mkdir(parents=True, exist_ok=True)
        return True


class ManifestFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        spec = importlib.util.spec_from_file_location("treasury_orchestration", MODULE)
        cls.runtime = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.runtime
        spec.loader.exec_module(cls.runtime)

    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.fs = LocalFs(Path(self.folder.name))
        document = metadata_document()
        document["capabilities"][0].update(verified=True, evidence_sha256="e" * 64)
        self.config = load_configuration(document)
        self.plan = compile_run_plan(
            self.config,
            "run-1",
            "2026-11-06T08:30:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=self.config.definition_sha256,
            require_ready=True,
        )

    def read(self, path, digest=None, **options):
        return self.runtime.read_run_manifest(
            self.fs,
            path,
            expected_snapshot_sha256=digest or self.plan.snapshot_sha256,
            run_id=self.plan.run_id,
            version_id=self.config.version_id,
            load_configuration=load_configuration,
            **options,
        )


class ManifestTests(ManifestFixture):
    def test_write_once_readback_and_detached_task_resolution(self) -> None:
        path = self.runtime.write_run_manifest(self.fs, self.plan)
        self.assertEqual(self.runtime.write_run_manifest(self.fs, self.plan), path)
        manifest = self.read(path)
        task = self.plan.to_document()["tasks"][1]
        result = self.runtime.resolve_worker_task(
            manifest,
            task["task_id"],
            worker_id="silver-worker",
            code_sha256="b" * 64,
            lakehouse="lh_brz_slv_treasury",
        )
        self.assertEqual(result["object"]["name"], "postings")
        self.assertEqual(result["task"]["source_batch_id"], task["source_batch_id"])
        result["object"]["columns"].clear()
        self.assertEqual(
            len(manifest.to_document()["configuration"]["objects"][1]["columns"]), 2
        )

    def test_tampering_wrong_context_and_worker_code_fail(self) -> None:
        path = self.runtime.write_run_manifest(self.fs, self.plan)
        for field in ("run_id", "version_id"):
            options = {
                "expected_snapshot_sha256": self.plan.snapshot_sha256,
                "run_id": self.plan.run_id,
                "version_id": self.config.version_id,
                "load_configuration": load_configuration,
            }
            options[field] = "different"
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                self.runtime.read_run_manifest(self.fs, path, **options)
        manifest = self.read(path)
        with self.assertRaises(RuntimeError):
            self.runtime.resolve_worker_task(
                manifest,
                self.plan.to_document()["tasks"][1]["task_id"],
                worker_id="silver-worker",
                code_sha256="f" * 64,
                lakehouse="lh_brz_slv_treasury",
            )
        self.fs.put(path, self.plan.snapshot_json.replace('"run-1"', '"other"'), True)
        with self.assertRaisesRegex(RuntimeError, "hash"):
            self.read(path)
        with self.assertRaisesRegex(RuntimeError, "overwrite|changed"):
            self.runtime.write_run_manifest(self.fs, self.plan)

    def test_copy_csv_transport_handles_bom_and_does_not_limit_to_64k(self) -> None:
        path = "Files/orchestration/run-1/export.csv"
        target = Path(self.folder.name) / path
        target.parent.mkdir(parents=True)
        payload = self.plan.to_document()
        payload["configuration"]["extensions"]["large_metadata"] = "x" * 100000
        config = load_configuration(payload["configuration"])
        plan = compile_run_plan(
            config,
            "run-1",
            "2026-11-06T08:30:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=config.definition_sha256,
            require_ready=True,
        )
        with target.open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.writer(stream, escapechar="\\", quoting=csv.QUOTE_ALL)
            writer.writerow(["snapshot_json"])
            writer.writerow([plan.snapshot_json])
        manifest = self.read(path, plan.snapshot_sha256, export_column="snapshot_json")
        self.assertGreater(len(manifest.snapshot_json), 65536)

    def test_oversized_truncated_and_unready_manifests_do_not_execute(self) -> None:
        path = self.runtime.write_run_manifest(self.fs, self.plan)
        with self.assertRaisesRegex(RuntimeError, "size"):
            self.read(path, max_bytes=100)
        original = self.fs.head
        self.fs.head = lambda p, n: original(p, min(n, 100))
        with self.assertRaisesRegex(RuntimeError, "truncated"):
            self.read(path)
        self.fs.head = original
        preview = compile_run_plan(self.config, "run-1", "2026-11-06T08:30:00Z")
        other = "Files/orchestration/run-1/preview.json"
        self.fs.put(other, preview.snapshot_json)
        with self.assertRaisesRegex(RuntimeError, "ready"):
            self.read(other, preview.snapshot_sha256)

    def test_paths_cannot_escape_or_read_another_run(self) -> None:
        for path in (
            "Files/orchestration/../other/file.json",
            "Files/orchestration/other/file.json",
            "Files/orchestration/run-1/%2e%2e/other/file.json",
            "https://example.invalid/manifest.json",
            "Files\\orchestration\\run-1\\file.json",
        ):
            with self.subTest(path=path), self.assertRaises(RuntimeError):
                self.read(path)


if __name__ == "__main__":
    unittest.main()
