"""Local all-source compiler acceptance, explicitly not native Fabric execution."""

from __future__ import annotations

import unittest
from copy import deepcopy
from pathlib import Path

from fabric_medallion_deployer.configuration import bind_configuration
from fabric_medallion_deployer.contracts import load_sources
from fabric_medallion_deployer.deployer import validation_id
from fabric_medallion_deployer.metadata import (
    compile_run_plan,
    copy_task_descriptors,
    load_configuration,
    metadata_digest,
)
from fabric_medallion_deployer.orchestrator import (
    bootstrap_configuration,
    metadata_worker_sha256,
)
from test_gold_runtime import gold

ROOT = Path(__file__).resolve().parents[2]


class LocalOrchestrationAcceptanceTests(unittest.TestCase):
    def test_all_44_sources_feed_one_pinned_gold_plan_with_preserved_identity(self):
        baseline = bootstrap_configuration(ROOT)
        sources = load_sources(ROOT)
        bound = bind_configuration(
            baseline,
            workspace_id=validation_id("local-workspace"),
            source_database_ids={s.code: validation_id(s.database) for s in sources},
            connection_ids={s.code: validation_id(s.connection) for s in sources},
        )
        document = bound.to_document()
        # Test-only platform evidence: never persisted to the deployable seed.
        for capability in document["capabilities"]:
            capability.update(verified=True, evidence_sha256="e" * 64)
        for obj in document["objects"]:
            obj["enabled"] = obj["materialization"] != "mlv"
        fixture = load_configuration(document)
        plan = compile_run_plan(
            fixture,
            "local-compiler-only",
            "2026-11-09T08:00:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=fixture.definition_sha256,
            require_ready=True,
        )
        descriptors = copy_task_descriptors(plan)
        self.assertEqual(len(descriptors), 44)
        self.assertEqual(
            {descriptor["connection_id"] for descriptor in descriptors},
            {validation_id(source.connection) for source in sources},
        )
        snapshot = plan.to_document()
        config = gold.gold_config_from_manifest(
            snapshot,
            snapshot_sha256=plan.snapshot_sha256,
            code_sha256=metadata_worker_sha256(ROOT, layer="gold"),
        )
        self.assertEqual(
            {
                (source["source_code"], table["name"])
                for source in config["sources"]
                for table in source["tables"]
            },
            {
                (source.code, table["name"])
                for source in sources
                for table in source.tables
            },
        )
        self.assertEqual(
            len({task["target_lock_key"] for task in snapshot["tasks"]}),
            len(snapshot["tasks"]),
        )
        self.assertEqual(
            {
                source["source_code"]: source["contract_hash"]
                for source in config["sources"]
            },
            {source.code: source.contract_hash for source in sources},
        )
        changed = deepcopy(document)
        changed["version_id"] = "next-version"
        self.assertEqual(plan.to_document(), snapshot)
        self.assertNotEqual(metadata_digest(changed), snapshot["definition_sha256"])
        self.assertTrue(
            all(not capability.verified for capability in baseline.capabilities)
        )


if __name__ == "__main__":
    unittest.main()
