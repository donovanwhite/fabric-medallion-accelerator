"""Pinned execution selection, deterministic lineage and bounded Lookup pages."""

from __future__ import annotations

import copy
import json
import unittest

from fabric_medallion_deployer.metadata import (
    MetadataError,
    compile_run_plan,
    load_configuration,
)
from metadata_fixtures import metadata_document


class RunPlanTests(unittest.TestCase):
    def compile(self, document=None, **options):
        config = load_configuration(
            metadata_document() if document is None else document
        )
        return compile_run_plan(config, "run-1", "2026-11-06T08:30:00.123Z", **options)

    def test_dependency_closure_order_and_ids_are_deterministic(self) -> None:
        plan = self.compile(object_ids=["bas-silver-postings"])
        self.assertEqual(
            plan.object_ids, ("bas-bronze-postings", "bas-silver-postings")
        )
        tasks = plan.to_document()["tasks"]
        self.assertEqual(tasks[1]["dependencies"][0]["task_id"], tasks[0]["task_id"])
        self.assertEqual(len(tasks[0]["task_id"]), 64)
        self.assertEqual(
            plan.snapshot_sha256,
            self.compile(object_ids=["bas-silver-postings"]).snapshot_sha256,
        )
        self.assertTrue(
            tasks[0]["target_path"].startswith("Files/bronze/bas_finance_sql/20261106/")
        )
        self.assertFalse(plan.can_execute)
        self.assertTrue(any("approval" in reason for reason in plan.blockers))
        self.assertTrue(any("capability" in reason for reason in plan.blockers))

    def test_snapshot_is_detached_from_active_metadata_and_returned_objects(
        self,
    ) -> None:
        document = metadata_document()
        plan = self.compile(document)
        before = plan.snapshot_sha256
        document["feeds"][0]["description_slug"] = "changed"
        returned = plan.to_document()
        returned["configuration"]["objects"].clear()
        self.assertEqual(plan.snapshot_sha256, before)
        self.assertEqual(len(plan.to_document()["configuration"]["objects"]), 2)

    def test_disabled_required_object_and_disabled_feed_block_selection(self) -> None:
        document = metadata_document()
        document["objects"][0]["enabled"] = False
        with self.assertRaisesRegex(MetadataError, "disabled"):
            self.compile(document, object_ids=["bas-silver-postings"])
        document = metadata_document()
        document["feeds"][0]["enabled"] = False
        with self.assertRaisesRegex(MetadataError, "disabled"):
            self.compile(document, object_ids=["bas-silver-postings"])

    def test_ready_plan_needs_approved_hash_bound_connections_and_evidence(
        self,
    ) -> None:
        document = metadata_document()
        document["capabilities"][0].update(verified=True, evidence_sha256="e" * 64)
        config = load_configuration(document)
        plan = compile_run_plan(
            config,
            "run-1",
            "2026-11-06T08:30:00.123Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=config.definition_sha256,
            require_ready=True,
        )
        self.assertTrue(plan.can_execute)
        with self.assertRaisesRegex(MetadataError, "approval|approved"):
            compile_run_plan(
                config,
                "run-1",
                "2026-11-06T08:30:00.123Z",
                lifecycle_state="APPROVED",
                approved_definition_sha256="f" * 64,
                require_ready=True,
            )
        document["connections"][0]["fabric_connection_id"] = None
        with self.assertRaisesRegex(MetadataError, "connection"):
            self.compile(document, require_ready=True)

    def test_invalid_identity_timestamp_selection_and_supplemental_feed_fail(
        self,
    ) -> None:
        config = load_configuration(metadata_document())
        for run, timestamp in (
            ("../other", "2026-11-06T08:30:00.123Z"),
            ("run", "2026-11-06"),
            ("run", "2026-11-06T10:30:00+02:00"),
            ("run", "2026-02-30T08:30:00Z"),
        ):
            with (
                self.subTest(run=run, timestamp=timestamp),
                self.assertRaises(MetadataError),
            ):
                compile_run_plan(config, run, timestamp)
        for selection in ([], ["missing"], ["bas-silver-postings"] * 2):
            with self.subTest(selection=selection), self.assertRaises(MetadataError):
                self.compile(object_ids=selection)
        document = metadata_document()
        document["feeds"][0]["authority"] = "supplemental"
        with self.assertRaisesRegex(MetadataError, "authority"):
            self.compile(document)

    def test_lookup_pages_never_drop_rows_or_exceed_the_bound(self) -> None:
        document = metadata_document()
        template = document["objects"][0]
        document.update(objects=[], rules=[], dependencies=[])
        document["extensions"]["hash_policies"] = {}
        for n in range(5001):
            obj = copy.deepcopy(template)
            obj.update(id=f"bronze-{n}", name=f"table_{n}", execution_priority=n)
            document["objects"].append(obj)
        plan = self.compile(document)
        pages = plan.pages(page_size=1000, max_bytes=1024 * 1024)
        self.assertEqual(sum(len(page) for page in pages), 5001)
        self.assertEqual(
            len({task["task_id"] for page in pages for task in page}), 5001
        )
        for page in pages:
            self.assertLessEqual(len(page), 1000)
            self.assertLessEqual(len(json.dumps(list(page)).encode()), 1024 * 1024)
        for count in (0, 5001, True):
            with self.assertRaises(MetadataError):
                plan.pages(page_size=count)

    def test_priority_does_not_override_same_layer_dependencies(self) -> None:
        document = metadata_document()
        parent = copy.deepcopy(document["objects"][1])
        parent.update(id="silver-parent", name="parent", execution_priority=999)
        document["objects"].append(parent)
        document["dependencies"].append(
            {
                "upstream_id": "silver-parent",
                "downstream_id": "bas-silver-postings",
                "condition": "succeeded",
            }
        )
        plan = self.compile(document)
        self.assertLess(
            plan.object_ids.index("silver-parent"),
            plan.object_ids.index("bas-silver-postings"),
        )


if __name__ == "__main__":
    unittest.main()
