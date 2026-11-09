"""Opt-in generic publication: isolated service, no data execution or activation."""

from __future__ import annotations

import base64
import json
import tempfile
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from fabric_medallion_deployer.contracts import load_sources
from fabric_medallion_deployer.deployer import (
    METADATA_COPY_PIPELINE,
    METADATA_MASTER_PIPELINE,
    AcceleratorDeployer,
    DeploymentOptions,
    validation_id,
)
from fabric_medallion_deployer.fabric import (
    ApiResponse,
    FabricApiError,
    FabricClient,
    FabricItem,
)
from fabric_medallion_deployer.orchestrator import METADATA_NOTEBOOKS
from test_deployer import InventoryClient

ROOT = Path(__file__).resolve().parents[2]


class PublicationClient(InventoryClient):
    """Canonical fake identities and observable mutation boundaries."""

    def __init__(self) -> None:
        super().__init__(
            [
                FabricItem(validation_id(s.database), s.database, "SQLDatabase")
                for s in load_sources(ROOT, use_case="treasury")
            ]
        )
        self.checked: list[str] = []
        self.definitions: dict[str, dict] = {}
        self.creations: list[str] = []

    def resolve_workspace(self, workspace: str) -> dict:
        return {
            "id": validation_id("workspace"),
            "displayName": workspace,
            "capacityId": "capacity",
        }

    def resolve_oauth2_connection(self, name: str, kind: str) -> dict:
        return {"id": validation_id(name), "displayName": name}

    def validate_connection_online(self, connection: dict) -> None:
        self.checked.append(connection["id"])

    def request(self, method: str, path: str, **kwargs) -> ApiResponse:
        if method != "GET" or "/lakehouses/" not in path:
            raise AssertionError(
                f"Unexpected data/job/activation request: {method} {path}"
            )
        return super().request(method, path, **kwargs)

    def create_folder(self, workspace: str, name: str) -> str:
        self.mutations.append("folder")
        identity = validation_id("folder/" + name)
        self.folders.append({"id": identity, "displayName": name})
        return identity

    def create_item(
        self, workspace: str, route: str, name: str, **kwargs
    ) -> FabricItem:
        self.mutations.append(route)
        self.creations.append(name)
        item = FabricItem(
            validation_id(name), name, self.routes[route], kwargs.get("folder_id")
        )
        self.items.append(item)
        if kwargs.get("definition"):
            self.definitions[item.id] = kwargs["definition"]
        return item

    def update_definition(self, workspace: str, item: str, definition: dict) -> None:
        self.mutations.append("update")
        self.definitions[item] = definition


class MetadataPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "publication.json"
        self.client = PublicationClient()

    def deploy(self, **overrides) -> dict[str, FabricItem]:
        options = {
            "use_case": "treasury",
            "bootstrap_orchestrator": True,
            "metadata_assets": True,
            "state_file": self.path,
        }
        options.update(overrides)
        return AcceleratorDeployer(
            ROOT,
            DeploymentOptions("test", **options),
            client=cast(FabricClient, self.client),
        ).deploy()

    def test_publication_never_reseeds_sources_starts_jobs_or_activates(self) -> None:
        with (
            patch(
                "fabric_medallion_deployer.definitions.sql_database_definition"
            ) as seed,
            patch("fabric_medallion_deployer.deployer.validate_seed_data") as data,
        ):
            result = self.deploy()
        seed.assert_not_called()
        data.assert_not_called()
        self.assertEqual(len(result), 11)
        self.assertEqual(sum(i.type == "Notebook" for i in result.values()), 6)
        self.assertEqual(sum(i.type == "DataPipeline" for i in result.values()), 2)
        self.assertEqual(self.client.creations.count("db_orchestrator"), 1)
        self.assertEqual(self.client.mutations.count("sqlDatabases"), 1)
        self.assertEqual(len(set(self.client.checked)), 4)
        self.assertFalse({"run", "activate", "delete"} & set(self.client.mutations))
        state = json.loads(self.path.read_text())
        self.assertEqual(
            state["resolved_items"], {name: item.id for name, item in result.items()}
        )
        before = list(self.client.mutations)
        self.assertEqual(self.deploy(resume=True), result)
        self.assertEqual(self.client.mutations, before)
        for step in ("gold", "gold_worker"):
            notebook = json.loads(
                base64.b64decode(
                    self.client.definitions[validation_id(METADATA_NOTEBOOKS[step])][
                        "parts"
                    ][0]["payload"]
                )
            )
            lakehouse = notebook["metadata"]["dependencies"]["lakehouse"]
            self.assertEqual(
                lakehouse["default_lakehouse"], validation_id("lh_gld_treasury")
            )
            self.assertIn(validation_id("lh_brz_slv_treasury"), json.dumps(notebook))
        for pipeline in (METADATA_COPY_PIPELINE, METADATA_MASTER_PIPELINE):
            document = base64.b64decode(
                self.client.definitions[validation_id(pipeline)]["parts"][0]["payload"]
            ).decode()
            self.assertIn(validation_id("db_orchestrator"), document)
            self.assertIn(validation_id("conn_orchestrator_sql"), document)

    def test_dry_run_has_no_mutations_or_journal_writes(self) -> None:
        self.assertEqual(self.deploy(dry_run=True), {})
        self.assertEqual(self.client.mutations, [])
        self.assertFalse(self.path.exists())
        self.assertEqual(len(self.client.checked), 4)

    def test_incompatible_or_duplicate_sources_are_not_reseeded(self) -> None:
        original = self.client.items[0]
        for replacement in (
            [FabricItem(original.id, original.display_name, "Report")],
            [original, original],
        ):
            with self.subTest(replacement=replacement):
                client = PublicationClient()
                client.items = replacement + client.items[1:]
                with (
                    patch.object(self, "client", client),
                    self.assertRaisesRegex(FabricApiError, "uniquely"),
                ):
                    self.deploy()
                self.assertEqual(client.mutations, [])

    def test_missing_active_capacity_blocks_publication(self) -> None:
        with (
            patch.object(self.client, "paged", return_value=[]),
            self.assertRaisesRegex(FabricApiError, "capacity"),
        ):
            self.deploy()
        self.assertEqual(self.client.mutations, [])

    def test_bad_lakehouse_and_duplicate_folders_block_before_writes(self) -> None:
        self.client.items.append(
            FabricItem(
                validation_id("lh_brz_slv_treasury"), "lh_brz_slv_treasury", "Lakehouse"
            )
        )
        with (
            patch.object(
                self.client,
                "request",
                return_value=ApiResponse(200, {}, b'{"properties":{}}'),
            ),
            self.assertRaisesRegex(FabricApiError, "schema-enabled"),
        ):
            self.deploy()
        self.assertEqual(self.client.mutations, [])
        self.client.folders = [
            {"id": "one", "displayName": "notebooks"},
            {"id": "two", "displayName": "notebooks"},
        ]
        with self.assertRaisesRegex(FabricApiError, "root folders"):
            self.deploy()
        self.assertEqual(self.client.mutations, [])

    def test_generic_name_type_and_overwrite_conflicts_block_preflight(self) -> None:
        for kind, message in (
            ("Report", "compatible Notebook"),
            ("Notebook", "overwrite"),
        ):
            with self.subTest(kind=kind):
                client = PublicationClient()
                client.items.append(
                    FabricItem(
                        validation_id("prepare"), METADATA_NOTEBOOKS["prepare"], kind
                    )
                )
                with (
                    patch.object(self, "client", client),
                    self.assertRaisesRegex(FabricApiError, message),
                ):
                    self.deploy(dry_run=True)
                self.assertEqual(client.mutations, [])

    def test_overwrite_never_updates_or_moves_source_databases(self) -> None:
        self.client.items.append(
            FabricItem(
                validation_id("existing-prepare"),
                METADATA_NOTEBOOKS["prepare"],
                "Notebook",
            )
        )
        self.deploy(overwrite=True)
        self.assertIn(validation_id("existing-prepare"), self.client.definitions)
        source_ids = {
            validation_id(s.database) for s in load_sources(ROOT, use_case="treasury")
        }
        self.assertFalse(source_ids & self.client.definitions.keys())
        self.assertTrue(
            all(i.folder_id is None for i in self.client.items if i.id in source_ids)
        )

    def test_journal_requires_explicit_resume_and_pins_all_binding_ids(self) -> None:
        self.deploy()
        before = list(self.client.mutations)
        with self.assertRaisesRegex(ValueError, "use --resume"):
            self.deploy()
        with self.assertRaisesRegex(ValueError, "bindings differ"):
            self.deploy(resume=True, bas_sql_connection="another")
        old = self.client.items[0]
        self.client.items[0] = FabricItem(
            validation_id("replacement"), old.display_name, old.type
        )
        with self.assertRaisesRegex(ValueError, "bindings differ"):
            self.deploy(resume=True)
        self.assertEqual(self.client.mutations, before)

    def test_missing_journal_and_programmatic_mode_constraints_fail_closed(
        self,
    ) -> None:
        with self.assertRaises(FileNotFoundError):
            self.deploy(resume=True)
        for options in (
            {"state_file": None},
            {"resume": True, "state_file": None, "dry_run": True},
            {"dry_run": True, "interactive": True},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.deploy(**options)
        self.assertEqual(self.client.mutations, [])

    def interrupt_create(self, *, receipt: bool) -> None:
        create = self.client.create_item

        def interrupted(workspace: str, route: str, name: str, **kwargs) -> FabricItem:
            item = create(workspace, route, name, **kwargs)
            if name == METADATA_NOTEBOOKS["prepare"]:
                if receipt:
                    self.client.operation_observer(
                        {
                            "method": "POST",
                            "request_path": f"/workspaces/{workspace}/{route}",
                            "operation_location": "https://api.fabric.microsoft.com/v1/operations/accepted",
                        }
                    )
                    self.client._wait_for_operation("unused", "initial monitor")
                raise FabricApiError("connection lost after submission")
            return item

        def monitor(*args) -> dict:
            state = json.loads(self.path.read_text())
            operation = state["operations"][f"create:{METADATA_NOTEBOOKS['prepare']}"]
            self.assertIsNotNone(operation["receipt"])
            self.assertEqual(
                state["resolved_items"]["db_orchestrator"],
                validation_id("db_orchestrator"),
            )
            self.assertEqual(
                state["resolved_items"]["lh_gld_treasury"],
                validation_id("lh_gld_treasury"),
            )
            raise FabricApiError("monitor timed out")

        with (
            patch.object(self.client, "create_item", side_effect=interrupted),
            patch.object(
                self.client, "_wait_for_operation", side_effect=monitor, create=True
            ),
            self.assertRaises(FabricApiError),
        ):
            self.deploy()

    def test_receipt_is_durable_before_monitor_and_resume_does_not_resubmit(
        self,
    ) -> None:
        self.interrupt_create(receipt=True)
        with patch.object(
            self.client,
            "_wait_for_operation",
            return_value={"status": "Succeeded"},
            create=True,
        ) as wait:
            self.deploy(resume=True)
        wait.assert_called_once()
        self.assertEqual(self.client.creations.count(METADATA_NOTEBOOKS["prepare"]), 1)
        self.assertIsNone(self.client.operation_observer)

    def test_unreceipted_submission_requires_inspection_even_if_item_is_visible(
        self,
    ) -> None:
        self.interrupt_create(receipt=False)
        before = list(self.client.mutations)
        with self.assertRaisesRegex(
            FabricApiError, "uncertain without a receipt.*inspect"
        ):
            self.deploy(resume=True)
        self.assertEqual(self.client.mutations, before)

    def test_pending_dry_run_never_monitors_or_rewrites_state(self) -> None:
        self.interrupt_create(receipt=True)
        before = self.path.read_bytes()
        mutations = list(self.client.mutations)
        with (
            patch.object(self.client, "_wait_for_operation", create=True) as wait,
            self.assertRaisesRegex(FabricApiError, "non-dry --resume"),
        ):
            self.deploy(resume=True, dry_run=True)
        wait.assert_not_called()
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual(mutations, self.client.mutations)

    def test_untrusted_receipt_is_not_polled_on_resume(self) -> None:
        self.interrupt_create(receipt=True)
        state = json.loads(self.path.read_text())
        state["operations"][f"create:{METADATA_NOTEBOOKS['prepare']}"]["receipt"][
            "operation_location"
        ] = "https://api.fabric.microsoft.com/v1/operations/accepted?credential=secret"
        self.path.write_text(json.dumps(state), encoding="utf-8")
        mutations = list(self.client.mutations)
        with (
            patch.object(self.client, "_wait_for_operation", create=True) as wait,
            self.assertRaisesRegex(FabricApiError, "untrusted"),
        ):
            self.deploy(resume=True)
        wait.assert_not_called()
        self.assertEqual(mutations, self.client.mutations)

    def test_completed_asset_identity_cannot_be_replaced_on_resume(self) -> None:
        self.deploy()
        name = METADATA_NOTEBOOKS["worker"]
        self.client.items = [
            FabricItem(validation_id("intruder"), i.display_name, i.type, i.folder_id)
            if i.display_name == name
            else i
            for i in self.client.items
        ]
        before = list(self.client.mutations)
        with self.assertRaisesRegex(FabricApiError, "identity differs"):
            self.deploy(resume=True)
        self.assertEqual(self.client.mutations, before)

    def test_completed_dry_run_resume_does_not_change_journal_or_items(self) -> None:
        self.deploy()
        before, mutations = self.path.read_bytes(), list(self.client.mutations)
        with patch(
            "builtins.input", side_effect=AssertionError("dry-run must not prompt")
        ):
            self.assertEqual(self.deploy(resume=True, dry_run=True), {})
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual(mutations, self.client.mutations)

    def test_existing_lakehouse_move_requires_overwrite(self) -> None:
        self.client.items.append(
            FabricItem(validation_id("lh_gld_treasury"), "lh_gld_treasury", "Lakehouse")
        )
        with self.assertRaisesRegex(FabricApiError, "overwrite"):
            self.deploy()
        self.assertEqual(self.client.mutations, [])

    def test_control_update_receipt_retains_target_id_and_resumes_without_reupdate(
        self,
    ) -> None:
        update = self.client.update_definition

        def interrupted(workspace: str, item: str, definition: dict) -> None:
            update(workspace, item, definition)
            self.client.operation_observer(
                {
                    "method": "POST",
                    "request_path": f"/workspaces/{workspace}/items/{item}/updateDefinition",
                    "operation_location": "https://wabi-test.analysis.windows.net/v1/operations/control-update",
                }
            )
            state = json.loads(self.path.read_text())
            self.assertEqual(state["resolved_items"]["db_orchestrator"], item)
            raise FabricApiError("update monitor timeout")

        with (
            patch.object(self.client, "update_definition", side_effect=interrupted),
            self.assertRaisesRegex(FabricApiError, "timeout"),
        ):
            self.deploy()
        self.assertEqual(self.client.mutations.count("update"), 1)
        with patch.object(
            self.client,
            "_wait_for_operation",
            return_value={"status": "Succeeded"},
            create=True,
        ) as wait:
            self.deploy(resume=True)
        wait.assert_called_once()
        self.assertEqual(self.client.mutations.count("update"), 1)

    def test_missing_source_database_blocks_before_mutation(self) -> None:
        self.client.items.pop()
        for dry_run in (False, True):
            with (
                self.subTest(dry_run=dry_run),
                self.assertRaisesRegex(FabricApiError, "existing.*SQLDatabase"),
            ):
                self.deploy(dry_run=dry_run)
        self.assertEqual(self.client.mutations, [])
        self.assertFalse(self.path.exists())

    def test_duplicate_connections_block_before_mutation(self) -> None:
        with (
            patch.object(
                self.client,
                "resolve_oauth2_connection",
                return_value={"id": validation_id("duplicate")},
            ),
            self.assertRaisesRegex(FabricApiError, "distinct"),
        ):
            self.deploy()
        self.assertEqual(len(self.client.checked), 4)
        self.assertEqual(self.client.mutations, [])

    def test_unavailable_connection_blocks_before_mutation(self) -> None:
        with (
            patch.object(
                self.client,
                "validate_connection_online",
                side_effect=FabricApiError("expired"),
            ),
            self.assertRaisesRegex(FabricApiError, "expired"),
        ):
            self.deploy()
        self.assertEqual(self.client.mutations, [])
        self.assertFalse(self.path.exists())

    def test_each_source_and_control_connection_is_a_required_prerequisite(
        self,
    ) -> None:
        for role in ("bas_finance", "csd_finance", "persal_finance", "orchestrator"):
            selected = f"conn_{role}_sql"
            identity = validation_id(selected)
            for dry_run in (False, True):

                def check(
                    connection: dict, identity: str = identity, selected: str = selected
                ) -> None:
                    if connection["id"] == identity:
                        raise FabricApiError(f"{selected} expired")

                with (
                    self.subTest(role=role, dry_run=dry_run),
                    patch.object(
                        self.client, "validate_connection_online", side_effect=check
                    ) as online,
                    self.assertRaisesRegex(FabricApiError, selected),
                ):
                    self.deploy(dry_run=dry_run)
                self.assertEqual(online.call_count, 4)
                self.assertEqual(self.client.mutations, [])
                self.assertFalse(self.path.exists())


if __name__ == "__main__":
    unittest.main()
