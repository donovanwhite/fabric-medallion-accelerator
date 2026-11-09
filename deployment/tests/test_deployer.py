"""Isolated deployment and no-mutation preflight behavior."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from fabric_medallion_deployer.contracts import load_sources
from fabric_medallion_deployer.deployer import (
    AcceleratorDeployer,
    DeploymentOptions,
    resolve_connection_prerequisites,
    validate_local,
)
from fabric_medallion_deployer.fabric import (
    ApiResponse,
    FabricApiError,
    FabricClient,
    FabricItem,
)

ROOT = Path(__file__).resolve().parents[2]


class InventoryClient:
    def __init__(self, items: list[FabricItem] | None = None) -> None:
        self.items = items or []
        self.mutations: list[str] = []
        self.folders: list[dict] = []
        self.routes = {
            "lakehouses": "Lakehouse",
            "sqlDatabases": "SQLDatabase",
            "notebooks": "Notebook",
            "dataPipelines": "DataPipeline",
        }
        self.operation_observer = None

    def list_items(self, workspace_id: str) -> list[FabricItem]:
        return self.items

    def resolve_workspace(self, workspace: str) -> dict:
        return {"id": "workspace", "displayName": workspace, "capacityId": "capacity"}

    def paged(self, path: str) -> list[dict]:
        if path != "/capacities":
            raise AssertionError(path)
        return [{"id": "capacity", "state": "Active"}]

    def resolve_oauth2_connection(self, name: str, kind: str) -> dict:
        return {"id": name, "displayName": name}

    def validate_connection_online(self, connection: dict) -> None:
        pass

    def list_folders(self, workspace: str) -> list[dict]:
        return self.folders

    def create_folder(self, workspace: str, name: str) -> str:
        self.mutations.append("folder")
        self.folders.append({"id": name, "displayName": name})
        return name

    def find_item(self, workspace: str, name: str, kind: str) -> FabricItem | None:
        return next(
            (i for i in self.items if i.display_name == name and i.type == kind), None
        )

    def create_item(
        self, workspace: str, route: str, name: str, **kwargs
    ) -> FabricItem:
        self.mutations.append(route)
        item = FabricItem(name, name, self.routes[route], kwargs["folder_id"])
        self.items.append(item)
        return item

    def update_definition(self, workspace: str, item: str, definition: dict) -> None:
        self.mutations.append("update")

    def move_items(self, workspace: str, ids: list[str], folder: str) -> None:
        self.mutations.append("move")
        self.items = [
            FabricItem(i.id, i.display_name, i.type, folder) if i.id in ids else i
            for i in self.items
        ]

    def request(self, method: str, path: str, **kwargs) -> ApiResponse:
        return ApiResponse(200, {}, b'{"properties":{"defaultSchema":"dbo"}}')


class DeploymentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sources = load_sources(ROOT, use_case="treasury")

    def deployer(
        self, client: InventoryClient, *, overwrite: bool = True, dry_run: bool = False
    ) -> AcceleratorDeployer:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        deployer = AcceleratorDeployer(
            ROOT,
            DeploymentOptions(
                "ws_kzn_treasury",
                overwrite=overwrite,
                dry_run=dry_run,
                state_file=Path(directory.name) / "publication.json",
                use_case="treasury",
            ),
            client=cast(FabricClient, client),
        )
        deployer.sources = self.sources
        return deployer

    def publication_client(self):
        from test_metadata_deployment import PublicationClient

        return PublicationClient()

    def test_conflicts_ignore_system_endpoints_but_reject_wrong_types(self) -> None:
        client = InventoryClient(
            [
                FabricItem("endpoint", "lh_brz_slv_treasury", "SQLEndpoint"),
                FabricItem("lakehouse", "lh_brz_slv_treasury", "Lakehouse"),
            ]
        )
        self.deployer(client)._check_conflicts("workspace")
        client.items.append(FabricItem("wrong", "db_orchestrator", "Report"))
        with self.assertRaisesRegex(FabricApiError, "expected SQLDatabase"):
            self.deployer(client)._check_conflicts("workspace")

    def test_duplicate_items_and_unapproved_overwrite_fail(self) -> None:
        item = FabricItem("one", "db_orchestrator", "SQLDatabase")
        with self.assertRaises(FabricApiError):
            self.deployer(InventoryClient([item, item]))._check_conflicts("workspace")
        with self.assertRaisesRegex(FabricApiError, "overwrite"):
            self.deployer(InventoryClient([item]), overwrite=False)._check_conflicts(
                "workspace"
            )

    def test_cloud_dry_run_never_mutates(self) -> None:
        client = self.publication_client()
        with patch(
            "fabric_medallion_deployer.deployer.validate_local",
            return_value=self.sources,
        ):
            self.assertEqual(self.deployer(client, dry_run=True).deploy(), {})
        self.assertEqual(client.mutations, [])

    def test_fresh_deployment_is_reusable_inventory_without_pipeline_execution(
        self,
    ) -> None:
        client = self.publication_client()
        with patch(
            "fabric_medallion_deployer.deployer.validate_local",
            return_value=self.sources,
        ):
            items = self.deployer(client).deploy()
        self.assertEqual(len(items), 11)
        self.assertEqual(sum(i.type == "Notebook" for i in items.values()), 6)
        self.assertEqual(sum(i.type == "DataPipeline" for i in items.values()), 2)
        self.assertEqual(sum(i.type == "SQLDatabase" for i in items.values()), 1)
        self.assertTrue(all(i.folder_id is not None for i in items.values()))
        self.assertNotIn("run", client.mutations)
        with patch(
            "fabric_medallion_deployer.deployer.validate_local",
            return_value=self.sources,
        ):
            self.deployer(client).deploy()
        self.assertEqual(
            len(client.items), 14
        )  # Includes three existing source prerequisites.

    def test_offline_preflight_compiles_every_asset(self) -> None:
        with patch("fabric_medallion_deployer.deployer.validate_seed_data") as seed:
            sources = validate_local(ROOT, use_case="treasury")
        self.assertEqual(len(sources), 3)
        from fabric_medallion_deployer.use_cases import load_use_case

        seed.assert_called_once_with(
            ROOT, use_case=load_use_case(ROOT, use_case="treasury").case_id
        )

    def test_expired_connection_blocks_all_mutation(self) -> None:
        client = self.publication_client()
        with (
            patch(
                "fabric_medallion_deployer.deployer.validate_local",
                return_value=self.sources,
            ),
            patch.object(
                client,
                "validate_connection_online",
                side_effect=FabricApiError("expired"),
            ),
            self.assertRaisesRegex(FabricApiError, "expired"),
        ):
            self.deployer(client).deploy()
        self.assertEqual(client.mutations, [])

    def test_bad_existing_lakehouse_fails_before_folder_creation(self) -> None:
        client = self.publication_client()
        client.items.append(FabricItem("old", "lh_brz_slv_treasury", "Lakehouse"))
        with (
            patch(
                "fabric_medallion_deployer.deployer.validate_local",
                return_value=self.sources,
            ),
            patch.object(
                client,
                "request",
                return_value=ApiResponse(200, {}, b'{"properties":{}}'),
            ),
            self.assertRaisesRegex(FabricApiError, "schema-enabled"),
        ):
            self.deployer(client).deploy()
        self.assertEqual(client.mutations, [])

    def test_duplicate_folders_fail_before_any_mutation(self) -> None:
        client = self.publication_client()
        client.folders = [
            {"displayName": "pipelines", "id": "one"},
            {"displayName": "pipelines", "id": "two"},
        ]
        with (
            patch(
                "fabric_medallion_deployer.deployer.validate_local",
                return_value=self.sources,
            ),
            self.assertRaisesRegex(FabricApiError, "root folders"),
        ):
            self.deployer(client).deploy()
        self.assertEqual(client.mutations, [])

    def test_guided_recheck_aggregates_missing_connections_without_consent_automation(
        self,
    ):
        client = InventoryClient()
        unavailable = {"bas", "control"}
        prompts = []

        def resolve(name, kind):
            if name in unavailable:
                raise FabricApiError("not authorized")
            return {"id": name}

        def prompt(message):
            prompts.append(message)
            unavailable.clear()
            return "recheck"

        with patch.object(client, "resolve_oauth2_connection", side_effect=resolve):
            result = resolve_connection_prerequisites(
                cast(FabricClient, client),
                {"bas": "bas", "orchestrator": "control"},
                interactive=True,
                prompt=prompt,
            )
        self.assertEqual(result, {"bas": "bas", "orchestrator": "control"})
        self.assertEqual(len(prompts), 1)
        self.assertIn("bas, control", prompts[0])
        self.assertEqual(client.mutations, [])

    def test_control_only_bootstrap_never_reseeds_sources_and_resumes_schema_ready(
        self,
    ):
        client = InventoryClient()
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "control.json"
            options = DeploymentOptions(
                "ws_kzn_treasury",
                bootstrap_orchestrator=True,
                state_file=state,
                use_case="treasury",
            )
            with (
                patch.object(
                    client,
                    "resolve_oauth2_connection",
                    side_effect=FabricApiError("missing"),
                ),
                self.assertRaisesRegex(FabricApiError, "conn_orchestrator_sql"),
            ):
                AcceleratorDeployer(
                    ROOT, options, client=cast(FabricClient, client)
                ).deploy()
            self.assertEqual(
                [item.display_name for item in client.items], ["db_orchestrator"]
            )
            self.assertEqual(client.mutations, ["sqlDatabases", "update"])
            self.assertTrue(state.is_file())
            before = list(client.mutations)
            result = AcceleratorDeployer(
                ROOT,
                DeploymentOptions(
                    "ws_kzn_treasury",
                    bootstrap_orchestrator=True,
                    state_file=state,
                    resume=True,
                    use_case="treasury",
                ),
                client=cast(FabricClient, client),
            ).deploy()
            self.assertEqual(set(result), {"db_orchestrator"})
            self.assertEqual(client.mutations, before)

    def test_control_only_dry_run_has_no_writes_or_state_file(self):
        client = InventoryClient()
        result = AcceleratorDeployer(
            ROOT,
            DeploymentOptions(
                "ws_kzn_treasury",
                bootstrap_orchestrator=True,
                dry_run=True,
                use_case="treasury",
            ),
            client=cast(FabricClient, client),
        ).deploy()
        self.assertEqual(result, {})
        self.assertEqual(client.items, [])
        self.assertEqual(client.mutations, [])


if __name__ == "__main__":
    unittest.main()
