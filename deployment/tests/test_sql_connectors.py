"""Distinct metadata-selected SQL adapters preserve one downstream contract."""

import base64
import json
import unittest
import uuid
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from fabric_medallion_deployer.configuration import bind_configuration
from fabric_medallion_deployer.definitions import (
    metadata_copy_pipeline_definition,
    source_copy_adapter,
)
from fabric_medallion_deployer.deployer import resolve_connection_prerequisites
from fabric_medallion_deployer.fabric import FabricApiError
from fabric_medallion_deployer.inventory import runtime_source_bindings
from fabric_medallion_deployer.metadata import (
    MetadataError,
    compile_run_plan,
    copy_task_descriptors,
    load_configuration,
)
from metadata_fixtures import metadata_document
from test_fabric import CONNECTION_ID, FakeFabricClient

ROOT = Path(__file__).resolve().parents[2]


def sql_server_document():
    document = metadata_document()
    document["connections"][0].update(
        connector_type="SqlServer", authentication_type="Basic"
    )
    document["feeds"][0]["geometry"] = None
    document["objects"][0]["required_capabilities"] = ["sql-server-copy-basic"]
    document["capabilities"].append(
        {"id": "sql-server-copy-basic", "verified": True, "evidence_sha256": "e" * 64}
    )
    return document


def ready_plan(document):
    config = load_configuration(document)
    return compile_run_plan(
        config,
        "sql-server-run",
        "2026-11-09T08:30:00Z",
        lifecycle_state="APPROVED",
        approved_definition_sha256=config.definition_sha256,
        require_ready=True,
    )


class SqlConnectorTests(unittest.TestCase):
    def test_both_sql_templates_are_reused_without_case_values_or_credentials(self):
        fabric = source_copy_adapter(ROOT, "FabricSql")
        server = source_copy_adapter(ROOT, "SqlServer")
        self.assertEqual(
            fabric["typeProperties"]["source"]["type"], "FabricSqlDatabaseSource"
        )
        self.assertEqual(server["typeProperties"]["source"]["type"], "SqlServerSource")
        self.assertEqual(
            server["typeProperties"]["source"]["datasetSettings"]["type"],
            "SqlServerTable",
        )
        self.assertEqual(
            server["typeProperties"]["sink"], fabric["typeProperties"]["sink"]
        )
        self.assertEqual(
            server["typeProperties"]["translator"],
            fabric["typeProperties"]["translator"],
        )
        for adapter in (fabric, server):
            self.assertEqual(adapter["policy"]["retry"], 0)
            self.assertTrue(adapter["policy"]["secureInput"])
            self.assertTrue(adapter["policy"]["secureOutput"])
            source = adapter["typeProperties"]["source"]
            self.assertEqual(
                source["sqlReaderQuery"],
                {"value": "@item().sql_query", "type": "Expression"},
            )
            self.assertEqual(
                source["datasetSettings"]["connectionSettings"]["properties"][
                    "externalReferences"
                ],
                {"connection": "@item().connection_id"},
            )
            self.assertNotIn("password", json.dumps(adapter).lower())
        self.assertNotIn("source_workspace_id", json.dumps(server))
        with self.assertRaises(ValueError):
            source_copy_adapter(ROOT, "unknown")

    def test_pipeline_switch_selects_explicit_metadata_connector_not_sql_family(self):
        definition = metadata_copy_pipeline_definition(
            ROOT,
            **{
                key: str(uuid.UUID(int=index))
                for index, key in enumerate(
                    (
                        "workspace_id",
                        "control_database_id",
                        "control_connection_id",
                        "bronze_lakehouse_id",
                        "prepare_notebook_id",
                    ),
                    1,
                )
            },
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        loop = document["properties"]["activities"][1]
        switch = next(
            activity
            for activity in loop["typeProperties"]["activities"]
            if activity["type"] == "Switch"
        )
        self.assertEqual(
            switch["typeProperties"]["on"]["value"], "@item().connector_type"
        )
        self.assertEqual(
            {case["value"] for case in switch["typeProperties"]["cases"]},
            {"FabricSql", "SqlServer", "Ftp", "Sftp"},
        )

    def test_sql_server_descriptor_uses_managed_reference_without_fabric_geometry(self):
        row = copy_task_descriptors(ready_plan(sql_server_document()))[0]
        self.assertEqual(row["source_type"], "sql")
        self.assertEqual(row["connector_type"], "SqlServer")
        self.assertEqual(row["authentication_type"], "Basic")
        self.assertIsNone(row["source_workspace_id"])
        self.assertIsNone(row["source_artifact_id"])
        self.assertIn("CONVERT(varchar(50)", row["sql_query"])
        self.assertEqual(row["file_name"], "postings.csv")

    def test_sql_server_copy_evidence_cannot_be_bypassed_by_empty_object_capabilities(
        self,
    ):
        document = sql_server_document()
        document["objects"][0]["required_capabilities"] = []
        document["capabilities"][-1].update(verified=False, evidence_sha256=None)
        with self.assertRaisesRegex(MetadataError, "sql-server-copy-basic"):
            ready_plan(document)

    def test_connector_authentication_geometry_and_transport_mismatches_fail_closed(
        self,
    ):
        for mutation in (
            lambda doc: doc["connections"][0].update(authentication_type="Anonymous"),
            lambda doc: doc["feeds"][0].update(
                geometry={
                    "workspace_id": str(uuid.UUID(int=1)),
                    "artifact_id": str(uuid.UUID(int=2)),
                }
            ),
            lambda doc: doc["feeds"][0].update(source_type="sftp"),
        ):
            document = sql_server_document()
            mutation(document)
            with self.assertRaises(MetadataError):
                load_configuration(document)

    def test_binding_sql_server_needs_connection_but_no_fabric_database_item(self):
        config = load_configuration(sql_server_document())
        result = bind_configuration(
            config,
            workspace_id=str(uuid.UUID(int=1)),
            source_database_ids={},
            connection_ids={"bas": str(uuid.UUID(int=2))},
        )
        self.assertIsNone(result.feeds[0].geometry)
        self.assertEqual(
            result.connections[0].fabric_connection_id, str(uuid.UUID(int=2))
        )
        with self.assertRaises(MetadataError):
            bind_configuration(
                config,
                workspace_id=str(uuid.UUID(int=1)),
                source_database_ids={"bas": str(uuid.UUID(int=3))},
                connection_ids={"bas": str(uuid.UUID(int=2))},
            )

    def test_sql_server_publication_has_no_fabric_sql_database_prerequisite(self):
        config = load_configuration(sql_server_document())
        with patch(
            "fabric_medallion_deployer.orchestrator.bootstrap_configuration",
            return_value=config,
        ):
            sources = runtime_source_bindings(ROOT, "inventory_example")
        self.assertEqual(sources[0].database, "")
        self.assertEqual(sources[0].connection, config.connections[0].name)

    def test_mixed_sql_binding_resolves_database_items_only_for_fabric_sources(self):
        document = metadata_document()
        connection = deepcopy(document["connections"][0])
        connection.update(
            id="connection.external",
            name="conn_external_sql",
            connector_type="SqlServer",
            authentication_type="Basic",
        )
        feed = deepcopy(document["feeds"][0])
        feed.update(
            id="external",
            source_code="external",
            source_instance="external_database",
            connection_id=connection["id"],
            geometry=None,
            legacy_bronze_root=None,
            legacy_contract_hash=None,
        )
        obj = deepcopy(document["objects"][0])
        obj.update(id="external.bronze.postings", feed_id=feed["id"])
        document["connections"].append(connection)
        document["feeds"].append(feed)
        document["objects"].append(obj)
        config = load_configuration(document)
        bound = bind_configuration(
            config,
            workspace_id=str(uuid.UUID(int=1)),
            source_database_ids={"bas": str(uuid.UUID(int=2))},
            connection_ids={
                "bas": str(uuid.UUID(int=3)),
                "external": str(uuid.UUID(int=4)),
            },
        )
        self.assertIsNotNone(bound.feeds[0].geometry)
        self.assertIsNone(bound.feeds[1].geometry)
        self.assertEqual(
            [connection.fabric_connection_id for connection in bound.connections],
            [str(uuid.UUID(int=3)), str(uuid.UUID(int=4))],
        )

    def test_sql_server_managed_connection_checks_auth_type_and_gateway(self):
        client = FakeFabricClient({"status": "Online"})
        gateway = str(uuid.UUID(int=15))
        client.connection.update(
            connectivityType="OnPremisesGateway",
            gatewayId=gateway,
            connectionDetails={"type": "SQL"},
            credentialDetails={"credentialType": "Basic"},
        )
        connection = client.resolve_managed_connection(
            CONNECTION_ID,
            "SQL",
            credential_type="Basic",
            gateway_id=gateway,
            connectivity_types={
                "ShareableCloud",
                "OnPremisesGateway",
                "VirtualNetworkGateway",
            },
        )
        self.assertEqual(connection["gatewayId"], gateway)
        client.validate_connection_online(connection)
        for field, value in (
            ("gatewayId", str(uuid.UUID(int=16))),
            ("credentialDetails", {"credentialType": "Anonymous"}),
            ("connectionDetails", {"type": "FabricSql"}),
        ):
            original = client.connection[field]
            client.connection[field] = value
            with self.assertRaises(FabricApiError):
                client.resolve_managed_connection(
                    CONNECTION_ID,
                    "SQL",
                    credential_type="Basic",
                    gateway_id=gateway,
                    connectivity_types={"OnPremisesGateway"},
                )
            client.connection[field] = original

    def test_publication_preflight_resolves_the_metadata_selected_sql_connector(self):
        client = FakeFabricClient({"status": "Online"})
        client.connection.update(
            connectionDetails={"type": "SQL"},
            credentialDetails={"credentialType": "Basic"},
        )
        config = load_configuration(sql_server_document())
        with patch.object(
            client,
            "resolve_oauth2_connection",
            side_effect=AssertionError("SQL Server must not use FabricSql preflight"),
        ):
            result = resolve_connection_prerequisites(
                client,
                {"source": CONNECTION_ID},
                requirements={"source": config.connections[0]},
            )
        self.assertEqual(result, {"source": CONNECTION_ID})

    def test_sql_control_plane_exports_connector_selection_and_distinct_readiness(self):
        sql = (ROOT / "artefacts" / "orchestrator" / "procedures.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("conn.connector_type AS connector_type", sql)
        self.assertIn("value.connector_type, value.feed_id", sql)
        self.assertEqual(
            sql.count(
                "SQL Server Copy requires verified connector/authentication evidence."
            ),
            2,
        )
        self.assertIn("conn.connector_type = 'FabricSql'", sql)
        self.assertIn("conn.connector_type = 'SqlServer'", sql)


if __name__ == "__main__":
    unittest.main()
