"""Runtime-selected Copy descriptors and generic pipeline definition contracts."""

from __future__ import annotations

import base64
import json
import unittest
import uuid
from pathlib import Path

from fabric_medallion_deployer import definitions, metadata
from metadata_fixtures import metadata_document

ROOT = Path(__file__).resolve().parents[2]


class MetadataPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        document = metadata_document()
        document["capabilities"][0].update(verified=True, evidence_sha256="e" * 64)
        document["feeds"][0]["geometry"] = {
            "workspace_id": "44444444-4444-4444-8444-444444444444",
            "artifact_id": "55555555-5555-4555-8555-555555555555",
        }
        self.config = metadata.load_configuration(document)
        self.plan = metadata.compile_run_plan(
            self.config,
            "run-1",
            "2026-11-06T08:30:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=self.config.definition_sha256,
            require_ready=True,
        )
        self.bindings = {
            "workspace_id": str(uuid.UUID(int=11)),
            "control_database_id": str(uuid.UUID(int=12)),
            "control_connection_id": str(uuid.UUID(int=13)),
            "bronze_lakehouse_id": str(uuid.UUID(int=14)),
        }
        self.copy_bindings = {
            **self.bindings,
            "prepare_notebook_id": str(uuid.UUID(int=15)),
        }

    def test_copy_descriptor_is_derived_from_pinned_geometry_and_typed_columns(self):
        rows = metadata.copy_task_descriptors(self.plan)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(
            row["connection_id"], self.config.connections[0].fabric_connection_id
        )
        self.assertEqual(
            row["source_workspace_id"], self.config.feeds[0].geometry.workspace_id
        )
        self.assertEqual(
            row["source_artifact_id"], self.config.feeds[0].geometry.artifact_id
        )
        self.assertEqual(
            row["sql_query"],
            "SELECT [id],CONVERT(varchar(50),CAST([amount] AS decimal(19,4))) AS [amount] FROM [bas].[postings]",
        )
        self.assertEqual(row["file_name"], "postings.csv")
        self.assertTrue(
            row["folder_path"].startswith("bronze/bas_finance_sql/20261106/")
        )
        row["connection_id"] = "changed"
        self.assertNotEqual(
            metadata.copy_task_descriptors(self.plan)[0]["connection_id"], "changed"
        )

    def test_missing_geometry_blocks_execution_but_preserves_readonly_preview(self):
        document = self.config.to_document()
        document["feeds"][0]["geometry"] = None
        config = metadata.load_configuration(document)
        preview = metadata.compile_run_plan(
            config,
            "run-1",
            "2026-11-06T08:30:00Z",
            lifecycle_state="APPROVED",
            approved_definition_sha256=config.definition_sha256,
        )
        self.assertFalse(preview.can_execute)
        self.assertTrue(any("geometry" in blocker for blocker in preview.blockers))
        with self.assertRaises(metadata.MetadataError):
            metadata.copy_task_descriptors(preview)

    def test_file_copy_descriptors_keep_transport_geometry_and_managed_auth(self):
        for source_type, connector in (("ftp", "Ftp"), ("sftp", "Sftp")):
            document = metadata_document()
            document["connections"][0].update(
                connector_type=connector, authentication_type="Basic"
            )
            document["feeds"][0].update(source_type=source_type, geometry=None)
            document["objects"][0].update(
                required_capabilities=[],
                contract={
                    "source_directory": "/exports/finance",
                    "file_pattern": "postings_*.csv",
                    "recursive": True,
                    "file_format": "csv",
                    "arrival_policy": {"kind": "marker", "marker": "_SUCCESS"},
                },
            )
            document["capabilities"].extend(
                [
                    {
                        "id": f"{source_type}-copy-basic",
                        "verified": True,
                        "evidence_sha256": "e" * 64,
                    },
                    {
                        "id": f"{source_type}-arrival-observer",
                        "verified": True,
                        "evidence_sha256": "f" * 64,
                    },
                ]
            )
            config = metadata.load_configuration(document)
            plan = metadata.compile_run_plan(
                config,
                "run-1",
                "2026-11-06T08:30:00Z",
                lifecycle_state="APPROVED",
                approved_definition_sha256=config.definition_sha256,
                require_ready=True,
            )
            row = metadata.copy_task_descriptors(plan)[0]
            self.assertEqual(row["source_type"], source_type)
            self.assertEqual(row["source_directory"], "/exports/finance")
            self.assertEqual(row["file_pattern"], "postings_*.csv")
            self.assertTrue(row["recursive"])
            self.assertIsNone(row["sql_query"])
            self.assertEqual(row["authentication_type"], "Basic")
            self.assertTrue(row["folder_path"].endswith("/postings"))
            for field, value in (
                ("source_directory", "../exports"),
                ("file_pattern", "../*.csv"),
                ("recursive", "true"),
                ("file_format", "parquet"),
            ):
                changed = config.to_document()
                changed["objects"][0]["contract"][field] = value
                with (
                    self.subTest(source_type=source_type, field=field),
                    self.assertRaises(metadata.MetadataError),
                ):
                    metadata.load_configuration(changed)

    def test_file_transport_requires_protocol_auth_and_observer_evidence(self):
        for source_type, connector, authentication in (
            ("ftp", "Ftp", "Basic"),
            ("ftp", "Ftp", "Anonymous"),
            ("sftp", "Sftp", "Basic"),
        ):
            with self.subTest(source_type=source_type, authentication=authentication):
                document = metadata_document()
                document["connections"][0].update(
                    connector_type=connector, authentication_type=authentication
                )
                document["feeds"][0].update(source_type=source_type, geometry=None)
                document["objects"][0].update(
                    required_capabilities=[],
                    contract={
                        "source_directory": "/exports/finance",
                        "file_pattern": "postings_*.csv",
                        "recursive": True,
                        "file_format": "csv",
                        "arrival_policy": {"kind": "marker", "marker": "_SUCCESS"},
                    },
                )
                required = (
                    f"{source_type}-copy-{authentication.lower()}",
                    f"{source_type}-arrival-observer",
                )
                config = metadata.load_configuration(document)
                preview = metadata.compile_run_plan(
                    config,
                    "run-file",
                    "2026-11-06T08:30:00Z",
                    lifecycle_state="APPROVED",
                    approved_definition_sha256=config.definition_sha256,
                )
                self.assertFalse(preview.can_execute)
                for capability in required:
                    self.assertTrue(
                        any(capability in reason for reason in preview.blockers)
                    )
                    document["capabilities"].append(
                        {
                            "id": capability,
                            "verified": True,
                            "evidence_sha256": "e" * 64,
                        }
                    )
                config = metadata.load_configuration(document)
                plan = metadata.compile_run_plan(
                    config,
                    "run-file",
                    "2026-11-06T08:30:00Z",
                    lifecycle_state="APPROVED",
                    approved_definition_sha256=config.definition_sha256,
                    require_ready=True,
                )
                self.assertTrue(plan.can_execute)
                document["capabilities"][-1].update(
                    verified=False, evidence_sha256=None
                )
                config = metadata.load_configuration(document)
                with self.assertRaisesRegex(metadata.MetadataError, required[-1]):
                    metadata.compile_run_plan(
                        config,
                        "run-file",
                        "2026-11-06T08:30:00Z",
                        lifecycle_state="APPROVED",
                        approved_definition_sha256=config.definition_sha256,
                        require_ready=True,
                    )

    def test_file_source_contract_requires_complete_arrival_metadata(self):
        document = metadata_document()
        document["connections"][0].update(
            connector_type="Ftp", authentication_type="Basic"
        )
        document["feeds"][0].update(source_type="ftp", geometry=None)
        document["objects"][0].update(required_capabilities=[], contract={})
        with self.assertRaisesRegex(metadata.MetadataError, "directory|arrival"):
            metadata.load_configuration(document)

    def test_file_source_contract_rejects_noncanonical_json_types(self):
        contract = {
            "source_directory": "/exports",
            "file_pattern": "postings_*.csv",
            "recursive": False,
            "file_format": "csv",
            "arrival_policy": {"kind": "marker", "marker": "_SUCCESS"},
        }
        cases = (
            ("source_directory", 123),
            ("file_pattern", 123),
            ("recursive", "false"),
            ("arrival_policy", {"kind": "marker", "marker": 123}),
            (
                "arrival_policy",
                {
                    "kind": "stable",
                    "minimum_age_seconds": "60",
                    "stable_for_seconds": 60,
                },
            ),
            (
                "arrival_policy",
                {
                    "kind": "stable",
                    "minimum_age_seconds": 60,
                    "stable_for_seconds": True,
                },
            ),
            (
                "arrival_policy",
                {
                    "kind": "stable",
                    "minimum_age_seconds": 60.0,
                    "stable_for_seconds": 60,
                },
            ),
        )
        for field, value in cases:
            candidate = {**contract, field: value}
            with (
                self.subTest(field=field, value=value),
                self.assertRaises(metadata.MetadataError),
            ):
                metadata.file_source_contract(candidate)

    def test_dispatch_pages_bound_both_rows_and_escaped_lookup_bytes(self):
        pages = metadata.copy_page_descriptors(self.plan, page_size=1, max_bytes=4096)
        self.assertEqual(pages, ({"after_ordinal": 0, "page_size": 1},))
        for size in (0, 5001, True):
            with self.subTest(size=size), self.assertRaises(metadata.MetadataError):
                metadata.copy_page_descriptors(self.plan, page_size=size)
        with self.assertRaisesRegex(metadata.MetadataError, "Lookup|bound"):
            metadata.copy_page_descriptors(self.plan, max_bytes=20)

    def test_child_uses_bound_lookup_and_dynamic_foreach_switch_copy(self):
        definition = definitions.metadata_copy_pipeline_definition(
            ROOT, **self.copy_bindings
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        activities = document["properties"]["activities"]
        lookup, loop = activities[:2]
        self.assertEqual(lookup["type"], "Lookup")
        source = lookup["typeProperties"]["source"]
        self.assertEqual(
            source["sqlReaderStoredProcedureName"], "ctl.usp_copy_task_page"
        )
        self.assertIn("run_id", source["storedProcedureParameters"])
        self.assertFalse(lookup["typeProperties"]["firstRowOnly"])
        self.assertEqual(loop["type"], "ForEach")
        self.assertTrue(loop["typeProperties"]["isSequential"])
        switch = next(
            activity
            for activity in loop["typeProperties"]["activities"]
            if activity["type"] == "Switch"
        )
        self.assertEqual(switch["type"], "Switch")
        sql_copy = next(
            c for c in switch["typeProperties"]["cases"] if c["value"] == "FabricSql"
        )["activities"][0]
        self.assertEqual(sql_copy["policy"]["retry"], 0)
        self.assertTrue(sql_copy["policy"]["secureInput"])
        self.assertTrue(sql_copy["policy"]["secureOutput"])
        self.assertEqual(
            sql_copy["typeProperties"]["source"]["sqlReaderQuery"],
            {"value": "@item().sql_query", "type": "Expression"},
        )
        settings = sql_copy["typeProperties"]["source"]["datasetSettings"][
            "connectionSettings"
        ]["properties"]
        self.assertEqual(
            settings["externalReferences"]["connection"], "@item().connection_id"
        )
        self.assertEqual(
            settings["typeProperties"]["artifactId"], "@item().source_artifact_id"
        )
        self.assertNotIn("p_table_config", json.dumps(document))
        self.assertNotIn(
            "p_tasks", json.dumps(document["properties"].get("parameters", {}))
        )

    def test_master_prepares_exports_and_pages_through_one_generic_child(self):
        notebooks = {
            name: str(uuid.UUID(int=20 + index))
            for index, name in enumerate(("prepare", "silver", "gold"))
        }
        definition = definitions.metadata_master_pipeline_definition(
            ROOT,
            **self.bindings,
            copy_pipeline_id=str(uuid.UUID(int=19)),
            notebook_ids=notebooks,
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        activities = document["properties"]["activities"]
        self.assertEqual(activities[0]["type"], "Script")
        self.assertEqual(activities[1]["type"], "Copy")
        self.assertEqual(
            activities[1]["typeProperties"]["source"]["sqlReaderStoredProcedureName"],
            "ctl.usp_export_run",
        )
        loop = next(a for a in activities if a["type"] == "ForEach")
        child = loop["typeProperties"]["activities"][0]
        self.assertEqual(child["type"], "ExecutePipeline")
        self.assertTrue(child["typeProperties"]["waitOnCompletion"])
        self.assertNotIn("p_table_config", json.dumps(document))
        for activity in activities:
            if activity["type"] in {"Script", "Copy", "TridentNotebook"}:
                self.assertEqual(activity["policy"]["retry"], 0)
                self.assertTrue(activity["policy"]["secureInput"])
                self.assertTrue(activity["policy"]["secureOutput"])

    def test_selected_gold_is_reserved_exported_and_reconciled_without_automatic_refresh(
        self,
    ):
        notebooks = {
            name: str(uuid.UUID(int=20 + index))
            for index, name in enumerate(("prepare", "silver", "gold"))
        }
        definition = definitions.metadata_master_pipeline_definition(
            ROOT,
            **self.bindings,
            copy_pipeline_id=str(uuid.UUID(int=19)),
            notebook_ids=notebooks,
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        stage = next(
            a
            for a in document["properties"]["activities"]
            if a["name"] == "selected_gold"
        )
        self.assertIn(".layers", stage["typeProperties"]["expression"]["value"])
        self.assertEqual(stage["typeProperties"]["ifFalseActivities"], [])
        activities = {a["name"]: a for a in stage["typeProperties"]["ifTrueActivities"]}
        self.assertEqual(
            activities["orchestrate_gold"]["dependsOn"],
            [
                {
                    "activity": "export_gold_dispatch",
                    "dependencyConditions": ["Succeeded"],
                }
            ],
        )
        preparation = activities["prepare_gold_attempts"]["typeProperties"]["scripts"][
            0
        ]
        self.assertIn("ctl.usp_prepare_notebook_layer", preparation["text"])
        self.assertEqual(
            next(p for p in preparation["parameters"] if p["name"] == "layer")["value"],
            "gold",
        )
        export = activities["export_gold_dispatch"]["typeProperties"]["source"]
        self.assertEqual(export["storedProcedureParameters"]["layer"]["value"], "gold")
        reconciliation = activities["reconcile_gold"]
        self.assertEqual(
            reconciliation["dependsOn"],
            [{"activity": "orchestrate_gold", "dependencyConditions": ["Completed"]}],
        )
        self.assertEqual(
            reconciliation["typeProperties"]["notebookId"], notebooks["gold"]
        )
        self.assertEqual(
            reconciliation["typeProperties"]["parameters"]["operation"]["value"],
            "reconcile_gold",
        )
        self.assertEqual(
            activities["require_gold_success"]["dependsOn"],
            [
                {
                    "activity": "reconcile_gold_pages",
                    "dependencyConditions": ["Succeeded"],
                }
            ],
        )
        self.assertEqual(activities["require_gold_success"]["type"], "TridentNotebook")
        self.assertEqual(
            activities["require_gold_success"]["typeProperties"]["parameters"][
                "operation"
            ]["value"],
            "require_gold_success",
        )
        self.assertFalse(
            any(a["type"] in {"IfCondition", "Switch"} for a in activities.values())
        )
        self.assertNotIn("CREATE OR REPLACE", json.dumps(stage))
        for activity in activities.values():
            if activity["type"] in {"Script", "Copy", "TridentNotebook"}:
                self.assertEqual(activity["policy"]["retry"], 0)
                self.assertTrue(activity["policy"]["secureInput"])
                self.assertTrue(activity["policy"]["secureOutput"])

    def test_copy_attempt_is_reserved_before_connector_execution(self):
        definition = definitions.metadata_copy_pipeline_definition(
            ROOT, **self.copy_bindings
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        loop = next(
            activity
            for activity in document["properties"]["activities"]
            if activity["type"] == "ForEach"
        )
        activities = loop["typeProperties"]["activities"]
        prepare = next(
            activity for activity in activities if activity["type"] == "Script"
        )
        self.assertEqual(prepare["name"], "prepare_copy_attempt")
        script = prepare["typeProperties"]["scripts"][0]
        self.assertEqual(
            script["text"],
            "EXEC ctl.usp_prepare_copy_attempt @run_id,@task_id,@execution_run_id,@workspace_id;",
        )
        parameters = {value["name"]: value for value in script["parameters"]}
        self.assertEqual(
            parameters["task_id"]["value"],
            {"value": "@item().task_id", "type": "Expression"},
        )
        self.assertEqual(
            parameters["execution_run_id"]["value"],
            {"value": "@pipeline().RunId", "type": "Expression"},
        )
        switch = next(
            activity for activity in activities if activity["type"] == "Switch"
        )
        self.assertEqual(
            switch["dependsOn"],
            [{"activity": prepare["name"], "dependencyConditions": ["Succeeded"]}],
        )
        for case in switch["typeProperties"]["cases"]:
            copy = case["activities"][0]
            self.assertEqual(copy["type"], "Copy")
            self.assertEqual(
                {value["name"] for value in copy["userProperties"]},
                {"run_id", "task_id", "attempt_id"},
            )
            self.assertEqual(copy["policy"]["retry"], 0)

    def test_failed_bronze_dispatch_reconciles_before_blocking_descendants(self):
        definition = definitions.metadata_master_pipeline_definition(
            ROOT,
            **self.bindings,
            copy_pipeline_id=str(uuid.UUID(int=19)),
            notebook_ids={
                name: str(uuid.UUID(int=20 + index))
                for index, name in enumerate(("prepare", "silver", "gold"))
            },
        )
        activities = {
            activity["name"]: activity
            for activity in json.loads(
                base64.b64decode(definition["parts"][0]["payload"])
            )["properties"]["activities"]
        }
        self.assertEqual(
            activities["bronze_dispatch_info"]["dependsOn"],
            [
                {
                    "activity": "dispatch_bronze_pages",
                    "dependencyConditions": ["Completed"],
                }
            ],
        )
        self.assertEqual(
            activities["finish_bronze_dispatch"]["dependsOn"],
            [
                {
                    "activity": "reconcile_bronze_pages",
                    "dependencyConditions": ["Succeeded"],
                }
            ],
        )
        script = activities["finish_bronze_dispatch"]["typeProperties"]["scripts"][0]
        self.assertIn("ctl.usp_finish_bronze_dispatch", script["text"])
        self.assertEqual(
            activities["require_bronze_success"]["dependsOn"],
            [
                {
                    "activity": "finish_bronze_dispatch",
                    "dependencyConditions": ["Succeeded"],
                }
            ],
        )

    def test_reconciliation_uses_same_child_and_verifies_callbacks_before_sql(self):
        definition = definitions.metadata_copy_pipeline_definition(
            ROOT, **self.copy_bindings
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        branch = next(
            activity
            for activity in document["properties"]["activities"]
            if activity["name"] == "reconcile_metadata_page"
        )
        self.assertEqual(branch["type"], "IfCondition")
        verify, lookup, loop = branch["typeProperties"]["ifTrueActivities"]
        self.assertEqual(verify["type"], "TridentNotebook")
        self.assertEqual(
            verify["typeProperties"]["parameters"]["operation"]["value"],
            "verify_callbacks",
        )
        self.assertEqual(
            lookup["dependsOn"],
            [{"activity": verify["name"], "dependencyConditions": ["Succeeded"]}],
        )
        self.assertEqual(loop["type"], "ForEach")
        script = loop["typeProperties"]["activities"][0]["typeProperties"]["scripts"][0]
        self.assertIn("ctl.usp_reconcile_pipeline_result", script["text"])
        self.assertEqual(len(script["parameters"]), 5)
        self.assertTrue(
            all(
                value["value"]["value"] == "@item()." + value["name"]
                for value in script["parameters"]
            )
        )

    def test_master_reconciles_copy_results_before_silver_with_no_extra_child(self):
        child_id = str(uuid.UUID(int=19))
        definition = definitions.metadata_master_pipeline_definition(
            ROOT,
            **self.bindings,
            copy_pipeline_id=child_id,
            notebook_ids={
                name: str(uuid.UUID(int=20 + index))
                for index, name in enumerate(("prepare", "silver", "gold"))
            },
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        activities = {
            value["name"]: value for value in document["properties"]["activities"]
        }
        record = activities["reconcile_bronze"]
        self.assertEqual(
            record["typeProperties"]["parameters"]["operation"]["value"],
            "reconcile_bronze",
        )
        callback = activities["reconcile_bronze_pages"]["typeProperties"]["activities"][
            0
        ]
        self.assertEqual(
            callback["typeProperties"]["pipeline"]["referenceName"], child_id
        )
        self.assertEqual(
            callback["typeProperties"]["parameters"]["p_operation"], "reconcile"
        )
        self.assertEqual(
            activities["orchestrate_silver"]["dependsOn"],
            [
                {
                    "activity": "export_silver_dispatch",
                    "dependencyConditions": ["Succeeded"],
                }
            ],
        )
        self.assertEqual(
            activities["prepare_silver_attempts"]["dependsOn"],
            [
                {
                    "activity": "require_bronze_success",
                    "dependencyConditions": ["Succeeded"],
                }
            ],
        )
        self.assertIn(
            "dispatch_sha256",
            activities["orchestrate_silver"]["typeProperties"]["parameters"],
        )


if __name__ == "__main__":
    unittest.main()
