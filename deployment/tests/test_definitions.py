"""Portable definition and lossless feed-projection contracts."""

from __future__ import annotations

import base64
import json
import unittest
from pathlib import Path

from fabric_medallion_deployer.contracts import load_sources
from fabric_medallion_deployer.definitions import (
    copy_table_config,
    notebook_definition,
    part,
    pipeline_definition,
    sql_database_definition,
    sql_literal,
    sql_type,
    validate_deployable_definition,
)

ROOT = Path(__file__).resolve().parents[2]


def decode_part(definition: dict, path: str) -> str:
    return base64.b64decode(
        next(p for p in definition["parts"] if p["path"] == path)["payload"]
    ).decode("utf-8")


class DefinitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sources = load_sources(ROOT)

    def test_three_sql_projects_cover_all_tables(self) -> None:
        for source in self.sources:
            definition = sql_database_definition(source)
            validate_deployable_definition(definition)
            ddl = decode_part(definition, "Deploy.sql")
            self.assertEqual(ddl.count("CREATE TABLE"), len(source.tables))
            self.assertEqual(len(definition["parts"]), len(source.tables) + 2)
            self.assertIn(source.database, decode_part(definition, "sqldb.sqlproj"))
            for table in source.tables:
                self.assertIn(f":r .\\Seeds\\{table['name']}.sql", ddl)
                self.assertIn(
                    "MERGE", decode_part(definition, f"Seeds/{table['name']}.sql")
                )

    def test_source_serialization_is_explicit(self) -> None:
        csd = copy_table_config(self.sources[1])
        self.assertIn("THEN N'true' ELSE N'false'", csd[0]["columnList"])
        self.assertIn("[SupplierNumber]", csd[0]["columnList"])
        self.assertEqual(csd[0]["sinkFolder"], "bronze/csd")
        persal = copy_table_config(self.sources[2])
        employee = next(t for t in persal if t["name"] == "t_employee")
        self.assertIn("decimal(19,4)", employee["columnList"])
        self.assertIn("DATEPART(millisecond,[extract_date])", employee["columnList"])
        self.assertIn(
            "CONVERT(char(1),CONVERT(int,[unique_identifier_ind])) AS [unique_identifier_ind]",
            employee["columnList"],
        )
        qualification = next(t for t in persal if t["name"] == "t_quali")
        self.assertIn(
            "CONVERT(char(1),CONVERT(int,[latest])) AS [latest]",
            qualification["columnList"],
        )
        self.assertNotIn("SAP", json.dumps(persal))

    def test_literals_preserve_text_and_escape_apostrophes(self) -> None:
        column = {"data_type": "varchar"}
        self.assertEqual(sql_literal("  O'Brien\n", column), "N'  O''Brien\n'")
        self.assertEqual(sql_literal("", column), "NULL")
        self.assertEqual(sql_literal("false", {"data_type": "boolean"}), "0")

    def test_simulator_type_mappings(self) -> None:
        self.assertEqual(sql_type({"data_type": "money"}), "money")
        self.assertEqual(sql_type({"data_type": "bigint"}), "bigint")
        self.assertEqual(
            sql_type({"data_type": "datetime", "format": "CSD_TIMESTAMP"}),
            "nvarchar(23)",
        )

    def test_pipeline_bindings_parameters_and_source_isolation(self) -> None:
        for source in self.sources:
            ids = {name: f"test-{step}" for step, name in source.notebooks.items()}
            definition = pipeline_definition(
                ROOT,
                source,
                workspace_id="workspace-test",
                sql_database_id="source-database",
                bronze_lakehouse_id="treasury-lakehouse",
                sql_connection_id="source-connection",
                notebook_ids=ids,
            )
            pipeline = json.loads(decode_part(definition, "pipeline-content.json"))[
                "properties"
            ]
            activities = pipeline["activities"]
            loop = activities[1]
            self.assertEqual(loop["name"], f"fe_copy_{source.code}_extract_batch")
            self.assertTrue(loop["typeProperties"]["isSequential"])
            copy = loop["typeProperties"]["activities"][0]
            self.assertEqual(copy["name"], f"cp_{source.code}_extract_table")
            self.assertEqual(copy["policy"]["retry"], 0)
            location = copy["typeProperties"]["sink"]["datasetSettings"][
                "typeProperties"
            ]["location"]
            self.assertIn("pipeline().RunId", location["folderPath"]["value"])
            self.assertEqual(
                location["fileName"]["value"], "@concat(item().filePrefix,'.csv')"
            )
            connection = copy["typeProperties"]["source"]["datasetSettings"][
                "connectionSettings"
            ]
            self.assertEqual(
                connection["properties"]["externalReferences"]["connection"],
                "source-connection",
            )
            self.assertEqual(
                len(pipeline["parameters"]["p_table_config"]["defaultValue"]),
                len(source.tables),
            )
            for activity in activities[2:]:
                self.assertIn(activity["name"], source.notebooks.values())
                self.assertEqual(
                    activity["typeProperties"]["parameters"]["pipeline_run_id"][
                        "value"
                    ]["value"],
                    "@pipeline().RunId",
                )
                self.assertTrue(activity["policy"]["secureInput"])
                self.assertEqual(
                    activity["typeProperties"]["parameters"]["batch_started_utc"][
                        "value"
                    ]["value"],
                    "@variables('var_run_upper_bound_utc')",
                )
            self.assertEqual(
                activities[-1]["dependsOn"][0]["activity"],
                source.notebooks["validation"],
            )
            self.assertNotIn("Gold", json.dumps(pipeline))

    def test_published_notebooks_are_self_contained_and_bronze_bound(self) -> None:
        for source in self.sources:
            for step in source.notebooks:
                definition = notebook_definition(
                    ROOT,
                    source,
                    step,
                    workspace_id="workspace-test",
                    lakehouse_id="lakehouse-test",
                )
                validate_deployable_definition(definition)
                notebook = json.loads(decode_part(definition, "artifact.content.ipynb"))
                self.assertEqual(
                    notebook["metadata"]["dependencies"]["lakehouse"][
                        "default_lakehouse_name"
                    ],
                    "lh_brz_slv_treasury",
                )
                source_code = "".join(
                    "".join(cell["source"])
                    for cell in notebook["cells"]
                    if cell["cell_type"] == "code"
                )
                self.assertIn("def prepare_rows(", source_code)
                self.assertNotIn("sys.path", source_code)
                self.assertTrue(
                    all(not cell.get("outputs") for cell in notebook["cells"])
                )

    def test_rejects_stale_or_unresolved_definitions(self) -> None:
        for content in (
            "__WORKSPACE_ID__",
            "0448ed4f-990e-4171-83f1-ea2a293e12d9",
        ):
            with self.assertRaises(ValueError):
                validate_deployable_definition({"parts": [part("x.json", content)]})
        with self.assertRaises(ValueError):
            validate_deployable_definition({"parts": []})
        with self.assertRaises(ValueError):
            validate_deployable_definition({"parts": [part("x", ""), part("x", "")]})


if __name__ == "__main__":
    unittest.main()
