"""Reproducible A1 probe definitions; unit discovery never submits cloud jobs.

The probe copies constant synthetic metadata, not business-source records.
Live execution requires explicit workspace/connection approval and a journal.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import unittest
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fabric_medallion_deployer.auth import azure_cli_token
from fabric_medallion_deployer.definitions import _code_cell, part
from fabric_medallion_deployer.fabric import FabricClient


def copy_probe_definition(
    workspace_id: str,
    database_id: str,
    lakehouse_id: str,
    connection_ids: tuple[str, str],
    probe_id: str,
) -> dict:
    for value in (workspace_id, database_id, lakehouse_id, probe_id, *connection_ids):
        if str(uuid.UUID(value)) != value:
            raise ValueError("Probe bindings must be canonical UUIDs")
    if connection_ids[0] == connection_ids[1]:
        raise ValueError("Dynamic connection proof requires two distinct connections")
    source = {
        "type": "FabricSqlDatabaseSource",
        "sqlReaderQuery": (
            f"SELECT CAST('{probe_id}' AS varchar(36)) AS probe_id,"
            'CAST(\'{"schema_version":1,"message":"synthetic_metadata_probe"}\''
            " AS varchar(200)) AS definition_json"
        ),
        "queryTimeout": "00:02:00",
        "partitionOption": "None",
        "datasetSettings": {
            "annotations": [],
            "type": "FabricSqlDatabaseTable",
            "schema": [],
            "typeProperties": {},
            "connectionSettings": {
                "name": "probe_dynamic_connection",
                "properties": {
                    "annotations": [],
                    "type": "FabricSqlDatabase",
                    "typeProperties": {
                        "workspaceId": workspace_id,
                        "artifactId": database_id,
                    },
                    "externalReferences": {"connection": "@item().connection_id"},
                },
            },
        },
    }
    sink = {
        "type": "DelimitedTextSink",
        "storeSettings": {"type": "LakehouseWriteSettings"},
        "formatSettings": {
            "type": "DelimitedTextWriteSettings",
            "quoteAllText": True,
            "fileExtension": ".csv",
        },
        "datasetSettings": {
            "annotations": [],
            "type": "DelimitedText",
            "schema": [],
            "typeProperties": {
                "location": {
                    "type": "LakehouseLocation",
                    "folderPath": f"orchestration/capability/{probe_id}",
                    "fileName": {
                        "value": "@concat(item().tag,'.csv')",
                        "type": "Expression",
                    },
                },
                "columnDelimiter": ",",
                "escapeChar": "\\",
                "firstRowAsHeader": True,
                "quoteChar": '"',
            },
            "linkedService": {
                "name": "probe_lakehouse",
                "properties": {
                    "annotations": [],
                    "type": "Lakehouse",
                    "typeProperties": {
                        "workspaceId": workspace_id,
                        "artifactId": lakehouse_id,
                        "rootFolder": "Files",
                    },
                },
            },
        },
    }
    payload = {
        "properties": {
            "parameters": {
                "p_connections": {
                    "type": "Array",
                    "defaultValue": [
                        {"connection_id": connection_id, "tag": f"connection_{i}"}
                        for i, connection_id in enumerate(connection_ids)
                    ],
                }
            },
            "activities": [
                {
                    "name": "probe_dynamic_connections",
                    "type": "ForEach",
                    "dependsOn": [],
                    "typeProperties": {
                        "items": {
                            "value": "@pipeline().parameters.p_connections",
                            "type": "Expression",
                        },
                        "isSequential": True,
                        "activities": [
                            {
                                "name": "probe_copy_metadata",
                                "type": "Copy",
                                "dependsOn": [],
                                "policy": {
                                    "timeout": "0.00:05:00",
                                    "retry": 0,
                                    "secureInput": True,
                                    "secureOutput": True,
                                },
                                "typeProperties": {
                                    "source": source,
                                    "sink": sink,
                                    "enableStaging": False,
                                    "validateDataConsistency": True,
                                },
                            }
                        ],
                    },
                }
            ],
        }
    }
    return {"parts": [part("pipeline-content.json", json.dumps(payload))]}


def notebook_probe_definition(
    workspace_id: str,
    lakehouse_id: str,
    lakehouse_name: str,
    probe_id: str,
    *,
    timed: bool = False,
) -> dict:
    for value in (workspace_id, lakehouse_id, probe_id):
        if str(uuid.UUID(value)) != value:
            raise ValueError("Probe bindings must be canonical UUIDs")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,127}", lakehouse_name):
        raise ValueError("Invalid probe Lakehouse name")
    timing = "import time\nstarted = time.time()\ntime.sleep(5)\n" if timed else ""
    timed_result = (
        "result.update(started=started, ended=time.time())\n" if timed else ""
    )
    code = (
        "import json\n"
        "from pathlib import PurePosixPath\n"
        f"probe_root = 'Files/orchestration/capability/{probe_id}'\n"
        "if probe_marker not in {'external', 'dag_a', 'dag_b', 'dag_join'}:\n"
        "    raise ValueError('Probe parameters were not injected exactly')\n"
        + timing
        + "result = {'marker': probe_marker, 'metadata': 'synthetic_only'}\n"
        + timed_result
        + "notebookutils.fs.put(str(PurePosixPath(probe_root) / "
        "(probe_marker + '.json')), json.dumps(result), False)\n"
        "notebookutils.notebook.exit(json.dumps(result))\n"
    )
    document = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
            "dependencies": {
                "lakehouse": {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": lakehouse_name,
                    "default_lakehouse_workspace_id": workspace_id,
                    "known_lakehouses": [{"id": lakehouse_id}],
                }
            },
        },
        "cells": [
            _code_cell("parameters", 'probe_marker = ""\n', ["parameters"]),
            _code_cell("probe", code, []),
        ],
    }
    return {
        "format": "ipynb",
        "parts": [part("artifact.content.ipynb", json.dumps(document))],
    }


def dag_probe_definition(
    workspace_id: str,
    lakehouse_id: str,
    lakehouse_name: str,
    probe_id: str,
    worker_name: str,
    other_lakehouse_worker: str,
) -> dict:
    for name in (worker_name, other_lakehouse_worker):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,127}", name):
            raise ValueError("Invalid probe worker name")
    definition = notebook_probe_definition(
        workspace_id, lakehouse_id, lakehouse_name, probe_id
    )
    document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
    dag = {
        "activities": [
            {
                "name": name,
                "path": worker_name,
                "timeoutPerCellInSeconds": 120,
                "args": {"probe_marker": marker},
                **({"dependencies": ["a", "b"]} if name == "join" else {}),
            }
            for name, marker in (("a", "dag_a"), ("b", "dag_b"), ("join", "dag_join"))
        ],
        "concurrency": 2,
        "timeoutInSeconds": 600,
    }
    code = (
        "import json\nimport urllib.request\n"
        f"DAG = {dag!r}\n"
        "if notebookutils.notebook.validateDAG(DAG) is not True:\n"
        "    raise RuntimeError('Native DAG validation failed')\n"
        "results = notebookutils.notebook.runMultiple(DAG)\n"
        "if any(result['exception'] for result in results.values()):\n"
        "    raise RuntimeError('A probe DAG child failed')\n"
        "values = {name: json.loads(result['exitVal']) for name, result in results.items()}\n"
        "a, b, joined = values['a'], values['b'], values['join']\n"
        "overlap = max(a['started'], b['started']) < min(a['ended'], b['ended'])\n"
        "ordered = joined['started'] >= max(a['ended'], b['ended'])\n"
        "if not overlap or not ordered:\n"
        "    raise RuntimeError('Native DAG overlap/dependency evidence failed')\n"
        f"request = urllib.request.Request('https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}',\n"
        "    headers={'Authorization': 'Bearer ' + notebookutils.credentials.getToken('pbi'),\n"
        "             'x-ms-fabric-skill': 'spark-cli'})\n"
        "with urllib.request.urlopen(request, timeout=30) as response:\n"
        "    rest_status = response.status\n"
        "try:\n"
        f"    notebookutils.notebook.run({other_lakehouse_worker!r}, 120, {{'probe_marker':'external'}})\n"
        "except Exception as error:\n"
        "    message = str(error).casefold()\n"
        "    if 'lakehouse' not in message or not any(word in message for word in ('same','different','inherit','attached')):\n"
        "        raise\n"
        "    binding_rejected = True\n"
        "else:\n"
        "    raise RuntimeError('Cross-Lakehouse child unexpectedly executed')\n"
        "evidence = {'overlap': overlap, 'dependency_waited': ordered,\n"
        "            'cross_lakehouse_rejected': binding_rejected, 'fabric_rest_read_status': rest_status}\n"
        f"notebookutils.fs.put('Files/orchestration/capability/{probe_id}/dag_results.json', json.dumps(evidence), False)\n"
        "notebookutils.notebook.exit(json.dumps(evidence))\n"
    )
    document["cells"] = [_code_cell("native-dag-probe", code, [])]
    return {
        "format": "ipynb",
        "parts": [part("artifact.content.ipynb", json.dumps(document))],
    }


def script_probe_definition(
    workspace_id: str, database_id: str, connection_id: str, probe_id: str
) -> dict:
    for value in (workspace_id, database_id, connection_id, probe_id):
        if str(uuid.UUID(value)) != value:
            raise ValueError("Probe bindings must be canonical UUIDs")
    document = {
        "properties": {
            "parameters": {
                "p_probe_value": {"type": "String", "defaultValue": probe_id}
            },
            "activities": [
                {
                    "name": "probe_parameterized_script",
                    "type": "Script",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.00:05:00",
                        "retry": 0,
                        "secureInput": True,
                        "secureOutput": False,
                    },
                    "connectionSettings": {
                        "name": "probe_sql_metadata",
                        "properties": {
                            "annotations": [],
                            "type": "FabricSqlDatabase",
                            "typeProperties": {
                                "workspaceId": workspace_id,
                                "artifactId": database_id,
                            },
                            "externalReferences": {"connection": connection_id},
                        },
                    },
                    "typeProperties": {
                        "scripts": [
                            {
                                "type": "Query",
                                "text": "SELECT @probe_value AS probe_value;",
                                "parameters": [
                                    {
                                        "name": "probe_value",
                                        "type": "String",
                                        "direction": "Input",
                                        "value": {
                                            "value": "@pipeline().parameters.p_probe_value",
                                            "type": "Expression",
                                        },
                                    }
                                ],
                            }
                        ],
                        "scriptBlockExecutionTimeout": "00:02:00",
                    },
                }
            ],
        }
    }
    return {"parts": [part("pipeline-content.json", json.dumps(document))]}


def metadata_export_probe_definition(
    workspace_id: str,
    database_id: str,
    lakehouse_id: str,
    connection_ids: tuple[str, str],
    probe_id: str,
    version_id: str,
) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", version_id):
        raise ValueError("Invalid metadata-version token")
    definition = copy_probe_definition(
        workspace_id, database_id, lakehouse_id, connection_ids, probe_id
    )
    document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
    source = document["properties"]["activities"][0]["typeProperties"]["activities"][0][
        "typeProperties"
    ]["source"]
    source["sqlReaderQuery"] = (
        f"SELECT CAST('{probe_id}' AS varchar(36)) AS probe_id,"
        "definition_json,definition_sha256 FROM cfg.configuration_version "
        f"WHERE version_id=N'{version_id}'"
    )
    return {"parts": [part("pipeline-content.json", json.dumps(document))]}


def stored_procedure_copy_probe_definition(
    workspace_id: str,
    database_id: str,
    lakehouse_id: str,
    connection_ids: tuple[str, str],
    probe_id: str,
) -> dict:
    definition = copy_probe_definition(
        workspace_id, database_id, lakehouse_id, connection_ids, probe_id
    )
    document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
    document["properties"]["parameters"]["p_version_id"] = {
        "type": "String",
        "defaultValue": "probe_parameter_must_be_injected",
    }
    source = document["properties"]["activities"][0]["typeProperties"]["activities"][0][
        "typeProperties"
    ]["source"]
    del source["sqlReaderQuery"]
    source["sqlReaderStoredProcedureName"] = "cfg.usp_preview_version"
    source["storedProcedureParameters"] = {
        "version_id": {
            "type": "String",
            "value": {
                "value": "@pipeline().parameters.p_version_id",
                "type": "Expression",
            },
        }
    }
    return {"parts": [part("pipeline-content.json", json.dumps(document))]}


def child_invocation_probe_definition(child_id: str) -> dict:
    if str(uuid.UUID(child_id)) != child_id:
        raise ValueError("Child probe identity must be a canonical UUID")
    document = {
        "properties": {
            "parameters": {
                "p_version_id": {
                    "type": "String",
                    "defaultValue": "treasury-bootstrap-v1",
                }
            },
            "activities": [
                {
                    "name": "invoke_metadata_export",
                    "type": "ExecutePipeline",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.00:10:00",
                        "retry": 0,
                        "secureInput": True,
                        "secureOutput": True,
                    },
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": child_id,
                            "type": "PipelineReference",
                        },
                        "waitOnCompletion": True,
                        "parameters": {
                            "p_version_id": {
                                "value": "@pipeline().parameters.p_version_id",
                                "type": "Expression",
                            }
                        },
                    },
                }
            ],
        }
    }
    return {"parts": [part("pipeline-content.json", json.dumps(document))]}


class CapabilityDefinitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ids = tuple(str(uuid.UUID(int=i + 1)) for i in range(6))

    def test_copy_probe_is_dynamic_bounded_and_reads_no_business_table(self) -> None:
        definition = copy_probe_definition(
            *self.ids[:3], (self.ids[3], self.ids[4]), self.ids[5]
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        loop = document["properties"]["activities"][0]
        copy = loop["typeProperties"]["activities"][0]
        self.assertTrue(loop["typeProperties"]["isSequential"])
        self.assertEqual(copy["policy"]["retry"], 0)
        settings = copy["typeProperties"]["source"]["datasetSettings"]
        self.assertEqual(
            settings["connectionSettings"]["properties"]["externalReferences"][
                "connection"
            ],
            "@item().connection_id",
        )
        self.assertNotIn(" FROM ", copy["typeProperties"]["source"]["sqlReaderQuery"])

    def test_probe_rejects_ambiguous_connections_and_unsafe_identity(self) -> None:
        for connections, probe in (
            ((self.ids[3], self.ids[3]), self.ids[5]),
            ((self.ids[3], self.ids[4]), "../other"),
        ):
            with self.assertRaises(ValueError):
                copy_probe_definition(*self.ids[:3], connections, probe)

    def test_notebook_probe_has_real_parameter_cell_and_fail_fast_guard(self) -> None:
        definition = notebook_probe_definition(
            self.ids[0], self.ids[1], "lh_brz_slv_treasury", self.ids[5]
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        self.assertEqual(document["cells"][0]["metadata"]["tags"], ["parameters"])
        for cell in document["cells"]:
            compile("".join(cell["source"]), "capability-probe", "exec")
            self.assertEqual(cell["outputs"], [])

    def test_script_probe_binds_values_instead_of_interpolating_sql(self) -> None:
        definition = script_probe_definition(*self.ids[:4])
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        script = document["properties"]["activities"][0]["typeProperties"]["scripts"][0]
        self.assertEqual(script["text"], "SELECT @probe_value AS probe_value;")
        self.assertEqual(script["parameters"][0]["direction"], "Input")

    def test_native_dag_probe_uses_bounded_native_orchestration(self) -> None:
        definition = dag_probe_definition(
            self.ids[0],
            self.ids[1],
            "lh_brz_slv_treasury",
            self.ids[5],
            "nb_probe_worker",
            "nb_probe_other_lakehouse",
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        code = "".join(document["cells"][0]["source"])
        compile(code, "dag-probe", "exec")
        self.assertIn("validateDAG", code)
        self.assertIn("runMultiple", code)
        self.assertIn("'concurrency': 2", code)
        self.assertNotIn("ThreadPoolExecutor", code)

    def test_large_export_probe_reads_only_the_requested_control_version(self) -> None:
        definition = metadata_export_probe_definition(
            *self.ids[:3],
            (self.ids[3], self.ids[4]),
            self.ids[5],
            "treasury-bootstrap-v1",
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        query = document["properties"]["activities"][0]["typeProperties"]["activities"][
            0
        ]["typeProperties"]["source"]["sqlReaderQuery"]
        self.assertIn("FROM cfg.configuration_version", query)
        with self.assertRaises(ValueError):
            metadata_export_probe_definition(
                *self.ids[:3], (self.ids[3], self.ids[4]), self.ids[5], "'; DROP TABLE"
            )

    def test_stored_procedure_export_binds_the_version_without_query_concatenation(
        self,
    ) -> None:
        definition = stored_procedure_copy_probe_definition(
            *self.ids[:3], (self.ids[3], self.ids[4]), self.ids[5]
        )
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        source = document["properties"]["activities"][0]["typeProperties"][
            "activities"
        ][0]["typeProperties"]["source"]
        self.assertNotIn("sqlReaderQuery", source)
        self.assertEqual(
            source["sqlReaderStoredProcedureName"], "cfg.usp_preview_version"
        )
        self.assertEqual(
            source["storedProcedureParameters"]["version_id"]["value"],
            {"value": "@pipeline().parameters.p_version_id", "type": "Expression"},
        )

    def test_child_invocation_waits_and_preserves_a_bound_runtime_parameter(
        self,
    ) -> None:
        definition = child_invocation_probe_definition(self.ids[0])
        document = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        activity = document["properties"]["activities"][0]
        self.assertEqual(activity["type"], "ExecutePipeline")
        self.assertTrue(activity["typeProperties"]["waitOnCompletion"])
        self.assertEqual(
            activity["typeProperties"]["pipeline"]["referenceName"], self.ids[0]
        )
        self.assertEqual(
            activity["typeProperties"]["parameters"]["p_version_id"],
            {"value": "@pipeline().parameters.p_version_id", "type": "Expression"},
        )


@unittest.skipUnless(
    os.environ.get("TREASURY_FABRIC_CAPABILITY_PROBES") == "1",
    "A1 opt-in: explicit probe journal and tenant required",
)
class LiveCapabilityReadbackTests(unittest.TestCase):
    def test_approved_probe_jobs_and_persisted_outputs(self) -> None:
        journal = json.loads(
            Path(os.environ["TREASURY_CAPABILITY_JOURNAL"]).read_text(encoding="utf-8")
        )
        tenant = str(uuid.UUID(os.environ["TREASURY_FABRIC_TENANT"]))
        workspace = str(uuid.UUID(journal["workspace_id"]))
        probe_id = str(uuid.UUID(journal["probe_id"]))

        def token_provider() -> str:
            token = azure_cli_token()
            segment = token.split(".")[1]
            claims = json.loads(
                base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
            )
            if claims.get("tid") != tenant:
                raise RuntimeError(
                    "Fabric token directory differs from the approved probe target"
                )
            return token

        client = FabricClient(token_provider)
        workspace_info = client.resolve_workspace(workspace)
        self.assertEqual(workspace_info["displayName"], "ws_kzn_treasury")
        jobs = {}
        keys = [
            "copy",
            "copy_invalid_connection",
            "script_parameters",
            "notebook_parameters",
            "native_dag",
        ]
        large_export = os.environ.get("TREASURY_LARGE_METADATA_PROBE") == "1"
        if large_export:
            keys.append("large_metadata_export")
        for key in keys:
            record = journal["jobs"][key]
            item_id = str(uuid.UUID(record["item_id"]))
            job_id = str(uuid.UUID(record["id"]))
            item = client.request(
                "GET", f"/workspaces/{workspace}/items/{item_id}"
            ).json()
            self.assertTrue(
                item["displayName"].startswith(
                    ("pl_probe_metadata_", "nb_probe_metadata_")
                ),
                "A live capability journal cannot select a business item",
            )
            self.assertTrue(item["displayName"].endswith(probe_id[:8]))
            jobs[key] = client.request(
                "GET",
                f"/workspaces/{workspace}/items/{item_id}/jobs/instances/{job_id}",
            ).json()
            self.assertEqual(
                jobs[key]["status"],
                "Failed" if key == "copy_invalid_connection" else "Completed",
            )
        self.assertTrue(
            "connection"
            in jobs["copy_invalid_connection"]["failureReason"]["message"].casefold()
        )
        script_job = jobs["script_parameters"]

        def utc(value: str) -> datetime:
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

        activities = client.request(
            "POST",
            f"/workspaces/{workspace}/datapipelines/pipelineruns/{script_job['id']}/queryactivityruns",
            body={
                "filters": [],
                "lastUpdatedAfter": (
                    utc(script_job["startTimeUtc"]) - timedelta(hours=1)
                ).isoformat(),
                "lastUpdatedBefore": (
                    utc(script_job["endTimeUtc"]) + timedelta(hours=1)
                ).isoformat(),
            },
        ).json()
        self.assertFalse(activities.get("continuationToken"))
        script = next(
            a
            for a in activities["value"]
            if a["activityName"] == "probe_parameterized_script"
        )
        self.assertEqual(
            script["output"]["resultSets"][0]["rows"],
            [{"probe_value": journal["script_probe_id"]}],
        )
        lakehouse = client.find_item(workspace, "lh_brz_slv_treasury", "Lakehouse")
        if lakehouse is None:
            self.fail("Approved probe Lakehouse is missing")
        details = client.request(
            "GET", f"/workspaces/{workspace}/lakehouses/{lakehouse.id}"
        ).json()
        base = details["properties"]["oneLakeFilesPath"]
        if not re.fullmatch(
            r"https://(?:[a-z0-9-]+-)?onelake\.dfs\.fabric\.microsoft\.com/[A-Za-z0-9/-]+",
            base,
        ):
            raise RuntimeError("Unexpected OneLake credential destination")
        az = shutil.which("az")
        if az is None:
            self.fail("Azure CLI is required for the approved readback")
        storage_token = subprocess.run(
            [
                az,
                "account",
                "get-access-token",
                "--tenant",
                tenant,
                "--resource",
                "https://storage.azure.com",
                "--query",
                "accessToken",
                "-o",
                "tsv",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        ).stdout.strip()

        def read(name: str, fixture_id: str = probe_id) -> str:
            if str(uuid.UUID(fixture_id)) != fixture_id:
                raise RuntimeError("Invalid probe fixture identity")
            request = urllib.request.Request(
                f"{base}/orchestration/capability/{fixture_id}/{name}",
                headers={
                    "Authorization": "Bearer " + storage_token,
                    "x-ms-version": "2021-06-08",
                },
            )
            with urllib.request.urlopen(request, timeout=60) as response:
                data = response.read(16 * 1024 * 1024 + 1)
                if len(data) > 16 * 1024 * 1024:
                    raise RuntimeError(
                        "Probe fixture exceeds its bounded transport size"
                    )
                return data.decode("utf-8-sig")

        for index in range(2):
            rows = list(
                csv.DictReader(
                    io.StringIO(read(f"connection_{index}.csv")), escapechar="\\"
                )
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["probe_id"], probe_id)
            self.assertEqual(
                json.loads(rows[0]["definition_json"]),
                {"schema_version": 1, "message": "synthetic_metadata_probe"},
            )
        self.assertEqual(
            json.loads(read("external.json")),
            {"marker": "external", "metadata": "synthetic_only"},
        )
        dag = json.loads(read("dag_results.json"))
        self.assertTrue(
            all(
                dag[field] is True
                for field in (
                    "overlap",
                    "dependency_waited",
                    "cross_lakehouse_rejected",
                )
            )
        )
        self.assertEqual(dag["fabric_rest_read_status"], 200)
        if large_export:
            fixture_id = journal["large_export_probe_id"]
            for index in range(2):
                text = read(f"connection_{index}.csv", fixture_id)
                csv.field_size_limit(max(csv.field_size_limit(), len(text)))
                rows = list(csv.DictReader(io.StringIO(text), escapechar="\\"))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["probe_id"], fixture_id)
                definition = rows[0]["definition_json"]
                digest = hashlib.sha256(definition.encode("ascii")).hexdigest()
                self.assertTrue(
                    digest
                    == rows[0]["definition_sha256"]
                    == journal["large_export_definition_sha256"],
                    "The control definition was truncated or changed during Copy",
                )
                self.assertGreater(len(definition), 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
