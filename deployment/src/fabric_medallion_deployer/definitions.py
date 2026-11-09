"""Compile portable SQL, notebook and pipeline definitions for selected cases."""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import re
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any

from .contracts import Source
from .metadata import sql_copy_projection

LEGACY_ENVIRONMENT_IDS = frozenset(
    {
        "0448ed4f-990e-4171-83f1-ea2a293e12d9",
        "333965d7-4af9-49d1-bfeb-78cabeccbf8b",
        "afdc0c5b-8ace-4cee-940f-b17076bcc0a7",
        "de838c75-ed06-488c-ae5c-7bb3484e8e53",
        "7f649a72-ce03-4788-8bc1-a92ca208867b",
        "378594a7-2b2f-4d78-97ca-7b4997fbc202",
        "608c836a-24ac-43d7-9275-ff9ba2bca24e",
        "8cea0e2b-5a4d-4996-aa55-a8b9d63faa36",
        "272aa892-f5e9-498f-af1d-054dda8b6d3c",
        "8483e349-36e3-4102-b69f-e24a807df8fb",
        "30005a8a-313e-48c0-9576-23c6b0bdd59c",
        "097d87ac-6153-49d9-94a6-6a53c042173e",
        "5f94118d-3996-43a2-be7b-7b9524a2c6f0",
        "bc3890bc-d0e1-4e1c-8eed-503e2a95f8d9",
    }
)


def json_text(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def part(path: str, payload: str) -> dict[str, str]:
    return {
        "path": path,
        "payload": base64.b64encode(payload.encode("utf-8")).decode("ascii"),
        "payloadType": "InlineBase64",
    }


def ingestion_runtime_sha256(root: Path) -> str:
    code = "\n".join(
        (root / "artefacts" / "medallion" / name).read_text(encoding="utf-8")
        for name in ("rules.py", "ingestion.py")
    )
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def validate_deployable_definition(definition: dict[str, Any]) -> None:
    """Reject malformed parts, unresolved bindings and original environment IDs."""
    parts = definition.get("parts")
    if not parts:
        raise ValueError("An item definition must contain non-empty parts")
    paths = set()
    for item in parts:
        if item["path"] in paths or item["payloadType"] != "InlineBase64":
            raise ValueError("Duplicate definition path or unsupported payload type")
        paths.add(item["path"])
        content = base64.b64decode(item["payload"], validate=True).decode("utf-8")
        if any(value in content for value in LEGACY_ENVIRONMENT_IDS):
            raise ValueError(f"{item['path']}: legacy IDs remain")
        unresolved = re.findall(r"__[A-Z][A-Z0-9_]*__", content)
        if unresolved:
            raise ValueError(
                f"{item['path']}: unresolved tokens: {sorted(set(unresolved))}"
            )


def sql_type(column: dict[str, Any]) -> str:
    """Map inferred feed types to explicit SQL simulator storage."""
    kind = column["data_type"]
    if kind == "varchar":
        return f"nvarchar({column.get('max_length') or 120})"
    if kind == "datetime":
        return (
            "nvarchar(23)"
            if column.get("format") == "CSD_TIMESTAMP"
            else "datetime2(3)"
        )
    if kind == "decimal":
        return f"decimal({column['precision']},{column['scale']})"
    return {
        "int": "int",
        "bigint": "bigint",
        "money": "money",
        "float": "float(53)",
        "bit": "bit",
        "boolean": "bit",
        "uuid": "uniqueidentifier",
    }[kind]


def sql_literal(value: str, column: dict[str, Any]) -> str:
    """Escape a validated seed value without trimming or changing its text."""
    if value == "":
        return "NULL"
    if column["data_type"] == "boolean":
        if value not in {"true", "false"}:
            raise ValueError("Invalid source Boolean")
        return "1" if value == "true" else "0"
    return "N'" + value.replace("'", "''") + "'"


def copy_projection(column: dict[str, Any]) -> str:
    """Serialize SQL values to the source CSV contract instead of connector defaults."""
    return sql_copy_projection(column)


def copy_table_config(source: Source) -> list[dict[str, str]]:
    return [
        {
            "name": table["name"],
            "sourceObject": f"[{source.source_schema}].[{table['name']}]",
            "columnList": ",".join(copy_projection(c) for c in table["columns"]),
            "sinkFolder": f"bronze/{source.code}",
            "filePrefix": table["name"],
        }
        for table in source.tables
    ]


def sql_database_definition(source: Source) -> dict[str, Any]:
    """Create an idempotent SQL project with a separate seed part per table."""
    schema = source.source_schema
    script = [
        "SET NOCOUNT ON;\nGO",
        f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');\nGO",
    ]
    seed_parts = []
    for table in source.tables:
        name = table["name"]
        target = f"[{schema}].[{name}]"
        declarations = [
            f"    [{col['name']}] {sql_type(col)} "
            + (
                "NOT NULL"
                if not col["nullable"] or col["name"] in table["keys"]
                else "NULL"
            )
            for col in table["columns"]
        ]
        keys = ", ".join(f"[{key}]" for key in table["keys"])
        declarations.append(f"    CONSTRAINT [pk_{schema}_{name}] PRIMARY KEY ({keys})")
        script.append(
            f"IF OBJECT_ID(N'{schema}.{name}',N'U') IS NULL\nBEGIN\n"
            f"CREATE TABLE {target} (\n" + ",\n".join(declarations) + "\n);\nEND;\nGO"
        )
        columns = table["columns"]
        names = ",".join(f"[{c['name']}]" for c in columns)
        updates = ",".join(
            f"target.[{c['name']}]=source.[{c['name']}]"
            for c in columns
            if c["name"] not in table["keys"]
        )
        join = " AND ".join(f"target.[{k}]=source.[{k}]" for k in table["keys"])
        inserts = ",".join(f"source.[{c['name']}]" for c in columns)
        blocks = []
        with source.seed_file(name).open(encoding="utf-8", newline="") as stream:
            batch = []
            for row in csv.DictReader(stream):
                batch.append(
                    "("
                    + ",".join(sql_literal(row[c["name"]], c) for c in columns)
                    + ")"
                )
                if len(batch) == 100:
                    blocks.append(batch)
                    batch = []
            if batch:
                blocks.append(batch)
        merges = []
        for batch in blocks:
            matched = f"WHEN MATCHED THEN UPDATE SET {updates}\n" if updates else ""
            merges.append(
                f"MERGE {target} AS target\nUSING (VALUES\n"
                + ",\n".join(batch)
                + f"\n) AS source ({names})\nON {join}\n{matched}"
                + f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({inserts});\nGO\n"
            )
        seed_path = f"Seeds/{name}.sql"
        seed_parts.append(
            part(seed_path, "\n".join(merges) or "-- Allowed empty seed\n")
        )
        script.append(f":r .\\Seeds\\{name}.sql\n")
    project = f"""<Project Sdk="Microsoft.Build.Sql/1.0.0">
  <PropertyGroup>
    <Name>{source.database}</Name>
    <DSP>Microsoft.Data.Tools.Schema.Sql.SqlDbFabricDatabaseSchemaProvider</DSP>
    <ModelCollation>1033, CS</ModelCollation>
    <DefaultCollation>Latin1_General_100_BIN2_UTF8</DefaultCollation>
  </PropertyGroup>
  <ItemGroup>
    <Build Remove="**\\*.sql" />
    <PostDeploy Include="Deploy.sql" />
  </ItemGroup>
</Project>
"""
    definition = {
        "format": "sqlproj",
        "parts": [
            part("sqldb.sqlproj", project),
            part("Deploy.sql", "\n\n".join(script)),
            *seed_parts,
        ],
    }
    validate_deployable_definition(definition)
    return definition


def notebook_document(root: Path, source: Source, step: str) -> dict[str, Any]:
    """Render the portable notebook through tagged, self-contained code cells."""
    name = source.notebooks[step]
    runtime = (root / "artefacts" / "medallion" / "ingestion.py").read_text(
        encoding="utf-8"
    )
    rules = (root / "artefacts" / "medallion" / "rules.py").read_text(encoding="utf-8")
    cells = [
        {
            "cell_type": "markdown",
            "id": "purpose",
            "metadata": {},
            "source": [
                f"# {name}\n",
                f"\nProcess {source.code.upper()} synthetic source data in {source.lakehouse}.\n",
                f"\nStep: `{step}`. Source fields and timezone semantics are preserved.\n",
                "\nRequires Fabric PySpark, Delta, notebookutils and the selected case Lakehouse.\n",
                "\nParameters: processing_date, pipeline_run_id, batch_started_utc (UTC ISO timestamp).\n",
                "\nSilver source tables are append-only business versions with SHA-256 hashes and batch lineage.\n",
                "\nThe legacy cleanup step verifies retention; it never deletes Bronze batches.\n",
                "\nGenerated by deployment/build.py; edit the shared runtime or source contracts instead.\n",
            ],
        },
        _code_cell(
            "parameters",
            'processing_date = ""\npipeline_run_id = ""\nbatch_started_utc = ""\n',
            ["parameters"],
        ),
        _code_cell(
            "source-contract",
            "import json\nSOURCE_CONFIG = json.loads("
            + repr(json.dumps(source.runtime_config()))
            + ")\n",
            ["source_contract"],
        ),
        _code_cell("medallion-rules", rules, ["medallion_rules"]),
        _code_cell("medallion-runtime", runtime, ["medallion_runtime"]),
        _code_cell(
            "execute",
            f'run_step(spark, notebookutils, SOURCE_CONFIG, "{step}", '
            "processing_date=processing_date, pipeline_run_id=pipeline_run_id, "
            "batch_started_utc=batch_started_utc)\n",
            [],
        ),
    ]
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _code_cell(name: str, source: str, tags: list[str]) -> dict[str, Any]:
    return {
        "cell_type": "code",
        "id": name,
        "metadata": {"tags": tags},
        "execution_count": None,
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def notebook_definition(
    root: Path,
    source: Source,
    step: str,
    *,
    workspace_id: str,
    lakehouse_id: str,
) -> dict[str, Any]:
    # Historical rollback definitions are reconstructed, never normal outputs.
    notebook = notebook_document(root, source, step)
    notebook["metadata"]["dependencies"] = {
        "lakehouse": {
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": source.lakehouse,
            "default_lakehouse_workspace_id": workspace_id,
            "known_lakehouses": [{"id": lakehouse_id}],
        }
    }
    return {
        "format": "ipynb",
        "parts": [part("artifact.content.ipynb", json_text(notebook))],
    }


def pipeline_definition(
    root: Path,
    source: Source,
    *,
    workspace_id: str,
    sql_database_id: str,
    bronze_lakehouse_id: str,
    sql_connection_id: str,
    notebook_ids: dict[str, str],
    use_case: str | None = None,
) -> dict[str, Any]:
    from .asset_paths import resolve_asset_path, use_case_descriptor

    descriptor = use_case_descriptor(root, use_case)
    relative = descriptor.get("legacy_pipeline_template")
    if relative is None:
        raise ValueError(
            "Selected case does not declare a historical pipeline template"
        )
    template = json.loads(
        resolve_asset_path(root, relative, descriptor["id"]).read_text(encoding="utf-8")
    )
    payload = deepcopy(template)
    replacements = {
        "__SOURCE__": source.code,
        "__WORKSPACE_ID__": workspace_id,
        "__SQL_DATABASE_ID__": sql_database_id,
        "__SQL_CONNECTION_ID__": sql_connection_id,
        "__BRONZE_LAKEHOUSE_ID__": bronze_lakehouse_id,
        "__CONNECTION_NAME__": source.connection,
        "__LAKEHOUSE_NAME__": source.lakehouse,
    }
    for step, name in source.notebooks.items():
        replacements[f"__NB_{step.upper()}_ID__"] = notebook_ids[name]
        replacements[f"__NB_{step.upper()}_NAME__"] = name
    content = json.dumps(payload)
    for old, value in replacements.items():
        content = content.replace(old, value)
    payload = json.loads(content)
    payload["properties"]["parameters"]["p_table_config"]["defaultValue"] = (
        copy_table_config(source)
    )
    for activity in payload["properties"]["activities"]:
        if activity["type"] == "TridentNotebook":
            activity["policy"] = {
                "timeout": "0.12:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureInput": True,
                "secureOutput": True,
            }
            activity["typeProperties"]["parameters"] = {
                "processing_date": {
                    "value": {
                        "value": "@formatDateTime(variables('var_run_upper_bound_utc'),'yyyy-MM-dd')",
                        "type": "Expression",
                    },
                    "type": "string",
                },
                "pipeline_run_id": {
                    "value": {"value": "@pipeline().RunId", "type": "Expression"},
                    "type": "string",
                },
                "batch_started_utc": {
                    "value": {
                        "value": "@variables('var_run_upper_bound_utc')",
                        "type": "Expression",
                    },
                    "type": "string",
                },
            }
    definition = {"parts": [part("pipeline-content.json", json_text(payload))]}
    validate_deployable_definition(definition)
    return definition


def _metadata_pipeline_template(
    root: Path, name: str, replacements: dict[str, str]
) -> dict[str, Any]:
    document = json.loads(
        (root / "artefacts" / "pipeline" / name).read_text(encoding="utf-8")
    )

    def bind(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: bind(child) for key, child in value.items()}
        if isinstance(value, list):
            return [bind(child) for child in value]
        return replacements.get(value, value) if isinstance(value, str) else value

    return bind(document)


def _metadata_bindings(
    workspace_id: str,
    control_database_id: str,
    control_connection_id: str,
    bronze_lakehouse_id: str,
) -> dict[str, str]:
    bindings = {
        "__WORKSPACE_ID__": workspace_id,
        "__CONTROL_DATABASE_ID__": control_database_id,
        "__CONTROL_CONNECTION_ID__": control_connection_id,
        "__BRONZE_LAKEHOUSE_ID__": bronze_lakehouse_id,
    }
    for value in bindings.values():
        if str(uuid.UUID(value)) != value:
            raise ValueError(
                "Metadata pipeline bindings must be resolved canonical UUIDs"
            )
    return bindings


def metadata_copy_pipeline_definition(
    root: Path,
    *,
    workspace_id: str,
    control_database_id: str,
    control_connection_id: str,
    bronze_lakehouse_id: str,
    prepare_notebook_id: str,
) -> dict[str, Any]:
    bindings = _metadata_bindings(
        workspace_id, control_database_id, control_connection_id, bronze_lakehouse_id
    )
    document = _metadata_pipeline_template(
        root, "copy_bronze_batch_pipeline.json", bindings
    )
    if str(uuid.UUID(prepare_notebook_id)) != prepare_notebook_id:
        raise ValueError(
            "Callback verification requires a resolved preparation notebook"
        )
    parameters = document["properties"]["parameters"]
    for name, default in (
        ("p_operation", "copy"),
        ("p_manifest_path", ""),
        ("p_snapshot_sha256", ""),
        ("p_version_id", ""),
        ("p_result_page_path", ""),
        ("p_result_page_sha256", ""),
        ("p_file_arrival_path", ""),
        ("p_file_arrival_sha256", ""),
    ):
        parameters[name] = {"type": "String", "defaultValue": default}
    switch = document["properties"]["activities"][1]["typeProperties"]["activities"][0]
    for kind in ("FabricSql", "SqlServer", "Ftp", "Sftp"):
        adapter = source_copy_adapter(root, kind)
        serialized = json_text(adapter)
        for token, value in bindings.items():
            serialized = serialized.replace(token, value)
        switch["typeProperties"]["cases"].append(
            {
                "value": kind,
                "activities": [json.loads(serialized)],
            }
        )
    lookup, loop = document["properties"]["activities"]
    lookup["typeProperties"]["source"]["storedProcedureParameters"]["operation"] = {
        "type": "String",
        "value": {"value": "@pipeline().parameters.p_operation", "type": "Expression"},
    }
    connection = lookup["typeProperties"]["source"]["datasetSettings"][
        "connectionSettings"
    ]
    prepare = {
        "name": "prepare_copy_attempt",
        "type": "Script",
        "dependsOn": [],
        "policy": {
            "timeout": "0.00:10:00",
            "retry": 0,
            "secureInput": True,
            "secureOutput": True,
        },
        "connectionSettings": connection,
        "typeProperties": {
            "scripts": [
                {
                    "type": "Query",
                    "text": "EXEC ctl.usp_prepare_copy_attempt @run_id,@task_id,@execution_run_id,@workspace_id;",
                    "parameters": [
                        {
                            "name": name,
                            "type": "String",
                            "direction": "Input",
                            "value": {"value": expression, "type": "Expression"},
                        }
                        for name, expression in (
                            ("run_id", "@pipeline().parameters.p_run_id"),
                            ("task_id", "@item().task_id"),
                            ("execution_run_id", "@pipeline().RunId"),
                        )
                    ]
                    + [
                        {
                            "name": "workspace_id",
                            "type": "String",
                            "direction": "Input",
                            "value": workspace_id,
                        }
                    ],
                }
            ],
            "scriptBlockExecutionTimeout": "00:05:00",
        },
    }
    switch["dependsOn"] = [
        {"activity": prepare["name"], "dependencyConditions": ["Succeeded"]}
    ]
    for case in switch["typeProperties"]["cases"]:
        case["activities"][0]["userProperties"] = [
            {"name": "run_id", "value": "@pipeline().parameters.p_run_id"},
            {"name": "task_id", "value": "@item().task_id"},
            {
                "name": "attempt_id",
                "value": "@activity('prepare_copy_attempt').output.resultSets[0].rows[0].attempt_id",
            },
        ]
    loop["typeProperties"]["activities"].insert(0, prepare)
    policy = {
        "timeout": "0.01:00:00",
        "retry": 0,
        "secureInput": True,
        "secureOutput": True,
    }
    verify = {
        "name": "verify_callback_page",
        "type": "TridentNotebook",
        "dependsOn": [],
        "policy": policy,
        "typeProperties": {
            "notebookId": prepare_notebook_id,
            "workspaceId": workspace_id,
            "parameters": {
                name: {
                    "type": "string",
                    "value": {"value": value, "type": "Expression"}
                    if value.startswith("@")
                    else value,
                }
                for name, value in (
                    ("operation", "verify_callbacks"),
                    ("run_id", "@pipeline().parameters.p_run_id"),
                    ("version_id", "@pipeline().parameters.p_version_id"),
                    ("snapshot_sha256", "@pipeline().parameters.p_snapshot_sha256"),
                    ("manifest_path", "@pipeline().parameters.p_manifest_path"),
                    ("callback_path", "@pipeline().parameters.p_result_page_path"),
                    ("callback_sha256", "@pipeline().parameters.p_result_page_sha256"),
                    ("file_arrival_path", "@pipeline().parameters.p_file_arrival_path"),
                    (
                        "file_arrival_sha256",
                        "@pipeline().parameters.p_file_arrival_sha256",
                    ),
                )
            },
        },
    }
    read_results = {
        "name": "lookup_callback_rows",
        "type": "Lookup",
        "dependsOn": [
            {"activity": verify["name"], "dependencyConditions": ["Succeeded"]}
        ],
        "policy": policy,
        "typeProperties": {
            "firstRowOnly": False,
            "source": {
                "type": "DelimitedTextSource",
                "storeSettings": {"type": "LakehouseReadSettings", "recursive": False},
                "formatSettings": {"type": "DelimitedTextReadSettings"},
                "datasetSettings": {
                    "annotations": [],
                    "type": "DelimitedText",
                    "schema": [],
                    "typeProperties": {
                        "location": {
                            "type": "LakehouseLocation",
                            "folderPath": {
                                "value": "@substring(pipeline().parameters.p_result_page_path,6,sub(lastIndexOf(pipeline().parameters.p_result_page_path,'/'),6))",
                                "type": "Expression",
                            },
                            "fileName": {
                                "value": "@last(split(pipeline().parameters.p_result_page_path,'/'))",
                                "type": "Expression",
                            },
                        },
                        "columnDelimiter": ",",
                        "escapeChar": "\\",
                        "firstRowAsHeader": True,
                        "quoteChar": '"',
                    },
                    "linkedService": {
                        "name": "metadata_bronze_lakehouse",
                        "properties": {
                            "annotations": [],
                            "type": "Lakehouse",
                            "typeProperties": {
                                "workspaceId": workspace_id,
                                "artifactId": bronze_lakehouse_id,
                                "rootFolder": "Files",
                            },
                        },
                    },
                },
            },
        },
    }
    accept = {
        "name": "accept_callback_rows",
        "type": "ForEach",
        "dependsOn": [
            {"activity": read_results["name"], "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
            "items": {
                "value": "@activity('lookup_callback_rows').output.value",
                "type": "Expression",
            },
            "isSequential": True,
            "activities": [
                {
                    "name": "reconcile_task_result",
                    "type": "Script",
                    "dependsOn": [],
                    "policy": policy,
                    "connectionSettings": connection,
                    "typeProperties": {
                        "scripts": [
                            {
                                "type": "NonQuery",
                                "text": "EXEC ctl.usp_reconcile_pipeline_result @result_json,@result_sha256,@observed_job_status,@job_evidence_sha256,@job_location;",
                                "parameters": [
                                    {
                                        "name": name,
                                        "type": "String",
                                        "direction": "Input",
                                        "value": {
                                            "value": "@item()." + name,
                                            "type": "Expression",
                                        },
                                    }
                                    for name in (
                                        "result_json",
                                        "result_sha256",
                                        "observed_job_status",
                                        "job_evidence_sha256",
                                        "job_location",
                                    )
                                ],
                            }
                        ],
                        "scriptBlockExecutionTimeout": "00:05:00",
                    },
                }
            ],
        },
    }
    document["properties"]["activities"].append(
        {
            "name": "reconcile_metadata_page",
            "type": "IfCondition",
            "dependsOn": [
                {"activity": loop["name"], "dependencyConditions": ["Succeeded"]}
            ],
            "typeProperties": {
                "expression": {
                    "value": "@equals(pipeline().parameters.p_operation,'reconcile')",
                    "type": "Expression",
                },
                "ifTrueActivities": [verify, read_results, accept],
                "ifFalseActivities": [],
            },
        }
    )
    definition = {"parts": [part("pipeline-content.json", json_text(document))]}
    validate_deployable_definition(definition)
    return definition


def file_copy_adapter(root: Path, source_type: str) -> dict[str, Any]:
    if source_type not in {"ftp", "sftp"}:
        raise ValueError("An explicit FTP or SFTP adapter is required")
    return source_copy_adapter(root, {"ftp": "Ftp", "sftp": "Sftp"}[source_type])


def source_copy_adapter(root: Path, connector_type: str) -> dict[str, Any]:
    templates = {
        "FabricSql": "fabric_sql_database_copy.json",
        "SqlServer": "sql_server_copy.json",
        "Ftp": "ftp_copy.json",
        "Sftp": "sftp_copy.json",
    }
    if connector_type not in templates:
        raise ValueError("An explicit supported source connector is required")
    return json.loads(
        (
            root / "artefacts" / "pipeline" / "connectors" / templates[connector_type]
        ).read_text(encoding="utf-8")
    )


def metadata_master_pipeline_definition(
    root: Path,
    *,
    workspace_id: str,
    control_database_id: str,
    control_connection_id: str,
    bronze_lakehouse_id: str,
    copy_pipeline_id: str,
    notebook_ids: dict[str, str],
) -> dict[str, Any]:
    bindings = _metadata_bindings(
        workspace_id, control_database_id, control_connection_id, bronze_lakehouse_id
    )
    if set(notebook_ids) != {"prepare", "silver", "gold"}:
        raise ValueError("All generic medallion notebook bindings are required")
    for value in (copy_pipeline_id, *notebook_ids.values()):
        if str(uuid.UUID(value)) != value:
            raise ValueError(
                "Metadata pipeline bindings must be resolved canonical UUIDs"
            )
    bindings.update(
        {
            "__COPY_PIPELINE_ID__": copy_pipeline_id,
            "__PREPARE_NOTEBOOK_ID__": notebook_ids["prepare"],
            "__SILVER_NOTEBOOK_ID__": notebook_ids["silver"],
            "__GOLD_NOTEBOOK_ID__": notebook_ids["gold"],
        }
    )
    document = _metadata_pipeline_template(
        root, "bronze_silver_gold_pipeline.json", bindings
    )
    document["properties"]["parameters"].update(
        {
            "p_file_arrival_path": {"type": "String", "defaultValue": ""},
            "p_file_arrival_sha256": {"type": "String", "defaultValue": ""},
        }
    )
    context = {
        "run_id": "@activity('prepare_pinned_run').output.resultSets[0].rows[0].run_id",
        "version_id": "@activity('prepare_pinned_run').output.resultSets[0].rows[0].version_id",
        "snapshot_sha256": "@activity('prepare_pinned_run').output.resultSets[0].rows[0].snapshot_sha256",
        "manifest_path": "@concat('Files/orchestration/',activity('prepare_pinned_run').output.resultSets[0].rows[0].run_id,'/snapshot_',pipeline().RunId,'.csv')",
        "file_arrival_path": "@pipeline().parameters.p_file_arrival_path",
        "file_arrival_sha256": "@pipeline().parameters.p_file_arrival_sha256",
    }
    for activity in document["properties"]["activities"]:
        if activity["type"] == "TridentNotebook":
            activity["typeProperties"]["parameters"] = {
                name: {
                    "value": {"value": expression, "type": "Expression"},
                    "type": "string",
                }
                for name, expression in context.items()
            }
    activities = document["properties"]["activities"]
    policy = {
        "timeout": "0.01:00:00",
        "retry": 0,
        "secureInput": True,
        "secureOutput": True,
    }
    info = {
        "name": "bronze_dispatch_info",
        "type": "Script",
        "dependsOn": [
            {"activity": "dispatch_bronze_pages", "dependencyConditions": ["Completed"]}
        ],
        "policy": policy,
        "connectionSettings": activities[0]["connectionSettings"],
        "typeProperties": {
            "scripts": [
                {
                    "type": "Query",
                    "text": "EXEC ctl.usp_export_pipeline_dispatch @run_id,@layer,@include_payload=0;",
                    "parameters": [
                        {
                            "name": "run_id",
                            "type": "String",
                            "direction": "Input",
                            "value": {"value": context["run_id"], "type": "Expression"},
                        },
                        {
                            "name": "layer",
                            "type": "String",
                            "direction": "Input",
                            "value": "bronze",
                        },
                    ],
                }
            ],
            "scriptBlockExecutionTimeout": "00:05:00",
        },
    }
    export = json.loads(json.dumps(activities[1]))
    export["name"] = "export_bronze_dispatch"
    export["dependsOn"] = [
        {"activity": info["name"], "dependencyConditions": ["Succeeded"]}
    ]
    source = export["typeProperties"]["source"]
    source["sqlReaderStoredProcedureName"] = "ctl.usp_export_pipeline_dispatch"
    source["storedProcedureParameters"]["layer"] = {"type": "String", "value": "bronze"}
    export["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["location"][
        "fileName"
    ] = {
        "value": "@concat('bronze_dispatch_',pipeline().RunId,'.csv')",
        "type": "Expression",
    }
    record = {
        "name": "reconcile_bronze",
        "type": "TridentNotebook",
        "dependsOn": [
            {"activity": export["name"], "dependencyConditions": ["Succeeded"]}
        ],
        "policy": policy,
        "typeProperties": {
            "notebookId": notebook_ids["prepare"],
            "workspaceId": workspace_id,
            "parameters": {
                name: {
                    "type": "string",
                    "value": {"value": value, "type": "Expression"}
                    if value.startswith("@")
                    else value,
                }
                for name, value in {
                    **context,
                    "operation": "reconcile_bronze",
                    "dispatch_path": "@concat('Files/orchestration/',activity('prepare_pinned_run').output.resultSets[0].rows[0].run_id,'/bronze_dispatch_',pipeline().RunId,'.csv')",
                    "dispatch_sha256": "@activity('bronze_dispatch_info').output.resultSets[0].rows[0].dispatch_sha256",
                    "reconcile_observed_utc": "@formatDateTime(utcNow(),'yyyy-MM-ddTHH:mm:ss.ffffffZ')",
                }.items()
            },
        },
    }
    callback = {
        "name": "reconcile_bronze_pages",
        "type": "ForEach",
        "dependsOn": [
            {"activity": record["name"], "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
            "items": {
                "value": "@json(activity('reconcile_bronze').output.result.exitValue).callbacks",
                "type": "Expression",
            },
            "isSequential": True,
            "activities": [
                {
                    "name": "ingest_bronze_result_page",
                    "type": "ExecutePipeline",
                    "dependsOn": [],
                    "policy": policy,
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": copy_pipeline_id,
                            "type": "PipelineReference",
                        },
                        "waitOnCompletion": True,
                        "parameters": {
                            name: {"value": value, "type": "Expression"}
                            if value.startswith("@")
                            else value
                            for name, value in {
                                "p_operation": "reconcile",
                                "p_run_id": context["run_id"],
                                "p_version_id": context["version_id"],
                                "p_snapshot_sha256": context["snapshot_sha256"],
                                "p_file_arrival_path": context["file_arrival_path"],
                                "p_file_arrival_sha256": context["file_arrival_sha256"],
                                "p_manifest_path": context["manifest_path"],
                                "p_result_page_path": "@item().path",
                                "p_result_page_sha256": "@item().sha256",
                            }.items()
                        },
                    },
                }
            ],
        },
    }
    finish = json.loads(json.dumps(info))
    finish["name"] = "finish_bronze_dispatch"
    finish["dependsOn"] = [
        {"activity": callback["name"], "dependencyConditions": ["Succeeded"]}
    ]
    finish["typeProperties"]["scripts"][0] = {
        "type": "Query",
        "text": "EXEC ctl.usp_finish_bronze_dispatch @run_id,@recovery_sha256;",
        "parameters": [
            {
                "name": name,
                "type": "String",
                "direction": "Input",
                "value": {"value": value, "type": "Expression"},
            }
            for name, value in (
                ("run_id", context["run_id"]),
                (
                    "recovery_sha256",
                    "@json(activity('reconcile_bronze').output.result.exitValue).recovery_sha256",
                ),
            )
        ],
    }
    gate = {
        "name": "require_bronze_success",
        "type": "IfCondition",
        "dependsOn": [
            {"activity": finish["name"], "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
            "expression": {
                "value": "@json(activity('reconcile_bronze').output.result.exitValue).has_failed",
                "type": "Expression",
            },
            "ifTrueActivities": [
                {
                    "name": "fail_bronze_results",
                    "type": "Fail",
                    "typeProperties": {
                        "errorCode": "BRONZE_TASK_FAILED",
                        "message": "Copy/output evidence failed; reconciled results are retained and Silver remains blocked.",
                    },
                }
            ],
            "ifFalseActivities": [],
        },
    }
    silver = next(
        activity for activity in activities if activity["name"] == "orchestrate_silver"
    )
    silver["dependsOn"] = [
        {"activity": gate["name"], "dependencyConditions": ["Succeeded"]}
    ]
    insertion = activities.index(silver)
    activities[insertion:insertion] = [info, export, record, callback, finish, gate]
    prepare_silver = json.loads(json.dumps(info))
    prepare_silver["name"] = "prepare_silver_attempts"
    prepare_silver["dependsOn"] = [
        {"activity": gate["name"], "dependencyConditions": ["Succeeded"]}
    ]
    script = prepare_silver["typeProperties"]["scripts"][0]
    script["text"] = (
        "EXEC ctl.usp_prepare_notebook_layer @run_id,@layer,@execution_run_id,@workspace_id;"
    )
    script["parameters"][1]["value"] = "silver"
    script["parameters"].extend(
        [
            {
                "name": "execution_run_id",
                "type": "String",
                "direction": "Input",
                "value": {"value": "@pipeline().RunId", "type": "Expression"},
            },
            {
                "name": "workspace_id",
                "type": "String",
                "direction": "Input",
                "value": workspace_id,
            },
        ]
    )
    export_silver = json.loads(json.dumps(export))
    export_silver["name"] = "export_silver_dispatch"
    export_silver["dependsOn"] = [
        {"activity": prepare_silver["name"], "dependencyConditions": ["Succeeded"]}
    ]
    export_silver["typeProperties"]["source"]["storedProcedureParameters"]["layer"][
        "value"
    ] = "silver"
    export_silver["typeProperties"]["sink"]["datasetSettings"]["typeProperties"][
        "location"
    ]["fileName"] = {
        "value": "@concat('silver_dispatch_',pipeline().RunId,'.csv')",
        "type": "Expression",
    }
    silver["dependsOn"] = [
        {"activity": export_silver["name"], "dependencyConditions": ["Succeeded"]}
    ]
    silver["typeProperties"]["parameters"].update(
        {
            "dispatch_path": {
                "type": "string",
                "value": {
                    "value": "@concat('Files/orchestration/',activity('prepare_pinned_run').output.resultSets[0].rows[0].run_id,'/silver_dispatch_',pipeline().RunId,'.csv')",
                    "type": "Expression",
                },
            },
            "dispatch_sha256": {
                "type": "string",
                "value": {
                    "value": "@activity('prepare_silver_attempts').output.resultSets[0].rows[0].dispatch_sha256",
                    "type": "Expression",
                },
            },
        }
    )
    silver["userProperties"] = [
        {"name": "run_id", "value": context["run_id"]},
        {"name": "layer", "value": "silver"},
        {
            "name": "dispatch_sha256",
            "value": "@activity('prepare_silver_attempts').output.resultSets[0].rows[0].dispatch_sha256",
        },
    ]
    insertion = activities.index(silver)
    activities[insertion:insertion] = [prepare_silver, export_silver]
    record_silver = json.loads(json.dumps(record))
    record_silver["name"] = "reconcile_silver"
    record_silver["dependsOn"] = [
        {"activity": silver["name"], "dependencyConditions": ["Completed"]}
    ]
    record_silver["typeProperties"]["parameters"]["operation"]["value"] = (
        "reconcile_silver"
    )
    record_silver["typeProperties"]["parameters"]["dispatch_path"] = silver[
        "typeProperties"
    ]["parameters"]["dispatch_path"]
    record_silver["typeProperties"]["parameters"]["dispatch_sha256"] = silver[
        "typeProperties"
    ]["parameters"]["dispatch_sha256"]
    callback_silver = json.loads(json.dumps(callback))
    callback_silver["name"] = "reconcile_silver_pages"
    callback_silver["dependsOn"] = [
        {"activity": record_silver["name"], "dependencyConditions": ["Succeeded"]}
    ]
    callback_silver["typeProperties"]["items"]["value"] = (
        "@json(activity('reconcile_silver').output.result.exitValue).callbacks"
    )
    callback_silver["typeProperties"]["activities"][0]["name"] = (
        "ingest_silver_result_page"
    )
    gate_silver = json.loads(json.dumps(gate))
    gate_silver["name"] = "require_silver_success"
    gate_silver["dependsOn"] = [
        {"activity": callback_silver["name"], "dependencyConditions": ["Succeeded"]}
    ]
    gate_silver["typeProperties"]["expression"]["value"] = (
        "@json(activity('reconcile_silver').output.result.exitValue).has_failed"
    )
    gate_silver["typeProperties"]["ifTrueActivities"][0] = {
        "name": "fail_silver_results",
        "type": "Fail",
        "typeProperties": {
            "errorCode": "SILVER_TASK_FAILED",
            "message": "Silver output/source-commit evidence failed; reconciled results are retained and Gold remains blocked.",
        },
    }
    gold = next(
        activity for activity in activities if activity["name"] == "orchestrate_gold"
    )
    gold["dependsOn"] = [
        {"activity": gate_silver["name"], "dependencyConditions": ["Succeeded"]}
    ]
    insertion = activities.index(gold)
    activities[insertion:insertion] = [record_silver, callback_silver, gate_silver]
    prepare_gold = deepcopy(prepare_silver)
    prepare_gold["name"] = "prepare_gold_attempts"
    prepare_gold["dependsOn"] = []
    for parameter in prepare_gold["typeProperties"]["scripts"][0]["parameters"]:
        if parameter["name"] == "layer":
            parameter["value"] = "gold"
    export_gold = deepcopy(export_silver)
    export_gold["name"] = "export_gold_dispatch"
    export_gold["dependsOn"] = [
        {"activity": prepare_gold["name"], "dependencyConditions": ["Succeeded"]}
    ]
    export_gold["typeProperties"]["source"]["storedProcedureParameters"]["layer"][
        "value"
    ] = "gold"
    export_gold["typeProperties"]["sink"]["datasetSettings"]["typeProperties"][
        "location"
    ]["fileName"] = {
        "value": "@concat('gold_dispatch_',pipeline().RunId,'.csv')",
        "type": "Expression",
    }
    gold["dependsOn"] = [
        {"activity": export_gold["name"], "dependencyConditions": ["Succeeded"]}
    ]
    gold["typeProperties"]["parameters"].update(
        {
            "dispatch_path": {
                "type": "string",
                "value": {
                    "value": "@concat('Files/orchestration/',activity('prepare_pinned_run').output.resultSets[0].rows[0].run_id,'/gold_dispatch_',pipeline().RunId,'.csv')",
                    "type": "Expression",
                },
            },
            "dispatch_sha256": {
                "type": "string",
                "value": {
                    "value": "@activity('prepare_gold_attempts').output.resultSets[0].rows[0].dispatch_sha256",
                    "type": "Expression",
                },
            },
        }
    )
    gold["userProperties"] = [
        {"name": "run_id", "value": context["run_id"]},
        {"name": "layer", "value": "gold"},
        {
            "name": "dispatch_sha256",
            "value": "@activity('prepare_gold_attempts').output.resultSets[0].rows[0].dispatch_sha256",
        },
    ]
    record_gold = deepcopy(record_silver)
    record_gold["name"] = "reconcile_gold"
    record_gold["dependsOn"] = [
        {"activity": gold["name"], "dependencyConditions": ["Completed"]}
    ]
    record_gold["typeProperties"]["notebookId"] = notebook_ids["gold"]
    record_gold["typeProperties"]["parameters"]["operation"]["value"] = "reconcile_gold"
    for name in ("dispatch_path", "dispatch_sha256"):
        record_gold["typeProperties"]["parameters"][name] = deepcopy(
            gold["typeProperties"]["parameters"][name]
        )
    callback_gold = deepcopy(callback_silver)
    callback_gold["name"] = "reconcile_gold_pages"
    callback_gold["dependsOn"] = [
        {"activity": record_gold["name"], "dependencyConditions": ["Succeeded"]}
    ]
    callback_gold["typeProperties"]["items"]["value"] = (
        "@json(activity('reconcile_gold').output.result.exitValue).callbacks"
    )
    callback_gold["typeProperties"]["activities"][0]["name"] = "ingest_gold_result_page"
    gate_gold = deepcopy(record_gold)
    gate_gold["name"] = "require_gold_success"
    gate_gold["dependsOn"] = [
        {"activity": callback_gold["name"], "dependencyConditions": ["Succeeded"]}
    ]
    gate_gold["typeProperties"]["parameters"]["operation"]["value"] = (
        "require_gold_success"
    )
    for name in ("gold_reconciliation_path", "gold_reconciliation_sha256"):
        gate_gold["typeProperties"]["parameters"][name] = {
            "type": "string",
            "value": {
                "value": f"@json(activity('reconcile_gold').output.result.exitValue).{name}",
                "type": "Expression",
            },
        }
    selected_gold = {
        "name": "selected_gold",
        "type": "IfCondition",
        "dependsOn": [
            {"activity": gate_silver["name"], "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
            "expression": {
                "value": "@contains(json(activity('prepare_dispatch').output.result.exitValue).layers,'gold')",
                "type": "Expression",
            },
            "ifTrueActivities": [
                prepare_gold,
                export_gold,
                gold,
                record_gold,
                callback_gold,
                gate_gold,
            ],
            "ifFalseActivities": [],
        },
    }
    activities[activities.index(gold)] = selected_gold
    definition = {"parts": [part("pipeline-content.json", json_text(document))]}
    validate_deployable_definition(definition)
    return definition
