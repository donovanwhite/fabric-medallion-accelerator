"""Compile an unbound draft control-plane version from the existing contracts.

This importer is a bootstrap adapter, not a runtime fallback. It never binds
credentials, claims cloud capability evidence, activates a version or seeds a
business-source database.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from .contracts import load_sources
from .definitions import (
    _code_cell,
    copy_projection,
    ingestion_runtime_sha256,
    part,
    validate_deployable_definition,
)
from .gold import gold_config
from .metadata import Configuration, load_configuration, metadata_digest
from .use_cases import load_case_extension, load_use_case

METADATA_NOTEBOOKS = {
    "prepare": "nb_prepare_medallion_run",
    "silver": "nb_orchestrate_silver",
    "worker": "nb_silver_worker",
    "gold": "nb_orchestrate_gold",
    "gold_worker": "nb_gold_worker",
}


def metadata_runtime_sources(
    root: Path,
    *,
    layer: str = "silver",
    use_case: str | None = None,
    case_sources: Mapping[str, str] | None = None,
) -> dict[str, str]:
    if layer not in {"silver", "gold"}:
        raise ValueError("Unsupported metadata worker layer")
    profile = load_use_case(root, use_case)
    if case_sources is not None and (
        set(case_sources) != {"source_rules", "gold_rules"}
        or any(
            not isinstance(value, str) or not value.strip()
            for value in case_sources.values()
        )
    ):
        raise ValueError(
            "Case source overrides must provide both reviewed rule modules"
        )

    def case_code(name: str) -> str:
        return (
            case_sources[name]
            if case_sources is not None
            else profile.input_path(name).read_text(encoding="utf-8")
        )

    sources = {
        "source_rules": case_code("source_rules"),
        "platform": (
            root / "deployment" / "src" / "fabric_medallion_deployer" / "fabric.py"
        ).read_text(encoding="utf-8"),
        "contracts": (
            root / "deployment" / "src" / "fabric_medallion_deployer" / "metadata.py"
        ).read_text(encoding="utf-8"),
        "rules": (root / "artefacts" / "medallion" / "rules.py").read_text(
            encoding="utf-8"
        ),
        "ingestion": (
            root / "artefacts" / "medallion" / "ingestion.py"
        ).read_text(encoding="utf-8"),
        "orchestration": (
            root / "artefacts" / "orchestrator" / "silver_orchestration.py"
        ).read_text(encoding="utf-8"),
        "layout": (
            root / "artefacts" / "orchestrator" / "layout.py"
        ).read_text(encoding="utf-8"),
    }
    if layer == "gold":
        sources["gold_rules"] = case_code("gold_rules")
        sources["gold"] = (root / "artefacts" / "gold" / "gold_runtime.py").read_text(
            encoding="utf-8"
        )
        sources["gold_control"] = (
            root / "artefacts" / "orchestrator" / "gold_orchestration.py"
        ).read_text(encoding="utf-8")
        sources["mlv"] = (
            root / "artefacts" / "orchestrator" / "mlv.py"
        ).read_text(encoding="utf-8")
    return sources


def metadata_worker_sha256(
    root: Path,
    *,
    layer: str = "silver",
    use_case: str | None = None,
    case_sources: Mapping[str, str] | None = None,
) -> str:
    return metadata_digest(
        {
            name: hashlib.sha256(code.encode("utf-8")).hexdigest()
            for name, code in metadata_runtime_sources(
                root, layer=layer, use_case=use_case, case_sources=case_sources
            ).items()
        }
    )


def _column(name: str, kind: str, *, nullable: bool = True) -> dict[str, object]:
    result: dict[str, object] = {
        "name": name,
        "data_type": kind,
        "nullable": nullable,
    }
    if kind == "decimal":
        result.update(precision=28, scale=4)
    return result


def bootstrap_configuration(
    root: Path, version_id: str | None = None, use_case: str | None = None
) -> Configuration:
    profile = load_use_case(root, use_case)
    if profile.adapter == "snapshot_bootstrap":
        from .spec_compiler import validate_case_documents

        validate_case_documents(root, profile.case_id)
    names = profile.notebook_names()
    if profile.adapter == "compiled":
        from .spec_compiler import validate_compilation_receipt

        # Verify the original compilation before parsing or refreshing anything.
        # Re-hashing current worker code must never legitimize stale business inputs.
        validate_compilation_receipt(root, profile.case_id)
        path = profile.input_path("compiled_metadata")
        if not path.is_file():
            raise ValueError(
                f"{profile.case_id}: missing generated metadata; compile the case first"
            )
        document = load_configuration(
            json.loads(path.read_text(encoding="utf-8"))
        ).to_document()
        if document["extensions"].get("use_case_id") != profile.case_id:
            raise ValueError("Generated metadata belongs to a different use case")
        if {worker["id"] for worker in document["workers"]} != {
            "silver-worker",
            "gold-worker",
        }:
            raise ValueError("Compiled cases must declare both reusable workers")
        contract = document["extensions"].get("gold_contract", {})
        calendar = contract.get("calendar", {})
        if (
            contract.get("target_lakehouse") != profile.resources["gold"]
            or type(calendar.get("fiscal_year_start_month")) is not int
            or not 1 <= calendar["fiscal_year_start_month"] <= 12
        ):
            raise ValueError(
                "Compiled Gold requires its declared case target and explicit fiscal calendar"
            )
        for obj in document["objects"]:
            expected = profile.resources[
                "gold" if obj["layer"] == "gold" else "bronze_silver"
            ]
            if obj["lakehouse"] != expected:
                raise ValueError(
                    f"{obj['id']}: metadata Lakehouse differs from its case resource"
                )
        for worker in document["workers"]:
            layer = "gold" if worker["id"] == "gold-worker" else "silver"
            if worker["id"] not in {"gold-worker", "silver-worker"}:
                raise ValueError(
                    "Compiled cases require the reusable Silver/Gold workers"
                )
            worker.update(
                notebook_name=names["gold_worker" if layer == "gold" else "worker"],
                lakehouse=profile.resources[
                    "gold" if layer == "gold" else "bronze_silver"
                ],
                code_sha256=metadata_worker_sha256(
                    root, layer=layer, use_case=profile.case_id
                ),
            )
        document["extensions"]["bootstrap_state"] = "draft_unbound"
        # A local rebuild is a new draft, not evidence that native capabilities passed.
        for capability in document["capabilities"]:
            capability.update(verified=False, evidence_sha256=None)
        document["version_id"] = version_id or (
            f"{profile.case_id}-compiled-"
            + metadata_digest(
                {key: value for key, value in document.items() if key != "version_id"}
            )[:24]
        )
        return load_configuration(document)
    sources = load_sources(root, profile.case_id)
    gold = gold_config(root, sources, use_case=profile.case_id)
    ingestion_hash = ingestion_runtime_sha256(root)
    gold_hash = metadata_worker_sha256(root, layer="gold", use_case=profile.case_id)
    # Legacy schema dictionaries enter through this validated JSON boundary.
    document: dict[str, Any] = {
        "schema_version": 1,
        "version_id": version_id,
        "owner": "data_engineering",
        "connections": [],
        "feeds": [],
        "objects": [],
        "dependencies": [],
        "rules": [],
        "constraints": [],
        "transformations": [],
        "workers": [
            {
                "id": "silver-worker",
                "notebook_name": names["worker"],
                "lakehouse": sources[0].lakehouse,
                "code_sha256": metadata_worker_sha256(root, use_case=profile.case_id),
            },
            {
                "id": "gold-worker",
                "notebook_name": names["gold_worker"],
                "lakehouse": sources[0].gold_lakehouse,
                "code_sha256": gold_hash,
            },
        ],
        "capabilities": [
            {"id": name, "verified": False, "evidence_sha256": None}
            for name in (
                "dynamic-fabric-sql",
                "metadata-export",
                "notebook-parameters",
                "notebook-dag",
                "gold-object-workers",
                "sql-mlv",
                "metadata-pipeline-runtime",
            )
        ],
        "extensions": {
            "use_case_id": profile.case_id,
            "bootstrap_state": "draft_unbound",
            "hash_policies": {},
            "synthetic_only": True,
            "legacy_gold_contract_hash": gold["contract_hash"],
            "gold_contract": {
                key: value
                for key, value in gold.items()
                if key not in {"sources", "runtime_sha256", "contract_hash", "mlvs"}
            },
            "blocked_gold": gold["blocked"],
        },
    }
    silver_ids: dict[tuple[str, str], str] = {}
    source_tables = {(s.code, t["name"]): t for s in sources for t in s.tables}
    for source in sources:
        connection_id = f"connection-{source.code}"
        feed_id = f"{source.code}-finance-sql"
        document["connections"].append(
            {
                "id": connection_id,
                "name": source.connection,
                "connector_type": "FabricSql",
                "authentication_type": "OAuth2",
                "fabric_connection_id": None,
                "gateway_id": None,
            }
        )
        document["feeds"].append(
            {
                "id": feed_id,
                "source_code": source.code,
                "source_instance": source.database,
                "source_type": "sql",
                "description_slug": "finance",
                "description": f"{source.code.upper()} synthetic finance source",
                "connection_id": connection_id,
                "silver_schema": source.silver_schema,
                "quarantine_schema": source.quarantine_schema,
                "authority": "authoritative",
                "enabled": True,
                "legacy_contract_hash": source.contract_hash,
                "legacy_bronze_root": source.bronze_root,
                "geometry": None,
            }
        )
        source_silver = []
        for ordinal, table in enumerate(source.tables, 1):
            bronze_id = f"{source.code}.bronze.{table['name']}"
            silver_id = f"{source.code}.silver.{table['name']}"
            silver_ids[source.code, table["name"]] = silver_id
            source_silver.append(silver_id)
            document["extensions"]["hash_policies"][silver_id] = {
                "key_columns": list(table["keys"]),
                "value_columns": [
                    c["name"]
                    for c in table["columns"]
                    if c["name"] not in table["change_hash_exclusions"]
                ],
                "encoding": "spark_json_struct_sha256_v1",
            }
            contract = deepcopy(table)
            columns = contract.pop("columns")
            for layer, identity, schema, kind in (
                ("bronze", bronze_id, None, "files"),
                ("silver", silver_id, source.silver_schema, "append_versions"),
            ):
                object_contract = deepcopy(contract)
                if layer == "bronze":
                    object_contract["source_schema"] = source.source_schema
                    object_contract["source_object"] = table["name"]
                    object_contract["projection_sql"] = ",".join(
                        copy_projection(c) for c in columns
                    )
                document["objects"].append(
                    {
                        "id": identity,
                        "feed_id": feed_id,
                        "layer": layer,
                        "name": table["name"],
                        "target_schema": schema,
                        "lakehouse": source.lakehouse,
                        "materialization": kind,
                        "enabled": True,
                        "execution_priority": ordinal,
                        "business_keys": list(table["keys"]),
                        "columns": deepcopy(columns),
                        "contract": object_contract,
                        "worker_id": "silver-worker" if layer == "silver" else None,
                        "layout": None,
                        "existing_target": layer == "silver",
                        "required_capabilities": (
                            ["notebook-parameters", "notebook-dag"]
                            if layer == "silver"
                            else ["dynamic-fabric-sql", "metadata-export"]
                        ),
                    }
                )
            document["dependencies"].append(
                {
                    "upstream_id": bronze_id,
                    "downstream_id": silver_id,
                    "condition": "succeeded",
                }
            )
            for handler in ("prepare_rows", "relationship_checks", "silver_checks"):
                document["rules"].append(
                    {
                        "id": f"{silver_id}.{handler}",
                        "object_id": silver_id,
                        "handler": f"legacy.{handler}",
                        "version": "1",
                        "severity": "error",
                        "action": "fail",
                        "parameters": {
                            "scope": "object",
                            "legacy_contract_hash": source.contract_hash,
                            "runtime_sha256": ingestion_hash,
                        },
                    }
                )
        document["rules"].append(
            {
                "id": f"{source.code}.source_business_checks",
                "object_id": source_silver[0],
                "handler": "legacy.source_business_checks",
                "version": "1",
                "severity": "error",
                "action": "fail",
                "parameters": {
                    "scope": "source",
                    "source_code": source.code,
                    "objects": source_silver,
                    "runtime_sha256": ingestion_hash,
                },
            }
        )

    gold_objects: dict[str, dict[str, Any]] = {}

    def add_gold(
        name: str,
        kind: str,
        keys: list[str],
        columns: list[dict],
        contract: dict,
        dependencies: list[str],
        transformation_kind: str = "pattern",
    ) -> None:
        identity = f"gold.{name}"
        obj = {
            "id": identity,
            "feed_id": None,
            "layer": "gold",
            "name": name,
            "target_schema": gold["schema"],
            "lakehouse": gold["target_lakehouse"],
            "materialization": kind,
            "enabled": False,
            "execution_priority": 100,
            "business_keys": keys,
            "columns": columns,
            "contract": deepcopy(contract),
            "worker_id": "gold-worker",
            "layout": None,
            "existing_target": True,
            "required_capabilities": [
                "notebook-parameters",
                "notebook-dag",
                "sql-mlv" if kind == "mlv" else "gold-object-workers",
            ],
        }
        document["objects"].append(obj)
        gold_objects[name] = obj
        definition = {"pattern": kind, "contract": deepcopy(contract)}
        if kind == "mlv":
            definition = {"mlv_query": contract["mlv_query"], "dialect": "spark_sql"}
        document["transformations"].append(
            {
                "id": f"transform.{name}",
                "object_id": identity,
                "kind": transformation_kind,
                "version": "1",
                "definition": definition,
                "definition_sha256": metadata_digest(definition),
            }
        )
        for upstream in dependencies:
            document["dependencies"].append(
                {
                    "upstream_id": upstream,
                    "downstream_id": identity,
                    "condition": "source_committed"
                    if upstream in silver_ids.values()
                    else "succeeded",
                }
            )

    for spec in gold["dimensions"]:
        table = source_tables[spec["source"], spec["table"]]
        fields = {c["name"]: c for c in table["columns"]}
        names = list(dict.fromkeys([*spec["keys"], *spec["attributes"]]))
        fields["_department_id"] = _column("_department_id", "string")
        columns = [deepcopy(fields[name]) for name in names]
        columns.extend(
            [
                _column("dimension_sk", "long", nullable=False),
                _column("durable_key", "string"),
                _column("attribute_hash", "string"),
                _column("effective_from", "timestamp"),
                _column("effective_to", "timestamp"),
                _column("is_current", "boolean"),
                _column("time_basis", "string"),
                _column("semantic_basis", "string"),
            ]
        )
        add_gold(
            spec["name"],
            "scd2_dimension",
            ["dimension_sk"],
            columns,
            spec,
            [silver_ids[spec["source"], spec["table"]]],
        )
    dimension_ids = [f"gold.{d['name']}" for d in gold["dimensions"]]
    for spec in gold["facts"]:
        table = source_tables[spec["source"], spec["table"]]
        fields = {c["name"]: c for c in table["columns"]}
        names = list(
            dict.fromkeys([*spec["keys"], *spec["attributes"], *spec["measures"]])
        )
        columns = [deepcopy(fields[name]) for name in names]
        columns.extend(
            [
                _column("fact_sk", "long", nullable=False),
                _column("posting_month", "int"),
                _column("event_date", "date"),
                _column("source_age_band", "string"),
                _column("supplier_business_key", "string"),
                *([_column("as_of_date", "date")] if spec["snapshot"] else []),
            ]
        )
        seen = set()
        columns = [c for c in columns if not (c["name"] in seen or seen.add(c["name"]))]
        for name in (
            "department_sk",
            "supplier_sk",
            "entity_sk",
            "employee_sk",
            "post_sk",
            "component_sk",
            *[d["name"] + "_sk" for d in gold["dimensions"][:8]],
        ):
            columns.append(_column(name, "long"))
        for column in columns:
            if column["name"] in spec["measure_types"]:
                kind = spec["measure_types"][column["name"]]
                column.update(data_type="double" if kind == "double" else "decimal")
                if kind != "double":
                    column.update(precision=28, scale=4)
        add_gold(
            spec["name"],
            "snapshot_fact" if spec["snapshot"] else "atomic_fact",
            ["fact_sk"],
            columns,
            spec,
            [silver_ids[spec["source"], spec["table"]], *dimension_ids],
        )
    calendar_contract = gold.get("calendar")
    if calendar_contract is None:
        calendar_contract = load_case_extension(
            profile, "gold_rules"
        ).legacy_calendar_contract()
    for name in ("dim_date", "dim_fiscal_period"):
        columns = [
            _column("calendar_year", "int"),
            _column("calendar_month", "int"),
            _column("fiscal_period_sk", "int"),
        ]
        if name == "dim_date":
            columns.extend(
                [_column("date_sk", "int"), _column("calendar_date", "date")]
            )
        else:
            columns.extend(
                [
                    _column("fiscal_year_start", "int"),
                    _column("fiscal_month", "int"),
                    _column("calendar_basis", "string"),
                ]
            )
        add_gold(
            name,
            "calendar",
            ["date_sk" if name == "dim_date" else "fiscal_period_sk"],
            columns,
            {
                "pattern": "calendar_frames",
                "time_basis": calendar_contract.get(
                    "basis", "DECLARED_FISCAL_CALENDAR"
                ),
            },
            [f"gold.{f['name']}" for f in gold["facts"]],
        )
    for name, statement in gold["mlvs"].items():
        match = re.search(r"\nFROM [a-z0-9_]+\.[a-z0-9_]+\.([a-z0-9_]+)\n", statement)
        if match is None:
            raise ValueError(
                f"{name}: bootstrap MLV does not match the known compiler shape"
            )
        fact = gold_objects[match[1]]
        groups = statement.split("\nGROUP BY ", 1)[1].split(", ")
        fields = {c["name"]: c for c in fact["columns"]}
        columns = [_column("data_scope", "string")]
        columns.extend(deepcopy(fields[group]) for group in groups)
        measures = re.findall(r"SUM\(([a-zA-Z0-9_]+)\)", statement)
        for measure in measures:
            column = deepcopy(fields[measure])
            if column["data_type"] == "decimal":
                column["precision"] = 38
            columns.append(column)
        columns.append(_column("source_row_count", "long"))
        add_gold(
            name,
            "mlv",
            [],
            columns,
            {"mlv_query": statement, "grain": groups, "synthetic_only": True},
            [fact["id"]],
            "sql",
        )
    if version_id is None:
        identity = metadata_digest(
            {key: value for key, value in document.items() if key != "version_id"}
        )
        document["version_id"] = f"{profile.case_id}-bootstrap-{identity[:24]}"
    return load_configuration(document)


def orchestrator_database_definition(
    root: Path, configuration: Configuration | None = None
) -> dict[str, Any]:
    config = configuration or bootstrap_configuration(root)
    directory = root / "artefacts" / "orchestrator"
    seed = (
        "EXEC cfg.usp_register_version\n"
        f"    @version_id='{config.version_id.replace(chr(39), chr(39) * 2)}',\n"
        f"    @definition_json=N'{config.definition_json.replace(chr(39), chr(39) * 2)}',\n"
        f"    @definition_sha256='{config.definition_sha256}';\nGO\n"
    )
    project = """<Project Sdk="Microsoft.Build.Sql/1.0.0">
  <PropertyGroup>
    <Name>db_orchestrator</Name>
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
            part(
                "Deploy.sql", ":r .\\Schema.sql\n:r .\\Procedures.sql\n:r .\\Seed.sql\n"
            ),
            part("Schema.sql", (directory / "schema.sql").read_text(encoding="utf-8")),
            part(
                "Procedures.sql",
                (directory / "procedures.sql").read_text(encoding="utf-8"),
            ),
            part("Seed.sql", seed),
        ],
    }
    validate_deployable_definition(definition)
    return definition


def metadata_notebook_document(
    root: Path, step: str, use_case: str | None = None
) -> dict[str, Any]:
    profile = load_use_case(root, use_case)
    names = profile.notebook_names()
    if step not in METADATA_NOTEBOOKS:
        raise ValueError("Unsupported generic medallion notebook step")
    is_gold = step in {"gold", "gold_worker"}
    layer = "gold" if is_gold else "silver"
    sources = metadata_runtime_sources(root, layer=layer, use_case=profile.case_id)
    parameters = (
        'run_id = ""\nversion_id = ""\nmanifest_path = ""\nsnapshot_sha256 = ""\n'
        'dispatch_path = ""\ndispatch_sha256 = ""\ncontext_path = ""\ncontext_sha256 = ""\n'
        f'task_id = ""\nworker_id = "{layer}-worker"\nconcurrency = 2\n'
        'operation = "prepare"\nreconcile_observed_utc = ""\n'
        'callback_path = ""\ncallback_sha256 = ""\n'
        'file_arrival_path = ""\nfile_arrival_sha256 = ""\n'
    )
    if is_gold:
        parameters += (
            'source_workspace_id = ""\nsource_lakehouse_id = ""\n'
            'gold_preparation_json = ""\n'
            'gold_reconciliation_path = ""\ngold_reconciliation_sha256 = ""\n'
        )
    modules = (
        "import hashlib, json, sys, types\n"
        f"RUNTIME_SOURCES = {sources!r}\n"
        f"WORKER_CODE_SHA256 = {metadata_worker_sha256(root, layer=layer, use_case=profile.case_id)!r}\n"
        f"CASE_ID = {profile.case_id!r}\n"
        "def load_embedded(name, source, injected=None):\n"
        "    identity = 'accelerator_' + name + '_' + hashlib.sha256((CASE_ID + WORKER_CODE_SHA256 + run_id + task_id).encode()).hexdigest()[:16]\n"
        "    module = types.ModuleType(identity)\n"
        "    sys.modules[identity] = module\n"
        "    if injected:\n"
        "        module.__dict__.update(injected)\n"
        "    exec(compile(source, '<packaged-' + name + '>', 'exec'), module.__dict__)\n"
        "    return module\n"
        "META = load_embedded('contracts', RUNTIME_SOURCES['contracts'])\n"
        "PLATFORM = load_embedded('platform', RUNTIME_SOURCES['platform'])\n"
        "CASE_SOURCE = load_embedded('source_rules', RUNTIME_SOURCES['source_rules'])\n"
        "RULES = load_embedded('rules', RUNTIME_SOURCES['rules'], {'_CASE_RULES': CASE_SOURCE})\n"
        "LAYOUT = load_embedded('layout', RUNTIME_SOURCES['layout'])\n"
        "INGESTION = load_embedded('ingestion', RUNTIME_SOURCES['ingestion'], "
        "{'_MEDALLION_RULE_HANDLERS': RULES._MEDALLION_RULE_HANDLERS, '_MEDALLION_LAYOUT': LAYOUT})\n"
        "ORCHESTRATION = load_embedded('orchestration', RUNTIME_SOURCES['orchestration'])\n"
    )
    if is_gold:
        modules += (
            "CASE_GOLD = load_embedded('gold_rules', RUNTIME_SOURCES['gold_rules'])\n"
            "MLV = load_embedded('mlv', RUNTIME_SOURCES['mlv'])\n"
            "GOLD = load_embedded('gold', RUNTIME_SOURCES['gold'], {'_CASE_GOLD': CASE_GOLD, '_MEDALLION_LAYOUT': LAYOUT, '_MEDALLION_MLV': MLV, '_MEDALLION_RULE_HANDLERS': RULES._MEDALLION_RULE_HANDLERS})\n"
            "GOLD_CONTROL = load_embedded('gold_control', RUNTIME_SOURCES['gold_control'])\n"
        )
    read = (
        "runtime_context = notebookutils.runtime.context\n"
        "lakehouse_name = runtime_context['defaultLakehouseName']\n"
        f"if lakehouse_name != {profile.resources['gold' if is_gold else 'bronze_silver']!r}:\n"
        "    raise RuntimeError('Worker default Lakehouse differs from its selected case resource')\n"
        "manifest = ORCHESTRATION.read_run_manifest(\n"
        "    notebookutils.fs, manifest_path, run_id=run_id, version_id=version_id,\n"
        "    expected_snapshot_sha256=snapshot_sha256, load_configuration=META.load_configuration,\n"
        "    export_column='snapshot_json' if manifest_path.endswith('.csv') else None)\n"
        "if manifest.to_document()['configuration']['extensions'].get('use_case_id') != CASE_ID:\n"
        "    raise RuntimeError('Pinned manifest belongs to another use case; dispatch is blocked')\n"
        f"expected_lakehouses = {{'bronze': {profile.resources['bronze_silver']!r}, 'silver': {profile.resources['bronze_silver']!r}, 'gold': {profile.resources['gold']!r}}}\n"
        "if any(obj['lakehouse'] != expected_lakehouses[obj['layer']] for obj in manifest.to_document()['configuration'].get('objects', []) if obj['enabled']):\n"
        "    raise RuntimeError('Pinned target resources differ from the selected case deployment; rebind before dispatch')\n"
        "file_arrivals = None\n"
        "if bool(file_arrival_path) != bool(file_arrival_sha256):\n"
        "    raise RuntimeError('File arrival path and hash must be supplied together')\n"
        "if file_arrival_path:\n"
        "    file_arrivals = ORCHESTRATION.read_file_arrival_manifest(notebookutils.fs,\n"
        "        file_arrival_path, manifest, expected_sha256=file_arrival_sha256)\n"
        "file_feeds = {feed['id'] for feed in manifest.to_document()['configuration']['feeds']\n"
        "              if feed['source_type'] in ('ftp', 'sftp')}\n"
        f"if {step not in {'worker', 'gold_worker'}} and operation != 'verify_callbacks' and file_arrivals is None and any(task['layer'] == 'bronze' and task['feed_id'] in file_feeds\n"
        "        for task in manifest.to_document()['tasks']):\n"
        "    raise RuntimeError('File feeds require persisted reviewed arrival observations; native observer acceptance remains gated')\n"
    )
    if is_gold:
        read = (
            "source_fs = GOLD_CONTROL.source_filesystem(notebookutils.fs,\n"
            "    source_workspace_id, source_lakehouse_id)\n"
            + read.replace("notebookutils.fs", "source_fs")
        )
        if step == "gold_worker":
            execution = (
                read
                + "dispatch = ORCHESTRATION.read_layer_dispatch(source_fs, dispatch_path,\n"
                "    manifest, layer='gold', expected_sha256=dispatch_sha256)\n"
                "stage_path = GOLD_CONTROL.execute_gold_task(spark, source_fs, manifest, dispatch,\n"
                "    task_id=task_id, worker_id=worker_id, code_sha256=WORKER_CODE_SHA256,\n"
                "    lakehouse=lakehouse_name, preparation_json=gold_preparation_json,\n"
                "    runtime_context=runtime_context, control=ORCHESTRATION, gold=GOLD, ingestion=INGESTION)\n"
                "notebookutils.notebook.exit(json.dumps({'task_id': task_id, 'stage_path': stage_path}))\n"
            )
        else:
            execution = (
                read
                + "pipeline_dispatch = ORCHESTRATION.read_pipeline_dispatch(source_fs, dispatch_path,\n"
                "    manifest, layer='gold', expected_sha256=dispatch_sha256)\n"
                "if operation == 'prepare':\n"
                "    result = GOLD_CONTROL.run_gold_parent(spark, source_fs, manifest, pipeline_dispatch,\n"
                "        manifest_path=manifest_path, lakehouse=lakehouse_name, code_sha256=WORKER_CODE_SHA256,\n"
                "        concurrency=concurrency, runner=notebookutils.notebook, control=ORCHESTRATION,\n"
                "        gold=GOLD, ingestion=INGESTION, load_configuration=META.load_configuration,\n"
                "        file_arrivals=file_arrivals)\n"
                "elif operation == 'reconcile_gold':\n"
                "    client = PLATFORM.FabricClient(lambda: notebookutils.credentials.getToken('pbi'))\n"
                "    try:\n"
                "        result = GOLD_CONTROL.reconcile_gold_pipeline(spark, source_fs, manifest, pipeline_dispatch,\n"
                "            client=client, observed_utc=reconcile_observed_utc, lakehouse=lakehouse_name,\n"
                "            code_sha256=WORKER_CODE_SHA256, control=ORCHESTRATION, gold=GOLD, ingestion=INGESTION)\n"
                "    except PLATFORM.FabricApiError:\n"
                "        raise RuntimeError('Gold activity discovery failed; reconcile without resubmission') from None\n"
                "elif operation == 'require_gold_success':\n"
                "    result = GOLD_CONTROL.require_gold_success(source_fs, manifest, pipeline_dispatch,\n"
                "        path=gold_reconciliation_path, expected_sha256=gold_reconciliation_sha256,\n"
                "        code_sha256=WORKER_CODE_SHA256, control=ORCHESTRATION, gold=GOLD)\n"
                "else:\n"
                "    raise RuntimeError('Unsupported Gold orchestration operation')\n"
                "notebookutils.notebook.exit(json.dumps(result))\n"
            )
    elif step == "prepare":
        execution = (
            read + "payload = manifest.to_document()\n"
            "config = META.load_configuration(payload['configuration'])\n"
            "plan = META.compile_run_plan(config, run_id, payload['observed_utc'],\n"
            "    object_ids=payload['requested_objects'], lifecycle_state='APPROVED',\n"
            "    approved_definition_sha256=config.definition_sha256, require_ready=True)\n"
            "if plan.snapshot_sha256 != snapshot_sha256:\n"
            "    raise RuntimeError('Prepared metadata plan differs from its SQL snapshot')\n"
            "canonical_path = ORCHESTRATION.write_run_manifest(notebookutils.fs, plan)\n"
            "if operation == 'prepare':\n"
            "    result = {'run_id': run_id, 'manifest_path': canonical_path,\n"
            "              'layers': sorted({task['layer'] for task in payload['tasks']}),\n"
            "              'pages': list(META.copy_page_descriptors(plan))}\n"
            "elif operation == 'reconcile_bronze':\n"
            "    dispatch = ORCHESTRATION.read_pipeline_dispatch(notebookutils.fs, dispatch_path,\n"
            "        manifest, layer='bronze', expected_sha256=dispatch_sha256)\n"
            "    client = PLATFORM.FabricClient(lambda: notebookutils.credentials.getToken('pbi'))\n"
            "    try:\n"
            "        result = ORCHESTRATION.reconcile_bronze_pipeline(spark, notebookutils.fs,\n"
            "            manifest, dispatch, client=client, observed_utc=reconcile_observed_utc,\n"
            "            ingestion=INGESTION, file_arrivals=file_arrivals)\n"
            "    except PLATFORM.FabricApiError:\n"
            "        raise RuntimeError('Pipeline activity discovery failed; reconcile reserved attempts without resubmission') from None\n"
            "elif operation == 'reconcile_silver':\n"
            "    dispatch = ORCHESTRATION.read_pipeline_dispatch(notebookutils.fs, dispatch_path,\n"
            "        manifest, layer='silver', expected_sha256=dispatch_sha256)\n"
            "    client = PLATFORM.FabricClient(lambda: notebookutils.credentials.getToken('pbi'))\n"
            "    try:\n"
            "        result = ORCHESTRATION.reconcile_silver_pipeline(spark, notebookutils.fs,\n"
            "            manifest, dispatch, client=client, observed_utc=reconcile_observed_utc,\n"
            "            lakehouse=lakehouse_name, ingestion=INGESTION, load_configuration=META.load_configuration,\n"
            "            file_arrivals=file_arrivals)\n"
            "    except PLATFORM.FabricApiError:\n"
            "        raise RuntimeError('Silver activity discovery failed; reconcile reserved attempts without resubmission') from None\n"
            "elif operation == 'verify_callbacks':\n"
            "    ORCHESTRATION.verify_pipeline_callbacks(notebookutils.fs, manifest,\n"
            "        callback_path, callback_sha256)\n"
            "    result = {'run_id': run_id, 'callback_path': callback_path, 'verified': True}\n"
            "else:\n"
            "    raise RuntimeError('Unsupported metadata preparation operation')\n"
            "notebookutils.notebook.exit(json.dumps(result))\n"
        )
    elif step == "worker":
        execution = (
            read
            + "dispatch = ORCHESTRATION.read_layer_dispatch(notebookutils.fs, dispatch_path,\n"
            "    manifest, layer='silver', expected_sha256=dispatch_sha256)\n"
            "context = INGESTION.manifest_processing_context(\n"
            "    ORCHESTRATION._read_bounded(notebookutils.fs, context_path, ORCHESTRATION.MAX_MANIFEST_BYTES),\n"
            "    context_sha256, expected_lakehouse=lakehouse_name)\n"
            "pinned = manifest.to_document()\n"
            "task = next(item for item in pinned['tasks'] if item['task_id'] == task_id)\n"
            "obj = next(item for item in pinned['configuration']['objects'] if item['id'] == task['object_id'])\n"
            "feed = next(item for item in pinned['configuration']['feeds'] if item['id'] == obj['feed_id'])\n"
            "if (context['source_code'] != feed['source_code'] or context['source_feed_id'] != feed['id']\n"
            "        or context['silver_schema'] != obj['target_schema']\n"
            "        or context['quarantine_schema'] != feed['quarantine_schema']):\n"
            "    raise RuntimeError('Worker namespaces differ from the pinned object and feed')\n"
            "stage_path = ORCHESTRATION.execute_silver_task(spark, notebookutils.fs, manifest, dispatch,\n"
            "    task_id=task_id, worker_id=worker_id, context_path=context_path,\n"
            "    context_sha256=context_sha256, lakehouse=lakehouse_name,\n"
            "    code_sha256=WORKER_CODE_SHA256, ingestion=INGESTION)\n"
            "notebookutils.notebook.exit(json.dumps({'task_id': task_id, 'stage_path': stage_path}))\n"
        )
    else:
        execution = (
            read
            + "pipeline_dispatch = ORCHESTRATION.read_pipeline_dispatch(notebookutils.fs, dispatch_path,\n"
            "    manifest, layer='silver', expected_sha256=dispatch_sha256)\n"
            "dispatch = ORCHESTRATION.notebook_layer_dispatch(manifest, pipeline_dispatch)\n"
            "worker_dispatch_path = ORCHESTRATION.write_layer_dispatch(notebookutils.fs, dispatch)\n"
            "contexts = ORCHESTRATION.prepare_silver_contexts(notebookutils.fs, manifest,\n"
            "    load_configuration=META.load_configuration, file_arrivals=file_arrivals)\n"
            "ORCHESTRATION.initialize_silver_sources(spark, notebookutils.fs, contexts,\n"
            "    lakehouse=lakehouse_name, ingestion=INGESTION)\n"
            "dag = ORCHESTRATION.build_silver_dag(manifest, contexts, dispatch,\n"
            "    manifest_path=manifest_path, dispatch_path=worker_dispatch_path,\n"
            "    lakehouse=lakehouse_name, concurrency=concurrency)\n"
            "ORCHESTRATION.run_notebook_dag(notebookutils.notebook, dag, dispatch)\n"
            "result = ORCHESTRATION.finalize_silver_sources(spark, notebookutils.fs, manifest,\n"
            "    dispatch, contexts, lakehouse=lakehouse_name, ingestion=INGESTION)\n"
            "notebookutils.notebook.exit(json.dumps(result))\n"
        )
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "id": "overview",
                "metadata": {},
                "source": [
                    f"# {names[step]}\n",
                    "\nInputs are immutable run, context and prepared-attempt references.\n",
                    "\nRequires schema-enabled Fabric PySpark, Delta and notebookutils.\n",
                    "\nNo active source contract or credentials are embedded. Missing dispatch proof fails closed.\n",
                    "\nGenerate locally with deployment/build.py --metadata; deployment remains gated.\n",
                ],
            },
            _code_cell("parameters", parameters, ["parameters"]),
            _code_cell("packaged-runtime", modules, ["medallion_runtime"]),
            _code_cell("execute", execution, []),
        ],
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


def metadata_notebook_definition(
    root: Path,
    step: str,
    *,
    workspace_id: str,
    lakehouse_id: str,
    lakehouse_name: str,
    source_workspace_id: str | None = None,
    source_lakehouse_id: str | None = None,
    use_case: str | None = None,
) -> dict[str, Any]:
    profile = load_use_case(root, use_case)
    for value in (workspace_id, lakehouse_id):
        if str(uuid.UUID(value)) != value:
            raise ValueError(
                "Generic notebook bindings must be resolved canonical UUIDs"
            )
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,127}", lakehouse_name):
        raise ValueError("Generic notebook Lakehouse name is not a safe identifier")
    is_gold = step in {"gold", "gold_worker"}
    expected = profile.resources["gold" if is_gold else "bronze_silver"]
    if lakehouse_name != expected:
        raise ValueError(
            "Notebook default Lakehouse differs from its selected case resource"
        )
    if is_gold:
        for value in (source_workspace_id, source_lakehouse_id):
            try:
                valid = isinstance(value, str) and str(uuid.UUID(value)) == value
            except ValueError:
                valid = False
            if not valid:
                raise ValueError(
                    "Gold source bindings require resolved canonical UUIDs"
                )
        if (source_workspace_id, source_lakehouse_id) == (workspace_id, lakehouse_id):
            raise ValueError("Gold and source Lakehouse bindings must be distinct")
    document = metadata_notebook_document(root, step, profile.case_id)
    path = profile.generated / "notebooks" / f"{profile.notebook_names()[step]}.ipynb"
    if not path.is_file() or json.loads(path.read_text(encoding="utf-8")) != document:
        raise ValueError(
            f"Stale generated generic notebook: {path.name}; run deployment/build.py"
        )
    if is_gold:
        document["cells"].insert(
            2,
            _code_cell(
                "source-bindings",
                f"source_workspace_id = {source_workspace_id!r}\n"
                f"source_lakehouse_id = {source_lakehouse_id!r}\n",
                [],
            ),
        )
    document["metadata"]["dependencies"] = {
        "lakehouse": {
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": lakehouse_name,
            "default_lakehouse_workspace_id": workspace_id,
            "known_lakehouses": [{"id": lakehouse_id}]
            + ([{"id": source_lakehouse_id}] if is_gold else []),
        }
    }
    definition = {
        "format": "ipynb",
        "parts": [part("artifact.content.ipynb", json.dumps(document))],
    }
    validate_deployable_definition(definition)
    return definition
