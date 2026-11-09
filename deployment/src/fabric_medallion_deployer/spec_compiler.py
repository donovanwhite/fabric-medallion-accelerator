"""Compile explicit case Markdown contracts into guarded metadata drafts.

Only fenced JSON contract blocks are machine inputs. Prose and Docling evidence
remain review context; neither supplies executable code or invented semantics.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import re
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

from .contracts import RESERVED_COLUMNS, load_sources
from .metadata import (
    canonical_json,
    load_configuration,
    metadata_digest,
    sql_copy_projection,
)
from .use_cases import UseCaseProfile, load_use_case

_TOKEN = re.compile(r"[a-z][a-z0-9_]{0,62}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_SECRET_KEYS = {
    "password",
    "token",
    "access_token",
    "client_secret",
    "private_key",
    "connection_string",
}
_FORBIDDEN = {
    "overrides",
    "baseline_overrides",
    "disable_baseline",
    "policy_path",
    "python",
    "exec",
    "eval",
    "sql_expression",
}


class SpecificationError(ValueError):
    """A missing business decision or unsupported contract blocks generation."""


def _validate_rule_profile(root: Path, rules: list[dict], columns: set[str]) -> None:
    path = root / "artefacts" / "medallion" / "rules.py"
    specification = importlib.util.spec_from_file_location(
        "accelerator_compiler_rule_schema", path
    )
    if specification is None or specification.loader is None:
        raise SpecificationError("Shared rule schema validator is unavailable")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    module.validate_rule_specs(
        [
            {key: value for key, value in rule.items() if key != "object_id"}
            for rule in rules
        ],
        scope="object",
    )
    for rule in rules:
        if rule["handler"].startswith("legacy."):
            continue
        parameters = rule["parameters"]
        names = (
            parameters["columns"]
            if rule["handler"] in {"required_columns", "business_key_unique"}
            else (
                [parameters["left"], parameters["right"]]
                if rule["handler"] == "equals_columns"
                else [parameters["column"]]
            )
        )
        if not set(names) <= columns:
            raise SpecificationError("DQ rules must reference declared source columns")


def _record(value: object, required: set[str], optional: set[str], label: str) -> dict:
    if (
        not isinstance(value, dict)
        or not required <= value.keys()
        or value.keys() - required - optional
    ):
        raise SpecificationError(
            f"{label}: fields must be {sorted(required)} with optional {sorted(optional)}"
        )
    return value


def _token(value: object, label: str, *, column: bool = False) -> str:
    if not isinstance(value, str) or not (_IDENTIFIER if column else _TOKEN).fullmatch(
        value
    ):
        raise SpecificationError(f"{label}: an explicit safe identifier is required")
    return value


def _names(value: object, label: str, *, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise SpecificationError(
            f"{label}: an explicit ordered column list is required"
        )
    result = [_token(name, label, column=True) for name in value]
    if len(set(result)) != len(result):
        raise SpecificationError(f"{label}: duplicate columns are not allowed")
    return result


def _scan(value: object, location: str = "contract") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str) or key.lower() in _SECRET_KEYS | _FORBIDDEN:
                raise SpecificationError(
                    f"{location}: forbidden credential, executable expression or baseline override field"
                )
            _scan(child, location + "." + key)
    elif isinstance(value, list):
        for child in value:
            _scan(child, location)
    elif isinstance(value, str):
        if not location.endswith(".values") and re.search(
            r"\b(?:TBD|TODO|REQUIRED)\b|<[^>]+>", value
        ):
            raise SpecificationError(
                f"{location}: resolve placeholders before compiling"
            )
    elif isinstance(value, float) and not math.isfinite(value):
        raise SpecificationError(f"{location}: finite values are required")


def baseline_policy(root: Path) -> dict:
    path = root / "baselines" / "requirements" / "policy.json"
    policy = json.loads(path.read_text(encoding="utf-8"))
    _record(
        policy,
        {
            "schema_version",
            "baseline_version",
            "required_rule_ids",
            "architecture",
            "compiler",
            "governing_documents",
        },
        set(),
        "baseline policy",
    )
    if policy["schema_version"] != 1 or len(set(policy["required_rule_ids"])) != len(
        policy["required_rule_ids"]
    ):
        raise SpecificationError("Unsupported or ambiguous baseline policy")
    if policy["compiler"]["native_capabilities_verified_by_generation"] is not False:
        raise SpecificationError("Generation must never certify a native capability")
    documents = policy["governing_documents"]
    if (
        not isinstance(documents, list)
        or not documents
        or len(set(documents)) != len(documents)
    ):
        raise SpecificationError(
            "Baseline governing documents must be explicit and distinct"
        )
    for relative in documents:
        path = (root / relative).resolve()
        if (
            not isinstance(relative, str)
            or not relative.startswith("baselines/")
            or ".." in Path(relative).parts
            or not path.is_relative_to((root / "baselines").resolve())
            or not path.is_file()
        ):
            raise SpecificationError(
                "A canonical baseline governing document is missing or out of scope"
            )
    return policy


def read_contract(path: Path, *, profile: UseCaseProfile, kind: str) -> dict:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n") or "title:" not in text.split("---", 2)[1]:
        raise SpecificationError(f"{path}: Markdown title/frontmatter is required")
    blocks = re.findall(
        r"^```json[ \t]*\r?\n(.*?)^```[ \t]*$", text, re.MULTILINE | re.DOTALL
    )
    if len(blocks) != 1:
        raise SpecificationError(
            f"{path}: exactly one fenced JSON contract block is required"
        )
    try:
        document = json.loads(blocks[0])
    except ValueError as error:
        raise SpecificationError(f"{path}: invalid JSON contract: {error}") from None
    _scan(document)
    common = {"schema_version", "document_type", "use_case_id", "baseline_version"}
    extra = {
        "requirements": {"purpose", "owners", "baseline_requirements", "requirements"},
        "sources": {"sources"},
        "gold": {
            "schema",
            "dimensions",
            "facts",
            "calendars",
            "aggregates",
            "semantics",
            "unavailable_measures",
        },
    }
    if profile.adapter == "snapshot_bootstrap":
        extra["sources"] = {"registry_ref"}
        extra["gold"] = {"profile_ref", "detail_ref"}
    _record(document, common | extra[kind], set(), f"{kind} document")
    if (
        document["schema_version"] != 1
        or document["document_type"] != kind
        or document["use_case_id"] != profile.case_id
    ):
        raise SpecificationError(
            f"{path}: contract identity/type does not match the selected case"
        )
    if document["baseline_version"] != profile.baseline_version:
        raise SpecificationError(f"{path}: baseline version drift")
    return document


def validate_case_documents(
    root: Path, use_case: str | None = None
) -> tuple[UseCaseProfile, dict, dict, dict, dict]:
    profile = load_use_case(root, use_case)
    policy = baseline_policy(root)
    if profile.baseline_version != policy["baseline_version"]:
        raise SpecificationError(
            "Use case must align with the current reviewed baseline version"
        )
    for path in profile.documents.values():
        if not profile.path(path).is_file():
            raise SpecificationError(
                f"{profile.case_id}: missing required case context {path}"
            )
    requirements = read_contract(
        profile.path(profile.documents["requirements"]),
        profile=profile,
        kind="requirements",
    )
    if (
        not isinstance(requirements["baseline_requirements"], list)
        or len(requirements["baseline_requirements"])
        != len(set(requirements["baseline_requirements"]))
        or set(requirements["baseline_requirements"])
        != set(policy["required_rule_ids"])
    ):
        raise SpecificationError(
            "Every baseline requirement must be inherited exactly; case overrides are forbidden"
        )
    if (
        not requirements["purpose"]
        or not requirements["owners"]
        or not requirements["requirements"]
    ):
        raise SpecificationError(
            "Purpose, ownership and measurable case requirements are required"
        )
    seen = set()
    for requirement in requirements["requirements"]:
        _record(
            requirement,
            {"id", "statement", "baseline_ids", "acceptance"},
            set(),
            "case requirement",
        )
        identity = requirement["id"]
        if (
            not isinstance(identity, str)
            or identity.startswith("BASE-")
            or identity in seen
        ):
            raise SpecificationError(
                "Case requirements need distinct case-owned IDs, not baseline redefinitions"
            )
        seen.add(identity)
        if (
            not requirement["baseline_ids"]
            or not set(requirement["baseline_ids"]) <= set(policy["required_rule_ids"])
            or not requirement["acceptance"]
        ):
            raise SpecificationError(
                "Every case requirement must map to baseline IDs and measurable acceptance"
            )
    sources = read_contract(
        profile.path(profile.documents["sources"]), profile=profile, kind="sources"
    )
    gold = read_contract(
        profile.path(profile.documents["gold"]), profile=profile, kind="gold"
    )
    if profile.adapter == "snapshot_bootstrap":
        if (
            sources["registry_ref"] != profile.inputs["source_registry"]
            or gold["profile_ref"] != profile.inputs["gold_profile"]
        ):
            raise SpecificationError(
                "Snapshot contracts must reference their declared reviewed case adapters"
            )
        if not profile.path(gold["detail_ref"]).is_file():
            raise SpecificationError("Snapshot Gold detail reference is missing")
    return profile, policy, requirements, sources, gold


def _column_type(column: dict, *, measure: bool = False) -> dict:
    kind = column["data_type"]
    result = deepcopy(column)
    result["data_type"] = {
        "varchar": "string",
        "uuid": "string",
        "bigint": "long",
        "float": "double",
        "bit": "boolean",
        "money": "decimal",
        "datetime": "timestamp",
    }.get(kind, kind)
    if measure and kind != "float":
        result.update(data_type="decimal", precision=28, scale=4)
    elif kind == "money":
        result.update(precision=19, scale=4)
    return result


def _layout(value: object, columns: list[dict], label: str) -> dict:
    row = _record(
        value,
        {"layout_strategy", "partition_columns", "cluster_by_columns"},
        set(),
        label,
    )
    strategy = row["layout_strategy"]
    if strategy not in {"partitioned", "liquid"}:
        raise SpecificationError(f"{label}: select one supported layout")
    partition = _names(row["partition_columns"], label, allow_empty=True)
    cluster = _names(row["cluster_by_columns"], label, allow_empty=True)
    if (strategy == "partitioned" and (not partition or cluster)) or (
        strategy == "liquid" and (not cluster or partition)
    ):
        raise SpecificationError(
            f"{label}: inactive layout columns conflict with the selected strategy"
        )
    if not set(partition + cluster) <= {c["name"] for c in columns}:
        raise SpecificationError(f"{label}: layout columns are not declared outputs")
    return deepcopy(row)


def _compile_documents(
    profile: UseCaseProfile,
    policy: dict,
    source_doc: dict,
    gold_doc: dict,
    worker_hashes: dict[str, str],
) -> dict:
    names = profile.notebook_names()
    resources = profile.resources
    config = {
        "schema_version": 1,
        "version_id": "compiled-content",
        "owner": profile.case_id,
        "connections": [],
        "feeds": [],
        "objects": [],
        "dependencies": [],
        "rules": [],
        "transformations": [],
        "constraints": [],
        "workers": [
            {
                "id": f"{layer}-worker",
                "notebook_name": names[
                    "worker" if layer == "silver" else "gold_worker"
                ],
                "lakehouse": resources[
                    "bronze_silver" if layer == "silver" else "gold"
                ],
                "code_sha256": worker_hashes[layer],
            }
            for layer in ("silver", "gold")
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
            "synthetic_only": True,
            "bootstrap_state": "compiled_draft_unbound",
            "baseline_version": policy["baseline_version"],
            "baseline_policy_sha256": metadata_digest(policy),
            "hash_policies": {},
        },
    }
    tables: dict[tuple[str, str], dict] = {}
    source_inputs = source_doc["sources"]
    if not isinstance(source_inputs, list) or not source_inputs:
        raise SpecificationError("At least one explicit source is required")
    for source in source_inputs:
        _record(
            source,
            {
                "code",
                "description",
                "description_slug",
                "source_instance",
                "source_schema",
                "connection_name",
                "source_type",
                "authentication_type",
                "silver_schema",
                "quarantine_schema",
                "specialist_rules",
                "tables",
            },
            {"connector_type"},
            "source",
        )
        code = _token(source["code"], "source.code")
        connector = source.get("connector_type", "FabricSql")
        if (
            source["source_type"] not in policy["compiler"]["source_types"]
            or not isinstance(connector, str)
            or connector not in policy["compiler"]["sql_connectors"]
            or source["authentication_type"]
            not in policy["compiler"]["sql_connectors"][connector]
        ):
            raise SpecificationError(
                "Unsupported managed SQL connector/authentication combination"
            )
        if source["specialist_rules"] != []:
            raise SpecificationError(
                "Specialist rules need a reviewed case extension; Markdown cannot provide executable code"
            )
        if any(feed["source_code"] == code for feed in config["feeds"]):
            raise SpecificationError("Duplicate source identity")
        feed_id, connection_id = f"{profile.case_id}.{code}", f"connection.{code}"
        config["connections"].append(
            {
                "id": connection_id,
                "name": _token(source["connection_name"], "connection"),
                "connector_type": connector,
                "authentication_type": source["authentication_type"],
                "fabric_connection_id": None,
                "gateway_id": None,
            }
        )
        config["feeds"].append(
            {
                "id": feed_id,
                "source_code": code,
                "source_instance": _token(source["source_instance"], "source instance"),
                "source_type": "sql",
                "description_slug": _token(
                    source["description_slug"], "description slug"
                ),
                "description": source["description"],
                "connection_id": connection_id,
                "silver_schema": _token(source["silver_schema"], "Silver schema"),
                "quarantine_schema": _token(
                    source["quarantine_schema"], "quarantine schema"
                ),
                "authority": "authoritative",
                "enabled": True,
                "legacy_contract_hash": None,
                "legacy_bronze_root": None,
                "geometry": None,
            }
        )
        sql_capability = (
            "dynamic-fabric-sql"
            if connector == "FabricSql"
            else "sql-server-copy-basic"
        )
        if not any(item["id"] == sql_capability for item in config["capabilities"]):
            config["capabilities"].append(
                {"id": sql_capability, "verified": False, "evidence_sha256": None}
            )
        source_silver = []
        if not isinstance(source["tables"], list) or not source["tables"]:
            raise SpecificationError(f"{code}: tables must be explicit")
        for table in source["tables"]:
            _record(
                table,
                {
                    "name",
                    "grain",
                    "keys",
                    "tracked_columns",
                    "columns",
                    "dq_rules",
                    "foreign_keys",
                    "allow_empty",
                    "layout",
                },
                set(),
                "source table",
            )
            name = _token(table["name"], "source table")
            if (code, name) in tables or not table["grain"]:
                raise SpecificationError(
                    "Source grain and unique table identity are required"
                )
            keys = _names(table["keys"], "business keys")
            tracked = _names(table["tracked_columns"], "tracked columns")
            columns = table["columns"]
            if not isinstance(columns, list) or not columns:
                raise SpecificationError(
                    "Ordered, explicitly typed source columns are required"
                )
            fields = {column["name"] for column in columns}
            if {field.casefold() for field in fields} & {
                field.casefold() for field in RESERVED_COLUMNS
            }:
                raise SpecificationError(
                    "Source columns cannot shadow shared engine/lineage fields"
                )
            if any(
                column.get("data_type")
                not in {
                    "varchar",
                    "uuid",
                    "datetime",
                    "int",
                    "bigint",
                    "money",
                    "float",
                    "bit",
                    "boolean",
                    "decimal",
                }
                for column in columns
            ):
                raise SpecificationError(
                    "Initial SQL source compilation requires a supported lossless source transport type"
                )
            if not set(keys + tracked) <= fields:
                raise SpecificationError(
                    "Business keys/hash columns must exist in the source projection"
                )
            contract = {
                "name": name,
                "grain": table["grain"],
                "keys": keys,
                "allow_empty": table["allow_empty"],
                "foreign_keys": table["foreign_keys"],
                "change_hash_exclusions": [
                    column["name"]
                    for column in columns
                    if column["name"] not in tracked
                ],
            }
            tables[code, name] = {"columns": columns, "contract": contract}
            bronze_id, silver_id = f"{code}.bronze.{name}", f"{code}.silver.{name}"
            source_silver.append(silver_id)
            for layer, identity, schema, kind in (
                ("bronze", bronze_id, None, "files"),
                ("silver", silver_id, source["silver_schema"], "append_versions"),
            ):
                details = deepcopy(contract)
                if layer == "bronze":
                    details.update(
                        source_schema=_token(
                            source["source_schema"], "source schema", column=True
                        ),
                        source_object=name,
                        projection_sql=",".join(
                            sql_copy_projection(column) for column in columns
                        ),
                    )
                config["objects"].append(
                    {
                        "id": identity,
                        "feed_id": feed_id,
                        "layer": layer,
                        "name": name,
                        "target_schema": schema,
                        "lakehouse": resources["bronze_silver"],
                        "materialization": kind,
                        "enabled": True,
                        "execution_priority": len(source_silver),
                        "business_keys": keys,
                        "columns": deepcopy(columns),
                        "contract": details,
                        "worker_id": "silver-worker" if layer == "silver" else None,
                        "layout": _layout(
                            table["layout"], columns, f"{silver_id}.layout"
                        )
                        if layer == "silver"
                        else None,
                        "existing_target": False,
                        "required_capabilities": ["notebook-parameters", "notebook-dag"]
                        if layer == "silver"
                        else [sql_capability, "metadata-export"],
                    }
                )
            config["dependencies"].append(
                {
                    "upstream_id": bronze_id,
                    "downstream_id": silver_id,
                    "condition": "succeeded",
                }
            )
            config["extensions"]["hash_policies"][silver_id] = {
                "key_columns": keys,
                "value_columns": tracked,
                "encoding": "spark_json_struct_sha256_v1",
            }
            for handler in ("prepare_rows", "relationship_checks", "silver_checks"):
                config["rules"].append(
                    {
                        "id": f"{silver_id}.{handler}",
                        "object_id": silver_id,
                        "handler": "legacy." + handler,
                        "version": "1",
                        "severity": "error",
                        "action": "fail",
                        "parameters": {"scope": "object"},
                    }
                )
            for rule in table["dq_rules"]:
                _record(
                    rule,
                    {"id", "handler", "version", "severity", "action", "parameters"},
                    set(),
                    "DQ rule",
                )
                if rule["handler"] not in policy["compiler"]["rule_handlers"]:
                    raise SpecificationError(
                        "Unsupported DQ handler; register a reviewed shared or case extension"
                    )
                config["rules"].append(deepcopy(rule) | {"object_id": silver_id})
            _validate_rule_profile(
                profile.root,
                [rule for rule in config["rules"] if rule["object_id"] == silver_id],
                fields,
            )
        config["rules"].append(
            {
                "id": f"{code}.source_business_checks",
                "object_id": source_silver[0],
                "handler": "legacy.source_business_checks",
                "version": "1",
                "severity": "error",
                "action": "fail",
                "parameters": {
                    "scope": "source",
                    "objects": source_silver,
                    "source_code": code,
                },
            }
        )
    for (code, name), table in tables.items():
        for foreign_key in table["contract"]["foreign_keys"]:
            _record(
                foreign_key,
                {"columns", "table", "target_columns"},
                set(),
                "foreign key",
            )
            parent = (code, foreign_key["table"])
            if (
                parent not in tables
                or not foreign_key["columns"]
                or len(foreign_key["columns"]) != len(foreign_key["target_columns"])
                or not set(foreign_key["columns"])
                <= {c["name"] for c in table["columns"]}
                or not set(foreign_key["target_columns"])
                <= {c["name"] for c in tables[parent]["columns"]}
            ):
                raise SpecificationError(
                    "Foreign key must reference declared source columns and parent"
                )
            if foreign_key["target_columns"] != tables[parent]["contract"]["keys"]:
                raise SpecificationError(
                    "Initial foreign-key pattern must reference the declared parent business key"
                )
            config["dependencies"].append(
                {
                    "upstream_id": f"{code}.silver.{parent[1]}",
                    "downstream_id": f"{code}.silver.{name}",
                    "condition": "succeeded",
                }
            )
    _compile_gold(config, profile, gold_doc, tables, policy)
    identity = metadata_digest(
        {key: value for key, value in config.items() if key != "version_id"}
    )
    config["version_id"] = f"{profile.case_id}-{identity[:32]}"
    load_configuration(config)
    return config


def _compile_gold(
    config: dict, profile: UseCaseProfile, document: dict, tables: dict, policy: dict
) -> None:
    schema = _token(document["schema"], "Gold schema")
    dimensions, facts = [], []
    outputs: dict[str, dict] = {}

    def add(
        name: str,
        kind: str,
        keys: list[str],
        columns: list[dict],
        spec: dict,
        parents: list[str],
        layout: object,
        *,
        transformation_kind: str = "pattern",
    ) -> None:
        _token(name, "Gold object")
        if name in outputs:
            raise SpecificationError("Gold output names must be distinct")
        identity = "gold." + name
        definition = {"pattern": kind, "contract": deepcopy(spec)}
        if kind == "mlv":
            definition = {"mlv_query": spec["mlv_query"], "dialect": "spark_sql"}
        obj = {
            "id": identity,
            "feed_id": None,
            "layer": "gold",
            "name": name,
            "target_schema": schema,
            "lakehouse": profile.resources["gold"],
            "materialization": kind,
            "enabled": False,
            "execution_priority": 100,
            "business_keys": keys,
            "columns": columns,
            "contract": deepcopy(spec),
            "worker_id": "gold-worker",
            "layout": _layout(layout, columns, identity) if kind != "mlv" else None,
            "existing_target": False,
            "required_capabilities": [
                "notebook-parameters",
                "notebook-dag",
                "sql-mlv" if kind == "mlv" else "gold-object-workers",
            ],
        }
        config["objects"].append(obj)
        outputs[name] = obj
        config["transformations"].append(
            {
                "id": "transform." + name,
                "object_id": identity,
                "kind": transformation_kind,
                "version": "1",
                "definition": definition,
                "definition_sha256": metadata_digest(definition),
            }
        )
        for parent in parents:
            config["dependencies"].append(
                {
                    "upstream_id": parent,
                    "downstream_id": identity,
                    "condition": "source_committed"
                    if ".silver." in parent
                    else "succeeded",
                }
            )

    def source_fields(spec: dict) -> dict[str, dict]:
        key = (spec["source"], spec["table"])
        if key not in tables:
            raise SpecificationError(
                "Gold input must reference a declared source table"
            )
        return {column["name"]: column for column in tables[key]["columns"]}

    for entry in document["dimensions"]:
        _record(
            entry,
            {
                "name",
                "source",
                "table",
                "grain",
                "keys",
                "attributes",
                "time_basis",
                "unknown_member",
                "layout",
            },
            set(),
            "dimension",
        )
        fields = source_fields(entry)
        keys, attributes = (
            _names(entry["keys"], "dimension keys"),
            _names(entry["attributes"], "dimension attributes", allow_empty=True),
        )
        if (
            not set(keys + attributes) <= fields.keys()
            or not entry["grain"]
            or entry["time_basis"] != "observed"
            or entry["unknown_member"] != 0
        ):
            raise SpecificationError(
                "Initial dimension pattern needs explicit source fields, observed time and unknown member 0"
            )
        spec = {
            key: deepcopy(entry[key])
            for key in ("name", "source", "table", "keys", "attributes")
        }
        spec["grain"] = entry["grain"]
        spec["source_instance"] = next(
            feed["source_instance"]
            for feed in config["feeds"]
            if feed["source_code"] == entry["source"]
        )
        dimensions.append(spec)
        columns = [
            {"name": "dimension_sk", "data_type": "long", "nullable": False},
            {"name": "durable_key", "data_type": "string", "nullable": False},
            {"name": "effective_from", "data_type": "timestamp", "nullable": True},
            {"name": "is_current", "data_type": "boolean", "nullable": False},
        ]
        columns += [
            _column_type(fields[name]) for name in dict.fromkeys(keys + attributes)
        ]
        add(
            entry["name"],
            "scd2_dimension",
            ["dimension_sk"],
            columns,
            spec,
            [f"{entry['source']}.silver.{entry['table']}"],
            entry["layout"],
        )
    for entry in document["facts"]:
        _record(
            entry,
            {
                "name",
                "source",
                "table",
                "kind",
                "grain",
                "keys",
                "date_field",
                "measures",
                "attributes",
                "period_field",
                "dimension_lookups",
                "layout",
            },
            set(),
            "fact",
        )
        fields = source_fields(entry)
        keys, attributes = (
            _names(entry["keys"], "fact keys"),
            _names(entry["attributes"], "fact attributes", allow_empty=True),
        )
        if (
            entry["kind"] not in {"atomic_fact", "snapshot_fact"}
            or not entry["grain"]
            or not set(keys + attributes + [entry["date_field"]]) <= fields.keys()
        ):
            raise SpecificationError(
                "Fact requires declared grain, source keys/date/attributes and a supported materialization"
            )
        date_column = fields[entry["date_field"]]
        if date_column.get("format") != "YYYYMMDD" and date_column["data_type"] not in {
            "date",
            "datetime",
            "timestamp",
        }:
            raise SpecificationError(
                "Fact date requires explicit YYYYMMDD/date/timestamp semantics"
            )
        measures = []
        measure_types = {}
        for measure in entry["measures"]:
            _record(measure, {"column", "unit"}, set(), "fact measure")
            name = measure["column"]
            if (
                name not in fields
                or fields[name]["data_type"]
                not in {"int", "bigint", "long", "money", "decimal", "float", "double"}
                or not measure["unit"]
            ):
                raise SpecificationError(
                    "Fact measures need declared numeric source fields and units"
                )
            measures.append(name)
            measure_types[name] = (
                "double"
                if fields[name]["data_type"] in {"float", "double"}
                else "decimal(28,4)"
            )
        if len(measures) != len(set(measures)):
            raise SpecificationError("Duplicate fact measure")
        for lookup in entry["dimension_lookups"]:
            _record(
                lookup,
                {"dimension", "source_columns", "dimension_columns", "unknown_member"},
                set(),
                "dimension lookup",
            )
            dimension = next(
                (item for item in dimensions if item["name"] == lookup["dimension"]),
                None,
            )
            if (
                dimension is None
                or lookup["unknown_member"] != 0
                or lookup["dimension_columns"] != dimension["keys"]
                or lookup["source_columns"] != dimension["keys"]
                or not set(lookup["source_columns"]) <= fields.keys()
            ):
                raise SpecificationError(
                    "Initial compiler requires explicit same-name dimension keys; aliases need a reviewed case adapter"
                )
        if {lookup["dimension"] for lookup in entry["dimension_lookups"]} != {
            dimension["name"] for dimension in dimensions
        }:
            raise SpecificationError(
                "Every dimension lookup must be declared explicitly; no inferred joins"
            )
        spec = {
            key: deepcopy(entry[key])
            for key in (
                "name",
                "source",
                "table",
                "keys",
                "date_field",
                "attributes",
                "period_field",
            )
        }
        if entry["period_field"] is not None and entry["period_field"] not in fields:
            raise SpecificationError(
                "Posting period must reference a declared source field"
            )
        spec.update(
            snapshot=entry["kind"] == "snapshot_fact",
            measures=measures,
            measure_types=measure_types,
            grain=entry["grain"],
            date_roles={},
            units={measure["column"]: measure["unit"] for measure in entry["measures"]},
            dimension_lookups=deepcopy(entry["dimension_lookups"]),
        )
        facts.append(spec)
        columns = [
            {"name": "fact_sk", "data_type": "long", "nullable": False},
            {"name": "event_date", "data_type": "date", "nullable": False},
            {"name": "posting_month", "data_type": "int", "nullable": True},
        ]
        columns += [
            {
                "name": dimension["name"].removeprefix("dim_") + "_sk",
                "data_type": "long",
                "nullable": False,
            }
            for dimension in dimensions
        ]
        columns += [_column_type(fields[name], measure=True) for name in measures]
        columns += [
            _column_type(fields[name])
            for name in attributes
            if name not in {c["name"] for c in columns}
        ]
        add(
            entry["name"],
            entry["kind"],
            ["fact_sk"],
            columns,
            spec,
            [
                f"{entry['source']}.silver.{entry['table']}",
                *["gold." + dimension["name"] for dimension in dimensions],
            ],
            entry["layout"],
        )
    calendar = _record(
        document["calendars"],
        {"enabled", "fiscal_year_start_month", "layout"},
        set(),
        "calendars",
    )
    if (
        calendar["enabled"] is not True
        or type(calendar["fiscal_year_start_month"]) is not int
        or not 1 <= calendar["fiscal_year_start_month"] <= 12
    ):
        raise SpecificationError(
            "Current runtime requires explicit date/period calendars and fiscal start month"
        )
    if not facts:
        raise SpecificationError(
            "At least one explicit fact is needed for calendar and release membership"
        )
    for name, key in (
        ("dim_date", "date_sk"),
        ("dim_fiscal_period", "fiscal_period_sk"),
    ):
        columns = [{"name": key, "data_type": "int", "nullable": False}]
        add(
            name,
            "calendar",
            [key],
            columns,
            {
                "pattern": "calendar_frames",
                "time_basis": "EXPLICIT_CASE_FISCAL_CALENDAR",
            },
            ["gold." + fact["name"] for fact in facts],
            calendar["layout"][name],
        )
    for entry in document["aggregates"]:
        _record(
            entry,
            {"name", "fact", "group_by", "sum_columns", "refresh_policy"},
            set(),
            "aggregate",
        )
        fact = outputs.get(entry["fact"])
        if fact is None or fact["materialization"] not in {
            "atomic_fact",
            "snapshot_fact",
        }:
            raise SpecificationError("MLVs must aggregate one declared curated fact")
        fields = {column["name"]: column for column in fact["columns"]}
        groups, sums = (
            _names(entry["group_by"], "MLV grain"),
            _names(entry["sum_columns"], "MLV sums", allow_empty=True),
        )
        if not set(groups + sums) <= fields.keys() or entry["refresh_policy"] not in {
            "Optimal",
            "Full",
        }:
            raise SpecificationError(
                "Aggregate columns/policy are not declared or supported"
            )
        for column in sums:
            if fields[column]["data_type"] not in {"decimal", "double", "long", "int"}:
                raise SpecificationError("SUM requires a declared numeric fact output")
            if column not in fact["contract"]["measures"]:
                raise SpecificationError(
                    "SUM cannot aggregate a key or non-measure fact attribute"
                )
        target = f"{profile.resources['gold']}.{schema}.{entry['name']}"
        source = f"{profile.resources['gold']}.{schema}.{entry['fact']}"
        query = (
            f"CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS {target}\nAS SELECT "
            "CAST('SYNTHETIC_PROVISIONAL' AS STRING) AS data_scope, "
            + ", ".join(
                [
                    *groups,
                    *[f"SUM({column}) AS {column}" for column in sums],
                    "COUNT(*) AS source_row_count",
                ]
            )
            + f"\nFROM {source}\nGROUP BY "
            + ", ".join(groups)
        )
        columns = [{"name": "data_scope", "data_type": "string", "nullable": False}] + [
            deepcopy(fields[column]) for column in groups + sums
        ]
        for column in columns:
            if column["name"] in sums and column["data_type"] == "decimal":
                column["precision"] = 38
        columns.append(
            {"name": "source_row_count", "data_type": "long", "nullable": False}
        )
        add(
            entry["name"],
            "mlv",
            [],
            columns,
            {
                "mlv_query": query,
                "grain": groups,
                "synthetic_only": True,
                "refresh_policy": entry["refresh_policy"],
            },
            ["gold." + entry["fact"]],
            None,
            transformation_kind="sql",
        )
    semantics = _record(document["semantics"], {"measures"}, set(), "semantics")
    if not isinstance(semantics["measures"], list):
        raise SpecificationError("Semantic measures must be an explicit list")
    for measure in semantics["measures"]:
        _record(
            measure,
            {"name", "fact", "column", "aggregation", "unit", "description"},
            set(),
            "semantic measure",
        )
        fact = outputs.get(measure["fact"])
        if (
            fact is None
            or measure["aggregation"] not in policy["compiler"]["semantic_aggregations"]
            or measure["column"] not in {c["name"] for c in fact["columns"]}
            or not measure["unit"]
            or not measure["description"]
        ):
            raise SpecificationError(
                "Semantic measures require explicit available fact outputs, aggregation and units"
            )
        if measure["aggregation"] == "sum" and measure["column"] not in fact[
            "contract"
        ].get("measures", []):
            raise SpecificationError(
                "Semantic SUM must reference a declared business measure"
            )
    if not isinstance(document["unavailable_measures"], dict) or any(
        not isinstance(name, str) or not isinstance(reason, str) or not reason.strip()
        for name, reason in document["unavailable_measures"].items()
    ):
        raise SpecificationError(
            "Unavailable measures require explicit names and reasons"
        )
    base = {
        "version": 1,
        "execution_mode": "synthetic_demo",
        "target_lakehouse": profile.resources["gold"],
        "schema": schema,
        "dimensions": dimensions,
        "facts": facts,
        "blocked": document["unavailable_measures"],
        "department_mapping": [],
        "supplier_crosswalk": [],
        "calendar": {"fiscal_year_start_month": calendar["fiscal_year_start_month"]},
    }
    config["extensions"].update(
        gold_contract=base,
        legacy_gold_contract_hash=metadata_digest(base),
        semantic_contract=document["semantics"],
        blocked_gold=document["unavailable_measures"],
    )


def extension_sources() -> dict[str, str]:
    return {
        "source_rules": '"""Generated generic-only case adapter; DQ specifications are pinned metadata."""\n\n'
        "def source_business_checks(data, source):\n    return []\n\n"
        "def row_rules(table):\n    return []\n",
        "gold_rules": '"""Generated generic case adapter; no domain-specific enrichment was declared."""\n\n'
        "def _department(frame, source, config):\n    return frame\n\n"
        'def dimension_lookup_keys(frame, spec):\n    return list(spec["keys"])\n',
    }


def compile_use_case(
    root: Path, use_case: str, *, check: bool = False
) -> dict[str, Any]:
    """Validate all inputs before replacing one case's generated directory."""
    profile, policy, requirements, sources, gold = validate_case_documents(
        root, use_case
    )
    code = extension_sources()
    from .orchestrator import metadata_worker_sha256

    if profile.adapter == "compiled":
        hashes = {
            layer: metadata_worker_sha256(
                root, layer=layer, use_case=use_case, case_sources=code
            )
            for layer in ("silver", "gold")
        }
        definition = _compile_documents(profile, policy, sources, gold, hashes)
    else:
        from .orchestrator import bootstrap_configuration

        if (
            sources["registry_ref"] != profile.inputs["source_registry"]
            or gold["profile_ref"] != profile.inputs["gold_profile"]
        ):
            raise SpecificationError(
                "Snapshot Markdown must reference its explicit reviewed case registry/profile"
            )
        if not profile.path(gold["detail_ref"]).is_file():
            raise SpecificationError("Snapshot Gold detail reference is missing")
        definition = bootstrap_configuration(root, use_case=use_case).to_document()
    inputs = {
        profile.path(path).relative_to(root).as_posix(): hashlib.sha256(
            profile.path(path).read_bytes()
        ).hexdigest()
        for path in profile.documents.values()
    }
    inputs["baselines/requirements/policy.json"] = metadata_digest(policy)
    for relative in policy["governing_documents"]:
        inputs[relative] = hashlib.sha256((root / relative).read_bytes()).hexdigest()
    for key in ("source_registry", "gold_profile", "source_rules", "gold_rules"):
        if profile.inputs[key] is not None and (
            profile.adapter != "compiled" or key not in {"source_rules", "gold_rules"}
        ):
            path = profile.input_path(key)
            inputs[path.relative_to(root).as_posix()] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    if profile.adapter == "snapshot_bootstrap":
        for source in load_sources(root, use_case):
            for path in (source.schema_path, source.manifest_path):
                inputs[path.relative_to(root).as_posix()] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
        audit = profile.input_path("source_registry").with_name(
            "provenance_rebaseline.json"
        )
        if audit.is_file():
            inputs[audit.relative_to(root).as_posix()] = hashlib.sha256(
                audit.read_bytes()
            ).hexdigest()
    files = {
        "contracts/requirements.json": canonical_json(requirements) + "\n",
        "metadata/definition.json": canonical_json(definition) + "\n",
        "contracts/source_contracts.json": canonical_json(sources) + "\n",
        "contracts/gold_contract.json": canonical_json(gold) + "\n",
    }
    if profile.adapter == "compiled":
        files.update(
            {
                "extensions/source_rules.py": code["source_rules"],
                "extensions/gold_rules.py": code["gold_rules"],
            }
        )
    for name, body in (
        (
            "sources",
            f"Declared source objects: {len([obj for obj in definition['objects'] if obj['layer'] == 'bronze'])}.",
        ),
        (
            "quality",
            f"Versioned DQ rules: {len(definition['rules'])}. Mandatory engine checks are retained; specialist rules are not inferred.",
        ),
        (
            "gold",
            "Declared grains, temporal rules and aggregate definitions are compiled; all Gold objects remain inactive in this draft.",
        ),
        (
            "semantics",
            "Declared measures are design contracts only. No semantic model, RLS or business publication is generated or approved.",
        ),
        (
            "implementation_plan",
            "Review the generated contracts, resolve capability/binding blockers, then use shared build/deployment tooling. No automatic activation or native acceptance is implied.",
        ),
    ):
        files[f"specifications/{name}.md"] = (
            f"---\ntitle: {profile.case_id} generated {name.replace('_', ' ')}\n"
            "description: Deterministic draft context compiled from explicit case contracts\n"
            "ms.topic: reference\n---\n\n## Generated draft\n\n"
            f"Use case: `{profile.case_id}`. Baseline: `{policy['baseline_version']}`.\n\n{body}\n\n"
            "Do not hand-edit generated files. Change the reviewed case input Markdown and recompile.\n"
        )
    source_index, gold_index, lineage = [], [], []
    for obj in definition["objects"]:
        if obj["layer"] == "bronze":
            continue
        if obj["layer"] == "silver":
            source = next(
                feed["source_code"]
                for feed in definition["feeds"]
                if feed["id"] == obj["feed_id"]
            )
            name = f"sources/{source}/{obj['name']}"
            hash_policy = definition["extensions"]["hash_policies"][obj["id"]]
            contract = {
                "object_id": obj["id"],
                "target": f"{obj['lakehouse']}.{obj['target_schema']}.{obj['name']}",
                "grain": obj["contract"].get("grain"),
                "business_keys": obj["business_keys"],
                "hash_policy": hash_policy,
                "columns": [
                    {
                        key: value
                        for key, value in column.items()
                        if key
                        in {
                            "name",
                            "data_type",
                            "nullable",
                            "precision",
                            "scale",
                            "format",
                            "max_length",
                        }
                    }
                    for column in obj["columns"]
                ],
                "rules": [
                    rule
                    for rule in definition["rules"]
                    if rule["object_id"] == obj["id"]
                ],
                "layout": obj["layout"],
            }
            source_index.append(f"* [{obj['id']}]({name}.md)")
        else:
            name = f"gold/{obj['name']}"
            contract = {
                "object_id": obj["id"],
                "kind": obj["materialization"],
                "enabled": obj["enabled"],
                "contract": obj["contract"],
                "columns": obj["columns"],
                "layout": obj["layout"],
            }
            gold_index.append(f"* [{obj['name']}]({name}.md)")
        files[f"specifications/{name}.md"] = (
            f"---\ntitle: {profile.case_id} {obj['name']} generated contract\n"
            "description: Read-only context derived from reviewed case inputs\nms.topic: reference\n---\n\n"
            "## Pinned draft context\n\nGenerated, unapproved and not native acceptance. Do not hand-edit.\n\n"
            "```json\n" + json.dumps(contract, indent=2, ensure_ascii=True) + "\n```\n"
        )
    for edge in definition["dependencies"]:
        lineage.append(
            f"* `{edge['upstream_id']}` -> `{edge['downstream_id']}` (`{edge['condition']}`)"
        )
    files["specifications/sources.md"] += (
        "\n## Object contracts\n\n" + "\n".join(source_index) + "\n"
    )
    files["specifications/gold.md"] += (
        "\n## Output contracts\n\n" + "\n".join(gold_index) + "\n"
    )
    files["specifications/semantics.md"] += (
        "\n## Declared semantic contract\n\n```json\n"
        + json.dumps(
            definition["extensions"].get("semantic_contract", {}),
            indent=2,
            ensure_ascii=True,
        )
        + "\n```\n"
    )
    files["specifications/lineage.md"] = (
        f"---\ntitle: {profile.case_id} generated dependency lineage\n"
        "description: Explicit dependency edges; not service-execution evidence\nms.topic: reference\n---\n\n"
        "## Declared edges\n\n" + "\n".join(lineage) + "\n"
    )
    receipt = {
        "schema_version": 1,
        "use_case_id": profile.case_id,
        "baseline_version": policy["baseline_version"],
        "baseline_policy_sha256": metadata_digest(policy),
        "compiler_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "inputs": inputs,
        "files": {
            name: hashlib.sha256(content.encode("utf-8")).hexdigest()
            for name, content in files.items()
        },
        "metadata_definition_sha256": metadata_digest(definition),
        "status": "draft_unbound_unapproved",
    }
    files["compilation.json"] = canonical_json(receipt) + "\n"
    if check:
        for name, content in files.items():
            path = profile.generated / name
            if not path.is_file() or path.read_text(encoding="utf-8") != content:
                raise SpecificationError(
                    f"{profile.case_id}: stale generated contract {name}; recompile"
                )
        return receipt
    if all(
        (profile.generated / name).is_file()
        and (profile.generated / name).read_text(encoding="utf-8") == content
        for name, content in files.items()
    ):
        return receipt
    prior = profile.generated / "compilation.json"
    if prior.is_file():
        previous = json.loads(prior.read_text(encoding="utf-8"))
        for name, fingerprint in previous["files"].items():
            path = profile.generated / name
            if (
                path.exists()
                and hashlib.sha256(path.read_bytes()).hexdigest() != fingerprint
            ):
                raise SpecificationError(
                    f"{name}: generated output was hand-edited; preserve/review that edit before recompilation"
                )
    with tempfile.TemporaryDirectory(
        prefix=f".{profile.case_id}-compile-", dir=profile.directory
    ) as temporary:
        staged = Path(temporary) / "generated"
        staged.mkdir()
        for name, content in files.items():
            path = staged / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8", newline="\n")
        # Notebook/deployment products are rebuilt separately; never publish stale
        # products as part of a changed contract.
        backup = Path(temporary) / "previous"
        existed = profile.generated.exists()
        if existed:
            profile.generated.rename(backup)
        try:
            staged.rename(profile.generated)
        except OSError:
            if existed:
                backup.rename(profile.generated)
            raise
    return receipt


def validate_compilation_receipt(root: Path, use_case: str) -> dict[str, Any]:
    """Recompute supported contracts; hashes are drift detection, not approval."""
    profile = load_use_case(root, use_case)
    if profile.adapter != "compiled":
        raise SpecificationError(
            "Receipt verification applies to compiled case profiles"
        )
    receipt = profile.generated / "compilation.json"
    if not receipt.is_file():
        raise SpecificationError(
            f"{use_case}: missing compilation receipt; compile the case first"
        )
    return compile_use_case(root, use_case, check=True)
