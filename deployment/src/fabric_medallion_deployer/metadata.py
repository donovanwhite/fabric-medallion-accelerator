"""Pure, immutable metadata contracts shared by deployment and notebook code.

Loading a definition validates its structure; it does not authorise a
connection, approve a configuration or prove a Fabric capability.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import cast


class MetadataError(ValueError):
    """A configuration is unsafe, ambiguous or structurally incompatible."""


IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
TOKEN = re.compile(r"[a-z][a-z0-9_]{0,62}\Z")
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
LAYERS = ("bronze", "silver", "gold")
MATERIALIZATIONS = {
    "bronze": {"files"},
    "silver": {"append_versions"},
    "gold": {"scd2_dimension", "atomic_fact", "snapshot_fact", "calendar", "mlv"},
}
CONNECTORS = {"sql": {"FabricSql", "SqlServer"}, "ftp": {"Ftp"}, "sftp": {"Sftp"}}
SQL_AUTHENTICATION = {"FabricSql": {"OAuth2"}, "SqlServer": {"Basic"}}
DATA_TYPES = {
    "int",
    "bigint",
    "money",
    "decimal",
    "float",
    "bit",
    "boolean",
    "datetime",
    "uuid",
    "varchar",
    "string",
    "long",
    "double",
    "date",
    "timestamp",
    "timestamp_ntz",
}
COLUMN_FIELDS = {
    "name",
    "data_type",
    "nullable",
    "precision",
    "scale",
    "max_length",
    "min_length",
    "fixed_length",
    "minimum",
    "maximum",
    "format",
    "pattern",
    "classification",
    "source_type",
    "unit",
    "code_list",
    "constraint_origin",
    "default",
    "extraction_repair",
    "generation_policy",
    "group_id",
    "pdf_pages",
    "source_field",
    "source_group",
    "source_rules",
    "specification_gap",
}
SECRET_FIELDS = {
    "password",
    "accesstoken",
    "access_token",
    "refreshtoken",
    "refresh_token",
    "clientsecret",
    "client_secret",
    "privatekey",
    "private_key",
    "connectionstring",
    "connection_string",
}


def _reject_credentials(
    value: object, location: str = "configuration", depth: int = 0
) -> None:
    if depth > 100:
        raise MetadataError("Metadata nesting exceeds the bounded contract depth")
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise MetadataError(f"{location}: metadata keys must be strings")
            if key.casefold() in SECRET_FIELDS:
                raise MetadataError(f"{location}: credentials are prohibited")
            _reject_credentials(child, location, depth + 1)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_credentials(child, location, depth + 1)


def canonical_json(value: object) -> str:
    _reject_credentials(value)
    try:
        return json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        raise MetadataError(
            "Metadata must contain JSON values and finite numbers"
        ) from None


def metadata_digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("ascii")).hexdigest()


def _record(value: object, location: str, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
        raise MetadataError(f"{location}: expected an object")
    unknown = set(value) - fields
    if unknown:
        raise MetadataError(f"{location}: unknown fields")
    return cast(dict[str, object], value)


def _text(value: object, location: str, pattern: re.Pattern[str] = IDENTITY) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise MetadataError(f"{location}: invalid identifier")
    return value


def _description(value: object, location: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 2000:
        raise MetadataError(f"{location}: invalid description")
    return value


def _boolean(value: object, location: str) -> bool:
    if type(value) is not bool:
        raise MetadataError(f"{location}: expected a Boolean flag")
    return value


def _list(value: object, location: str) -> list[object]:
    if not isinstance(value, list):
        raise MetadataError(f"{location}: expected an ordered array")
    return value


def _names(value: object, location: str) -> tuple[str, ...]:
    result = tuple(_text(v, location, IDENTIFIER) for v in _list(value, location))
    if len({v.casefold() for v in result}) != len(result):
        raise MetadataError(f"{location}: duplicate column")
    return result


def _identities(value: object, location: str) -> tuple[str, ...]:
    result = tuple(_text(v, location) for v in _list(value, location))
    if len(set(result)) != len(result):
        raise MetadataError(f"{location}: duplicate identity")
    return result


def _hash(value: object, location: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise MetadataError(f"{location}: expected a lowercase SHA-256 hash")
    return value


def _uuid(value: object, location: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise MetadataError(f"{location}: invalid UUID")
    try:
        parsed = str(uuid.UUID(value))
    except ValueError:
        raise MetadataError(f"{location}: invalid UUID") from None
    if parsed != value:
        raise MetadataError(f"{location}: UUID must be canonical")
    return parsed


@dataclass(frozen=True)
class ConnectionSpec:
    id: str
    name: str
    connector_type: str
    authentication_type: str
    fabric_connection_id: str | None
    gateway_id: str | None


@dataclass(frozen=True)
class SqlSourceGeometry:
    workspace_id: str
    artifact_id: str


@dataclass(frozen=True)
class FeedSpec:
    id: str
    source_code: str
    source_instance: str
    source_type: str
    description_slug: str
    description: str
    connection_id: str
    silver_schema: str
    quarantine_schema: str
    authority: str
    enabled: bool
    legacy_contract_hash: str | None
    legacy_bronze_root: str | None
    geometry: SqlSourceGeometry | None

    @property
    def bronze_prefix(self) -> str:
        return (
            f"Files/bronze/{self.source_code}_"
            f"{self.description_slug}_{self.source_type}"
        )


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    ordinal: int
    data_type: str
    nullable: bool
    definition_json: str


@dataclass(frozen=True)
class LayoutSpec:
    strategy: str
    partition_columns: tuple[str, ...]
    cluster_by_columns: tuple[str, ...]


@dataclass(frozen=True)
class ObjectSpec:
    id: str
    feed_id: str | None
    layer: str
    name: str
    target_schema: str | None
    lakehouse: str
    materialization: str
    enabled: bool
    execution_priority: int
    business_keys: tuple[str, ...]
    columns: tuple[ColumnSpec, ...]
    contract_json: str
    worker_id: str | None
    layout: LayoutSpec | None
    required_capabilities: tuple[str, ...]
    existing_target: bool

    @property
    def target(self) -> tuple[str, str | None, str]:
        return self.lakehouse, self.target_schema, self.name


@dataclass(frozen=True)
class DependencySpec:
    upstream_id: str
    downstream_id: str
    condition: str


@dataclass(frozen=True)
class RuleSpec:
    id: str
    object_id: str
    handler: str
    version: str
    severity: str
    action: str
    parameters_json: str


@dataclass(frozen=True)
class TransformationSpec:
    id: str
    object_id: str
    kind: str
    version: str
    definition_json: str
    definition_sha256: str


@dataclass(frozen=True)
class ConstraintSpec:
    id: str
    object_id: str
    kind: str
    enforcement: str
    definition_json: str


@dataclass(frozen=True)
class WorkerSpec:
    id: str
    notebook_name: str
    lakehouse: str
    code_sha256: str


@dataclass(frozen=True)
class CapabilitySpec:
    id: str
    verified: bool
    evidence_sha256: str | None


@dataclass(frozen=True)
class Configuration:
    version_id: str
    owner: str
    connections: tuple[ConnectionSpec, ...]
    feeds: tuple[FeedSpec, ...]
    objects: tuple[ObjectSpec, ...]
    dependencies: tuple[DependencySpec, ...]
    rules: tuple[RuleSpec, ...]
    constraints: tuple[ConstraintSpec, ...]
    transformations: tuple[TransformationSpec, ...]
    workers: tuple[WorkerSpec, ...]
    capabilities: tuple[CapabilitySpec, ...]
    definition_json: str

    @property
    def definition_sha256(self) -> str:
        return hashlib.sha256(self.definition_json.encode("ascii")).hexdigest()

    def to_document(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.definition_json))


def sql_copy_projection(column: Mapping[str, object]) -> str:
    """Compile the allowlisted SQL-to-text encodings shared by legacy and metadata Copy."""
    name = "[" + _text(column.get("name"), "SQL projection column", IDENTIFIER) + "]"
    kind = column.get("data_type")
    if kind == "boolean":
        return (
            f"CASE WHEN {name} IS NULL THEN NULL WHEN {name}=1 "
            f"THEN N'true' ELSE N'false' END AS {name}"
        )
    if kind == "bit":
        return f"CONVERT(char(1),CONVERT(int,{name})) AS {name}"
    if kind == "datetime" and column.get("format") != "CSD_TIMESTAMP":
        return (
            f"CONVERT(char(19),{name},126)+'.'+"
            f"RIGHT('00'+CONVERT(varchar(3),DATEPART(millisecond,{name})),3) AS {name}"
        )
    if kind in {"timestamp", "timestamp_ntz"}:
        return (
            f"CONVERT(char(19),{name},126)+'.'+"
            f"RIGHT('00000'+CONVERT(varchar(6),DATEPART(microsecond,{name})),6) AS {name}"
        )
    if kind in {"money", "decimal"}:
        precision, scale = (
            (19, 4)
            if kind == "money"
            else (column.get("precision"), column.get("scale"))
        )
        if (
            type(precision) is not int
            or type(scale) is not int
            or not 0 <= scale <= precision <= 38
            or precision == 0
        ):
            raise MetadataError(
                "SQL decimal projection needs explicit precision and scale"
            )
        return f"CONVERT(varchar(50),CAST({name} AS decimal({precision},{scale}))) AS {name}"
    if column.get("format") == "HHMMSS":
        return f"RIGHT('000000'+CONVERT(varchar(6),{name}),6) AS {name}"
    if kind == "uuid":
        return f"LOWER(CONVERT(char(36),{name})) AS {name}"
    if kind == "date":
        return f"CONVERT(char(10),{name},23) AS {name}"
    if not isinstance(kind, str) or kind not in DATA_TYPES:
        raise MetadataError("Unsupported SQL Copy column type")
    return name


def _column(value: object, ordinal: int, location: str) -> ColumnSpec:
    row = _record(value, location, COLUMN_FIELDS)
    name = _text(row.get("name"), location + ".name", IDENTIFIER)
    kind = row.get("data_type")
    if not isinstance(kind, str) or kind not in DATA_TYPES:
        raise MetadataError(f"{location}: unsupported column type")
    nullable = _boolean(row.get("nullable"), location + ".nullable")
    if kind == "decimal":
        precision, scale = row.get("precision"), row.get("scale")
        if (
            type(precision) is not int
            or type(scale) is not int
            or not 0 <= scale <= precision <= 38
            or precision == 0
        ):
            raise MetadataError(f"{location}: invalid precision or scale")
    for field in ("max_length", "min_length", "fixed_length"):
        n = row.get(field)
        if n is not None and (type(n) is not int or n < 0):
            raise MetadataError(f"{location}: invalid length constraint")
    return ColumnSpec(name, ordinal, kind, nullable, canonical_json(row))


def _layout(
    value: object, columns: tuple[ColumnSpec, ...], location: str
) -> LayoutSpec:
    row = _record(
        value, location, {"layout_strategy", "partition_columns", "cluster_by_columns"}
    )
    strategy = row.get("layout_strategy")
    if not isinstance(strategy, str) or strategy not in {"partitioned", "liquid"}:
        raise MetadataError(f"{location}: select one supported layout strategy")
    partitions = _names(row.get("partition_columns"), location + ".partition_columns")
    clusters = _names(row.get("cluster_by_columns"), location + ".cluster_by_columns")
    if strategy == "partitioned" and (not partitions or clusters):
        raise MetadataError(
            f"{location}: missing selected or conflicting inactive columns"
        )
    if strategy == "liquid" and (not clusters or partitions):
        raise MetadataError(
            f"{location}: missing selected or conflicting inactive columns"
        )
    if not set(partitions + clusters) <= {c.name for c in columns}:
        raise MetadataError(f"{location}: unknown layout column")
    return LayoutSpec(cast(str, strategy), partitions, clusters)


def _object(value: object) -> ObjectSpec:
    row = _record(
        value,
        "object",
        {
            "id",
            "feed_id",
            "layer",
            "name",
            "target_schema",
            "lakehouse",
            "materialization",
            "enabled",
            "execution_priority",
            "business_keys",
            "columns",
            "contract",
            "worker_id",
            "layout",
            "required_capabilities",
            "existing_target",
        },
    )
    identity = _text(row.get("id"), "object.id")
    layer = row.get("layer")
    if not isinstance(layer, str) or layer not in LAYERS:
        raise MetadataError(f"{identity}: unknown layer")
    materialization = row.get("materialization")
    if (
        not isinstance(materialization, str)
        or materialization not in MATERIALIZATIONS[layer]
    ):
        raise MetadataError(f"{identity}: unsupported materialization for layer")
    columns = tuple(
        _column(c, ordinal, f"{identity}.columns")
        for ordinal, c in enumerate(_list(row.get("columns"), identity), 1)
    )
    names = [c.name for c in columns]
    if len({n.casefold() for n in names}) != len(names):
        raise MetadataError(f"{identity}: duplicate column")
    if not columns and materialization not in {"calendar", "mlv"}:
        raise MetadataError(f"{identity}: an explicit column contract is required")
    keys = _names(row.get("business_keys"), identity + ".business_keys")
    if not set(keys) <= set(names):
        raise MetadataError(f"{identity}: unknown business key column")
    if (
        materialization
        in {"append_versions", "scd2_dimension", "atomic_fact", "snapshot_fact"}
        and not keys
    ):
        raise MetadataError(f"{identity}: business keys are required")
    priority = row.get("execution_priority")
    if type(priority) is not int or not 0 <= priority <= 2_147_483_647:
        raise MetadataError(f"{identity}: invalid execution priority")
    schema = row.get("target_schema")
    if layer == "bronze":
        if schema is not None or row.get("layout") is not None:
            raise MetadataError(
                f"{identity}: raw files cannot have a Delta layout/schema"
            )
    else:
        _text(schema, identity + ".target_schema", TOKEN)
    existing = _boolean(row.get("existing_target"), identity + ".existing_target")
    layout = (
        _layout(row["layout"], columns, identity)
        if row.get("layout") is not None
        else None
    )
    if (
        layer != "bronze"
        and materialization != "mlv"
        and not existing
        and layout is None
    ):
        raise MetadataError(
            f"{identity}: new managed Delta targets need an explicit layout"
        )
    if materialization == "mlv" and layout is not None and layout.strategy == "liquid":
        raise MetadataError(
            f"{identity}: liquid layout is not a verified MLV capability"
        )
    feed = row.get("feed_id")
    worker = row.get("worker_id")
    contract = row.get("contract")
    if not isinstance(contract, dict):
        raise MetadataError(f"{identity}: object contract must be structured")
    return ObjectSpec(
        identity,
        _text(feed, identity + ".feed_id") if feed is not None else None,
        layer,
        _text(row.get("name"), identity + ".name", TOKEN),
        cast(str | None, schema),
        _text(row.get("lakehouse"), identity + ".lakehouse", TOKEN),
        cast(str, materialization),
        _boolean(row.get("enabled"), identity + ".enabled"),
        priority,
        keys,
        columns,
        canonical_json(contract),
        _text(worker, identity + ".worker_id") if worker is not None else None,
        layout,
        _identities(
            row.get("required_capabilities"), identity + ".required_capabilities"
        ),
        existing,
    )


def _unique(identities: tuple[str, ...], location: str) -> None:
    if len(set(identities)) != len(identities):
        raise MetadataError(f"Duplicate {location} identity")


def load_configuration(document: object) -> Configuration:
    """Validate a definition and detach it completely from caller-owned objects."""
    snapshot = canonical_json(document)
    row = _record(
        json.loads(snapshot),
        "configuration",
        {
            "schema_version",
            "version_id",
            "owner",
            "connections",
            "feeds",
            "objects",
            "dependencies",
            "rules",
            "transformations",
            "workers",
            "capabilities",
            "constraints",
            "extensions",
        },
    )
    if type(row.get("schema_version")) is not int or row["schema_version"] != 1:
        raise MetadataError("Unsupported metadata schema version")
    version = _text(row.get("version_id"), "configuration.version_id")
    owner = _text(row.get("owner"), "configuration.owner")
    if not isinstance(row.get("extensions"), dict):
        raise MetadataError("configuration.extensions: expected an object")
    connections = []
    for value in _list(row.get("connections"), "connections"):
        item = _record(
            value,
            "connection",
            {
                "id",
                "name",
                "connector_type",
                "authentication_type",
                "fabric_connection_id",
                "gateway_id",
            },
        )
        connector = item.get("connector_type")
        if not isinstance(connector, str) or connector not in {
            name for names in CONNECTORS.values() for name in names
        }:
            raise MetadataError("connection: unsupported connector type")
        auth = _text(item.get("authentication_type"), "connection.authentication_type")
        if (
            connector in SQL_AUTHENTICATION
            and auth not in SQL_AUTHENTICATION[connector]
        ):
            raise MetadataError(
                f"connection: {connector} requires supported authentication "
                f"{sorted(SQL_AUTHENTICATION[connector])}"
            )
        connections.append(
            ConnectionSpec(
                _text(item.get("id"), "connection.id"),
                _text(item.get("name"), "connection.name", TOKEN),
                cast(str, connector),
                auth,
                _uuid(
                    item.get("fabric_connection_id"), "connection.fabric_connection_id"
                ),
                _uuid(item.get("gateway_id"), "connection.gateway_id"),
            )
        )
    feeds = []
    for value in _list(row.get("feeds"), "feeds"):
        item = _record(
            value,
            "feed",
            {
                "id",
                "source_code",
                "source_instance",
                "source_type",
                "description_slug",
                "description",
                "connection_id",
                "silver_schema",
                "quarantine_schema",
                "authority",
                "enabled",
                "legacy_contract_hash",
                "legacy_bronze_root",
                "geometry",
            },
        )
        transport = item.get("source_type")
        if not isinstance(transport, str) or transport not in CONNECTORS:
            raise MetadataError("feed: unsupported source type")
        authority = item.get("authority")
        if not isinstance(authority, str) or authority not in {
            "authoritative",
            "supplemental",
        }:
            raise MetadataError("feed: explicit authority policy is required")
        source = _text(item.get("source_code"), "feed.source_code", TOKEN)
        legacy_root = item.get("legacy_bronze_root")
        if legacy_root is not None and legacy_root != f"Files/bronze/{source}":
            raise MetadataError("feed: invalid legacy Bronze scope")
        legacy_hash = item.get("legacy_contract_hash")
        geometry = None
        if item.get("geometry") is not None:
            if transport != "sql":
                raise MetadataError("Fabric SQL geometry belongs only to SQL feeds")
            value = _record(
                item["geometry"], "feed.geometry", {"workspace_id", "artifact_id"}
            )
            workspace_id = _uuid(
                value.get("workspace_id"), "feed.geometry.workspace_id"
            )
            artifact_id = _uuid(value.get("artifact_id"), "feed.geometry.artifact_id")
            if workspace_id is None or artifact_id is None:
                raise MetadataError(
                    "SQL geometry requires explicit workspace and artifact UUIDs"
                )
            geometry = SqlSourceGeometry(workspace_id, artifact_id)
        feeds.append(
            FeedSpec(
                _text(item.get("id"), "feed.id"),
                source,
                _text(item.get("source_instance"), "feed.source_instance"),
                cast(str, transport),
                _text(item.get("description_slug"), "feed.description_slug", TOKEN),
                _description(item.get("description"), "feed.description"),
                _text(item.get("connection_id"), "feed.connection_id"),
                _text(item.get("silver_schema"), "feed.silver_schema", TOKEN),
                _text(item.get("quarantine_schema"), "feed.quarantine_schema", TOKEN),
                cast(str, authority),
                _boolean(item.get("enabled"), "feed.enabled"),
                _hash(legacy_hash, "feed.legacy_contract_hash")
                if legacy_hash is not None
                else None,
                cast(str | None, legacy_root),
                geometry,
            )
        )
    workers = []
    for value in _list(row.get("workers"), "workers"):
        item = _record(
            value, "worker", {"id", "notebook_name", "lakehouse", "code_sha256"}
        )
        workers.append(
            WorkerSpec(
                _text(item.get("id"), "worker.id"),
                _text(item.get("notebook_name"), "worker.notebook_name", TOKEN),
                _text(item.get("lakehouse"), "worker.lakehouse", TOKEN),
                _hash(item.get("code_sha256"), "worker.code_sha256"),
            )
        )
    capabilities = []
    for value in _list(row.get("capabilities"), "capabilities"):
        item = _record(value, "capability", {"id", "verified", "evidence_sha256"})
        verified = _boolean(item.get("verified"), "capability.verified")
        evidence = item.get("evidence_sha256")
        if verified and evidence is None:
            raise MetadataError("capability: verified status requires evidence")
        capabilities.append(
            CapabilitySpec(
                _text(item.get("id"), "capability.id"),
                verified,
                _hash(evidence, "capability.evidence_sha256")
                if evidence is not None
                else None,
            )
        )
    objects = tuple(_object(v) for v in _list(row.get("objects"), "objects"))
    dependencies = []
    for value in _list(row.get("dependencies"), "dependencies"):
        item = _record(
            value, "dependency", {"upstream_id", "downstream_id", "condition"}
        )
        condition = item.get("condition")
        if not isinstance(condition, str) or condition not in {
            "succeeded",
            "source_committed",
            "facts_ready",
        }:
            raise MetadataError("dependency: unsupported readiness condition")
        dependencies.append(
            DependencySpec(
                _text(item.get("upstream_id"), "dependency.upstream_id"),
                _text(item.get("downstream_id"), "dependency.downstream_id"),
                cast(str, condition),
            )
        )
    rules = []
    for value in _list(row.get("rules"), "rules"):
        item = _record(
            value,
            "rule",
            {
                "id",
                "object_id",
                "handler",
                "version",
                "severity",
                "action",
                "parameters",
            },
        )
        if not isinstance(item.get("severity"), str) or item["severity"] not in {
            "error",
            "warning",
        }:
            raise MetadataError("rule: invalid severity")
        if not isinstance(item.get("action"), str) or item["action"] not in {
            "fail",
            "quarantine",
            "warn",
        }:
            raise MetadataError("rule: unsupported action")
        if not isinstance(item.get("parameters"), dict):
            raise MetadataError("rule: parameters must be structured")
        rules.append(
            RuleSpec(
                _text(item.get("id"), "rule.id"),
                _text(item.get("object_id"), "rule.object_id"),
                _text(item.get("handler"), "rule.handler"),
                _text(item.get("version"), "rule.version"),
                cast(str, item["severity"]),
                cast(str, item["action"]),
                canonical_json(item["parameters"]),
            )
        )
    transformations = []
    for value in _list(row.get("transformations"), "transformations"):
        item = _record(
            value,
            "transformation",
            {
                "id",
                "object_id",
                "kind",
                "version",
                "definition",
                "definition_sha256",
            },
        )
        if not isinstance(item.get("kind"), str) or item["kind"] not in {
            "pattern",
            "sql",
            "notebook",
        }:
            raise MetadataError("transformation: unsupported kind")
        if not isinstance(item.get("definition"), dict):
            raise MetadataError("transformation: definition must be structured")
        fingerprint = _hash(
            item.get("definition_sha256"), "transformation.definition_sha256"
        )
        if metadata_digest(item["definition"]) != fingerprint:
            raise MetadataError("transformation: definition hash mismatch")
        transformations.append(
            TransformationSpec(
                _text(item.get("id"), "transformation.id"),
                _text(item.get("object_id"), "transformation.object_id"),
                cast(str, item["kind"]),
                _text(item.get("version"), "transformation.version"),
                canonical_json(item["definition"]),
                fingerprint,
            )
        )
    constraints = []
    for value in _list(row.get("constraints", []), "constraints"):
        item = _record(
            value,
            "constraint",
            {
                "id",
                "object_id",
                "kind",
                "enforcement",
                "definition",
            },
        )
        if not isinstance(item.get("kind"), str) or item["kind"] not in {
            "check",
            "unique",
            "relationship",
            "reconciliation",
        }:
            raise MetadataError("constraint: unsupported kind")
        if not isinstance(item.get("enforcement"), str) or item["enforcement"] not in {
            "runtime",
            "engine",
        }:
            raise MetadataError("constraint: declare the enforcement mechanism")
        if not isinstance(item.get("definition"), dict):
            raise MetadataError("constraint: definition must be structured")
        constraints.append(
            ConstraintSpec(
                _text(item.get("id"), "constraint.id"),
                _text(item.get("object_id"), "constraint.object_id"),
                cast(str, item["kind"]),
                cast(str, item["enforcement"]),
                canonical_json(item["definition"]),
            )
        )
    config = Configuration(
        version,
        owner,
        tuple(connections),
        tuple(feeds),
        objects,
        tuple(dependencies),
        tuple(rules),
        tuple(constraints),
        tuple(transformations),
        tuple(workers),
        tuple(capabilities),
        snapshot,
    )
    _validate_references(config)
    _validate_hash_policies(config)
    _validate_sql_sources(config)
    _validate_file_sources(config)
    return config


def file_source_contract(value: object) -> dict[str, object]:
    row = _record(
        value,
        "file source contract",
        {
            "source_directory",
            "file_pattern",
            "recursive",
            "file_format",
            "arrival_policy",
        },
    )
    directory = row.get("source_directory")
    if (
        not isinstance(directory, str)
        or re.fullmatch(r"/?[A-Za-z0-9_./ -]{1,1999}", directory) is None
    ):
        raise MetadataError(
            "File source directory must be an explicit relative remote path"
        )
    if any(part in {"", ".", ".."} for part in directory.strip("/").split("/")):
        raise MetadataError("File source directory cannot traverse another root")
    pattern = row.get("file_pattern")
    if (
        not isinstance(pattern, str)
        or re.fullmatch(r"[A-Za-z0-9_*?][A-Za-z0-9_. *?-]{0,255}", pattern) is None
    ):
        raise MetadataError("File selection requires an explicit basename pattern")
    recursive = _boolean(row.get("recursive"), "file source recursive flag")
    if row.get("file_format") != "csv":
        raise MetadataError(
            "Only explicitly contracted CSV file ingestion is implemented"
        )
    policy = row.get("arrival_policy")
    if not isinstance(policy, dict):
        raise MetadataError(
            "File ingestion requires an explicit reviewed arrival policy"
        )
    if policy.get("kind") == "marker":
        _record(policy, "arrival marker", {"kind", "marker"})
        if (
            not isinstance(policy.get("marker"), str)
            or re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,255}", policy["marker"])
            is None
        ):
            raise MetadataError("File arrival marker must be a safe explicit basename")
    elif policy.get("kind") == "stable":
        _record(
            policy,
            "stable arrival",
            {"kind", "minimum_age_seconds", "stable_for_seconds"},
        )
        if any(
            type(policy.get(name)) is not int or not 1 <= policy[name] <= 604800
            for name in ("minimum_age_seconds", "stable_for_seconds")
        ):
            raise MetadataError("Stable arrival intervals must be positive and bounded")
    else:
        raise MetadataError("File existence alone is not an arrival policy")
    return {
        "source_directory": directory,
        "file_pattern": pattern,
        "recursive": recursive,
        "file_format": "csv",
        "arrival_policy": json.loads(canonical_json(policy)),
    }


def _validate_file_sources(config: Configuration) -> None:
    feeds = {feed.id: feed for feed in config.feeds}
    connections = {connection.id: connection for connection in config.connections}
    for obj in config.objects:
        if obj.layer != "bronze" or obj.feed_id is None:
            continue
        feed = feeds[obj.feed_id]
        if feed.source_type == "sql":
            continue
        auth = connections[feed.connection_id].authentication_type
        if auth not in (
            {"Basic", "Anonymous"} if feed.source_type == "ftp" else {"Basic"}
        ):
            raise MetadataError(
                "File connector authentication has no supported implementation"
            )
        file_source_contract(json.loads(obj.contract_json))


def _validate_sql_sources(config: Configuration) -> None:
    feeds = {feed.id: feed for feed in config.feeds}
    for obj in config.objects:
        if (
            obj.layer != "bronze"
            or obj.feed_id is None
            or feeds[obj.feed_id].source_type != "sql"
        ):
            continue
        contract = json.loads(obj.contract_json)
        _text(contract.get("source_schema"), "SQL source schema", IDENTIFIER)
        _text(contract.get("source_object"), "SQL source object", IDENTIFIER)
        projection = ",".join(
            sql_copy_projection(json.loads(c.definition_json)) for c in obj.columns
        )
        if "projection_sql" in contract and contract["projection_sql"] != projection:
            raise MetadataError(
                "SQL projection differs from the allowlisted typed column contract"
            )


def _validate_hash_policies(config: Configuration) -> None:
    extensions = cast(dict[str, object], config.to_document()["extensions"])
    policies = extensions.get("hash_policies", {})
    if not isinstance(policies, dict):
        raise MetadataError("Hash policies must be structured object contracts")
    objects = {o.id: o for o in config.objects}
    for identity, value in policies.items():
        obj = objects.get(identity)
        if obj is None or obj.layer != "silver":
            raise MetadataError("Hash policy must reference a Silver object")
        policy = _record(
            value, "hash_policy", {"key_columns", "value_columns", "encoding"}
        )
        if policy.get("encoding") != "spark_json_struct_sha256_v1":
            raise MetadataError("Unsupported hash encoding requires explicit migration")
        if (
            _names(policy.get("key_columns"), "hash_policy.key_columns")
            != obj.business_keys
        ):
            raise MetadataError("Hash key order differs from the business-key contract")
        fields = _names(policy.get("value_columns"), "hash_policy.value_columns")
        contract = cast(dict[str, object], json.loads(obj.contract_json))
        exclusions = _names(
            contract.get("change_hash_exclusions", []), "hash exclusions"
        )
        if not set(exclusions) <= {c.name for c in obj.columns}:
            raise MetadataError("Unknown observation-only hash exclusion")
        if set(fields) != {c.name for c in obj.columns} - set(exclusions):
            raise MetadataError("Hash fields differ from the tracked business columns")


def _validate_references(config: Configuration) -> None:
    for name, values in (
        ("connections", config.connections),
        ("feeds", config.feeds),
        ("objects", config.objects),
        ("rules", config.rules),
        ("constraints", config.constraints),
        ("transformations", config.transformations),
        ("workers", config.workers),
        ("capabilities", config.capabilities),
    ):
        _unique(tuple(item.id for item in values), name)
    connections = {c.id: c for c in config.connections}
    feeds = {f.id: f for f in config.feeds}
    workers = {w.id: w for w in config.workers}
    objects = {o.id: o for o in config.objects}
    capabilities = {c.id for c in config.capabilities}
    authority = set()
    paths = set()
    for feed in config.feeds:
        connection = connections.get(feed.connection_id)
        if (
            connection is None
            or connection.connector_type not in CONNECTORS[feed.source_type]
        ):
            raise MetadataError(
                f"{feed.id}: missing or incompatible connector reference"
            )
        if connection.connector_type == "SqlServer" and feed.geometry is not None:
            raise MetadataError(
                f"{feed.id}: SQL Server uses its managed connection, not Fabric item geometry"
            )
        if feed.enabled and feed.authority == "authoritative":
            key = feed.source_code, feed.source_instance
            if key in authority:
                raise MetadataError(f"{feed.id}: ambiguous source authority")
            authority.add(key)
        if feed.bronze_prefix in paths:
            raise MetadataError(f"{feed.id}: duplicate Bronze target prefix")
        paths.add(feed.bronze_prefix)
    targets = set()
    for obj in config.objects:
        if obj.feed_id is not None and obj.feed_id not in feeds:
            raise MetadataError(f"{obj.id}: missing feed reference")
        if obj.layer in {"bronze", "silver"} and obj.feed_id is None:
            raise MetadataError(f"{obj.id}: source-aligned objects require a feed")
        if (
            obj.layer == "silver"
            and obj.target_schema != feeds[cast(str, obj.feed_id)].silver_schema
        ):
            raise MetadataError(f"{obj.id}: Silver schema differs from feed contract")
        if not set(obj.required_capabilities) <= capabilities:
            raise MetadataError(f"{obj.id}: missing capability declaration")
        if obj.worker_id is not None:
            worker = workers.get(obj.worker_id)
            if worker is None or worker.lakehouse != obj.lakehouse:
                raise MetadataError(f"{obj.id}: incompatible worker/Lakehouse binding")
        if obj.layer == "silver" and obj.worker_id is None:
            raise MetadataError(f"{obj.id}: Silver worker reference is required")
        target = (
            feeds[cast(str, obj.feed_id)].bronze_prefix
            if obj.layer == "bronze"
            else obj.target_schema
        )
        key = obj.lakehouse, target, obj.name
        if key in targets:
            raise MetadataError(f"{obj.id}: duplicate target writer")
        targets.add(key)
    for item in (*config.rules, *config.constraints, *config.transformations):
        if item.object_id not in objects:
            raise MetadataError(f"{item.id}: missing object reference")
    edges = set()
    parents: dict[str, set[str]] = {o.id: set() for o in config.objects}
    for dependency in config.dependencies:
        upstream = objects.get(dependency.upstream_id)
        downstream = objects.get(dependency.downstream_id)
        if upstream is None or downstream is None:
            raise MetadataError("Missing dependency object")
        if LAYERS.index(upstream.layer) > LAYERS.index(downstream.layer):
            raise MetadataError("A dependency cannot move backwards through a layer")
        edge = dependency.upstream_id, dependency.downstream_id
        if edge in edges:
            raise MetadataError("Duplicate dependency edge")
        edges.add(edge)
        parents[dependency.downstream_id].add(dependency.upstream_id)
    remaining = set(parents)
    while remaining:
        ready = {
            identity for identity in remaining if not parents[identity] & remaining
        }
        if not ready:
            raise MetadataError("Dependency cycle detected")
        remaining -= ready


@dataclass(frozen=True)
class RunPlan:
    """Pinned configuration readiness, not proof of live permissions or service health."""

    run_id: str
    version_id: str
    definition_sha256: str
    object_ids: tuple[str, ...]
    blockers: tuple[str, ...]
    snapshot_json: str

    @property
    def can_execute(self) -> bool:
        return not self.blockers

    @property
    def snapshot_sha256(self) -> str:
        return hashlib.sha256(self.snapshot_json.encode("ascii")).hexdigest()

    def to_document(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.snapshot_json))

    def pages(
        self, page_size: int = 1000, max_bytes: int = 4 * 1024 * 1024
    ) -> tuple[tuple[dict[str, object], ...], ...]:
        if type(page_size) is not int or not 1 <= page_size <= 5000:
            raise MetadataError("Lookup page size must be between 1 and 5000")
        if type(max_bytes) is not int or not 2 <= max_bytes <= 4 * 1024 * 1024:
            raise MetadataError("Lookup byte limit must not exceed 4 MiB")
        tasks = cast(list[dict[str, object]], self.to_document()["tasks"])
        pages = []
        current: list[dict[str, object]] = []
        current_bytes = 2
        for task in tasks:
            size = len(canonical_json(task).encode("ascii"))
            if size + 2 > max_bytes:
                raise MetadataError("One task exceeds the Lookup byte limit")
            added = size + (1 if current else 0)
            if current and (
                len(current) == page_size or current_bytes + added > max_bytes
            ):
                pages.append(tuple(current))
                current = []
                current_bytes = 2
                added = size
            current.append(task)
            current_bytes += added
        if current:
            pages.append(tuple(current))
        return tuple(pages)


def compile_run_plan(
    configuration: Configuration,
    run_id: str,
    observed_utc: str,
    *,
    object_ids: list[str] | tuple[str, ...] | None = None,
    lifecycle_state: str = "DRAFT",
    approved_definition_sha256: str | None = None,
    require_ready: bool = False,
) -> RunPlan:
    """Compile a read-only preview; callers must separately validate live bindings."""
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id):
        raise MetadataError("Run identity must be a bounded path-safe token")
    if not isinstance(observed_utc, str) or not re.fullmatch(
        r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", observed_utc
    ):
        raise MetadataError("Observation timestamp must be an explicit UTC instant")
    try:
        observed = datetime.fromisoformat(observed_utc)
    except ValueError:
        raise MetadataError("Invalid observation timestamp") from None
    if not isinstance(lifecycle_state, str) or lifecycle_state not in {
        "DRAFT",
        "VALIDATED",
        "APPROVED",
        "RETIRED",
    }:
        raise MetadataError("Unknown metadata approval state")
    _boolean(require_ready, "require_ready")
    objects = {o.id: o for o in configuration.objects}
    feeds = {f.id: f for f in configuration.feeds}
    connections = {c.id: c for c in configuration.connections}
    if object_ids is not None and not isinstance(object_ids, (list, tuple)):
        raise MetadataError("Requested object identities must be an array")
    roots = (
        list(object_ids)
        if object_ids is not None
        else [o.id for o in configuration.objects if o.enabled]
    )
    if not roots or any(not isinstance(identity, str) for identity in roots):
        raise MetadataError("Select at least one valid object identity")
    if len(set(roots)) != len(roots) or not set(roots) <= objects.keys():
        raise MetadataError("Unknown or duplicate object selection")
    parents: dict[str, set[str]] = {o.id: set() for o in configuration.objects}
    conditions = {}
    for dependency in configuration.dependencies:
        parents[dependency.downstream_id].add(dependency.upstream_id)
        conditions[dependency.upstream_id, dependency.downstream_id] = (
            dependency.condition
        )
    selected = set(roots)
    pending = list(roots)
    while pending:
        identity = pending.pop()
        obj = objects[identity]
        if not obj.enabled:
            raise MetadataError(f"{identity}: required object is disabled")
        if obj.feed_id is not None:
            feed = feeds[obj.feed_id]
            if not feed.enabled:
                raise MetadataError(f"{identity}: required feed is disabled")
            if feed.authority != "authoritative":
                raise MetadataError(
                    f"{identity}: supplemental authority needs an approved merge policy"
                )
        for upstream in parents[identity]:
            if upstream not in selected:
                selected.add(upstream)
                pending.append(upstream)

    children: dict[str, list[str]] = {identity: [] for identity in selected}
    indegree = {identity: len(parents[identity] & selected) for identity in selected}
    for child in selected:
        for parent in parents[child]:
            children[parent].append(child)

    def priority(identity: str) -> tuple[int, int, str]:
        obj = objects[identity]
        return LAYERS.index(obj.layer), obj.execution_priority, identity

    ready = [priority(identity) for identity, degree in indegree.items() if not degree]
    heapq.heapify(ready)
    order = []
    while ready:
        _, _, identity = heapq.heappop(ready)
        order.append(identity)
        for child in children[identity]:
            indegree[child] -= 1
            if not indegree[child]:
                heapq.heappush(ready, priority(child))
    if len(order) != len(selected):
        raise MetadataError("Selected dependency graph contains a cycle")
    blockers = []
    if (
        lifecycle_state != "APPROVED"
        or approved_definition_sha256 != configuration.definition_sha256
    ):
        blockers.append("Metadata approval does not match the pinned definition")
    needed = {
        name for identity in order for name in objects[identity].required_capabilities
    }
    for identity in order:
        obj = objects[identity]
        if obj.layer != "bronze" or obj.feed_id is None:
            continue
        feed = feeds[obj.feed_id]
        if connections[feed.connection_id].connector_type == "SqlServer":
            needed.add("sql-server-copy-basic")
        if feed.source_type in ("ftp", "sftp"):
            authentication = connections[feed.connection_id].authentication_type
            needed.update(
                (
                    f"{feed.source_type}-copy-{authentication.lower()}",
                    f"{feed.source_type}-arrival-observer",
                )
            )
    capabilities = {c.id: c for c in configuration.capabilities}
    for name in sorted(needed):
        if name not in capabilities or not capabilities[name].verified:
            blockers.append(f"Unverified capability: {name}")
    extensions = cast(dict[str, object], configuration.to_document()["extensions"])
    policies = cast(dict[str, object], extensions.get("hash_policies", {}))
    for identity in order:
        if objects[identity].layer == "silver" and identity not in policies:
            blockers.append(f"Missing explicit business hash policy: {identity}")
        obj = objects[identity]
        if (
            obj.layer == "bronze"
            and obj.feed_id is not None
            and feeds[obj.feed_id].source_type == "sql"
            and "projection_sql" not in json.loads(obj.contract_json)
        ):
            blockers.append(f"Missing typed SQL Copy projection: {identity}")
    used_feeds = {objects[identity].feed_id for identity in order} - {None}
    for feed_id in sorted(cast(set[str], used_feeds)):
        connection = connections[feeds[feed_id].connection_id]
        if connection.fabric_connection_id is None:
            blockers.append(f"Unbound connection: {connection.name}")
        if (
            feeds[feed_id].source_type == "sql"
            and connection.connector_type == "FabricSql"
            and feeds[feed_id].geometry is None
            and any(
                objects[identity].feed_id == feed_id
                and objects[identity].layer == "bronze"
                for identity in order
            )
        ):
            blockers.append(f"Missing explicit SQL source geometry: {feed_id}")
    if require_ready and blockers:
        raise MetadataError("; ".join(blockers))
    task_ids = {
        identity: metadata_digest({"run_id": run_id, "object_id": identity})
        for identity in order
    }
    tasks = []
    for ordinal, identity in enumerate(order, 1):
        obj = objects[identity]
        feed = feeds[obj.feed_id] if obj.feed_id is not None else None
        source_batch_id = (
            metadata_digest({"run_id": run_id, "feed_id": feed.id}) if feed else None
        )
        if obj.layer == "bronze":
            if feed is None:
                raise MetadataError("Bronze task requires a pinned feed")
            folder = f"{feed.bronze_prefix}/{observed:%Y%m%d}/{source_batch_id}"
            target = (
                f"{folder}/{obj.name}.csv"
                if feed.source_type == "sql"
                else f"{folder}/{obj.name}"
            )
        else:
            target = f"{obj.lakehouse}.{obj.target_schema}.{obj.name}"
        tasks.append(
            {
                "task_id": task_ids[identity],
                "object_id": identity,
                "ordinal": ordinal,
                "layer": obj.layer,
                "materialization": obj.materialization,
                "feed_id": obj.feed_id,
                "source_batch_id": source_batch_id,
                "target_path": target,
                "target_lock_key": metadata_digest(
                    {
                        "lakehouse": obj.lakehouse,
                        "scope": feed.bronze_prefix
                        if obj.layer == "bronze" and feed
                        else obj.target_schema,
                        "name": obj.name,
                    }
                ),
                "source_lock_key": metadata_digest(
                    {
                        "lakehouse": obj.lakehouse,
                        "scope": "source",
                        "silver_schema": feed.silver_schema,
                        "source_code": feed.source_code,
                    }
                )
                if feed is not None
                else None,
                "worker_id": obj.worker_id,
                "dependencies": [
                    {
                        "task_id": task_ids[parent],
                        "condition": conditions[parent, identity],
                    }
                    for parent in sorted(parents[identity])
                ],
            }
        )
    snapshot = {
        "format_version": 1,
        "run_id": run_id,
        "version_id": configuration.version_id,
        "definition_sha256": configuration.definition_sha256,
        "observed_utc": observed.isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        ),
        "requested_objects": sorted(roots),
        "tasks": tasks,
        "can_execute": not blockers,
        "blockers": blockers,
        "configuration": configuration.to_document(),
    }
    return RunPlan(
        run_id,
        configuration.version_id,
        configuration.definition_sha256,
        tuple(order),
        tuple(blockers),
        canonical_json(snapshot),
    )


def copy_task_descriptors(plan: RunPlan) -> tuple[dict[str, object], ...]:
    """Resolve Copy inputs from one detached snapshot, never an active pointer."""
    if not plan.can_execute:
        raise MetadataError("A blocked preview cannot dispatch source Copy")
    document = plan.to_document()
    configuration = load_configuration(document["configuration"])
    objects = {obj.id: obj for obj in configuration.objects}
    feeds = {feed.id: feed for feed in configuration.feeds}
    connections = {
        connection.id: connection for connection in configuration.connections
    }
    rows = []
    for task in cast(list[dict[str, object]], document["tasks"]):
        if task["layer"] != "bronze":
            continue
        obj = objects[cast(str, task["object_id"])]
        feed = feeds[cast(str, obj.feed_id)]
        if (
            connections[feed.connection_id].connector_type == "FabricSql"
            and feed.geometry is None
        ):
            raise MetadataError("SQL Copy requires pinned source geometry")
        contract = json.loads(obj.contract_json)
        projection = ",".join(
            sql_copy_projection(json.loads(c.definition_json)) for c in obj.columns
        )
        path = str(task["target_path"]).removeprefix("Files/")
        folder, _, filename = path.rpartition("/")
        row = {
            "task_id": task["task_id"],
            "task_ordinal": task["ordinal"],
            "feed_id": task["feed_id"],
            "source_type": feed.source_type,
            "connector_type": connections[feed.connection_id].connector_type,
            "source_batch_id": task["source_batch_id"],
            "target_lock_key": task["target_lock_key"],
            "source_lock_key": task["source_lock_key"],
            "connection_id": connections[feed.connection_id].fabric_connection_id,
            "authentication_type": connections[feed.connection_id].authentication_type,
            "source_workspace_id": feed.geometry.workspace_id
            if feed.geometry is not None
            else None,
            "source_artifact_id": feed.geometry.artifact_id
            if feed.geometry is not None
            else None,
            "sql_query": f"SELECT {projection} FROM [{contract['source_schema']}].[{contract['source_object']}]"
            if feed.source_type == "sql"
            else None,
            "folder_path": folder,
            "file_name": filename,
        }
        if feed.source_type != "sql":
            geometry = file_source_contract(contract)
            row.update(
                {
                    "folder_path": path,
                    "file_name": None,
                    "source_directory": geometry["source_directory"],
                    "file_pattern": geometry["file_pattern"],
                    "recursive": geometry["recursive"],
                    "file_format": geometry["file_format"],
                    "arrival_policy_json": canonical_json(geometry["arrival_policy"]),
                }
            )
        rows.append(row)
    return tuple(rows)


def copy_page_descriptors(
    plan: RunPlan, *, page_size: int = 1000, max_bytes: int = 4 * 1024 * 1024 - 1024
) -> tuple[dict[str, int], ...]:
    """Bound the escaped SQL Lookup response without passing source arrays to pipelines."""
    if type(page_size) is not int or not 1 <= page_size <= 5000:
        raise MetadataError("Lookup page size must be between 1 and 5000")
    if type(max_bytes) is not int or not 2 <= max_bytes <= 4 * 1024 * 1024 - 1024:
        raise MetadataError("Invalid Lookup response byte bound")
    pages = []
    after = count = size = 0
    for row in copy_task_descriptors(plan):
        required = len(canonical_json(row).encode("ascii")) * 2 + 256
        if required > max_bytes:
            raise MetadataError("One Copy task exceeds the Lookup response bound")
        if count and (count == page_size or size + required > max_bytes):
            pages.append({"after_ordinal": after, "page_size": count})
            after += count
            count = size = 0
        if row["task_ordinal"] != after + count + 1:
            raise MetadataError(
                "Bronze task ordinals must be contiguous for bounded dispatch"
            )
        count += 1
        size += required
    if count:
        pages.append({"after_ordinal": after, "page_size": count})
    return tuple(pages)
