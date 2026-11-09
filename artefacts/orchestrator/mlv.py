# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Bounded, opt-in MLV lifecycle; no network or Spark work on import.

Only the shipped single-curated-fact SUM/COUNT aggregates are supported. This is
not an arbitrary SQL engine, a scheduler daemon, or business publication approval.
The parent must hold/fence the entire selected serving set (including inputs)
through registration, refresh and reconciliation. All callbacks must raise on
lost ownership. State storage must be durable and exclusive under that fence.

Native capability/approval gates are deliberately NOT changed by this module.
Public contracts are MlvDefinition, MlvLifecycle, FabricMlvJobs, SparkMlvRuntime,
LocalStateStore, NotebookStateStore and definitions_from_metadata.
Framework objects are untyped only at
the Fabric/Spark/notebookutils adapter boundary.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from functools import reduce
from pathlib import Path
from typing import Any, Protocol

MAX_STATE_BYTES = 4 * 1024 * 1024
IDENT = r"[A-Za-z_][A-Za-z0-9_]{0,127}"
TABLE = rf"{IDENT}\.{IDENT}\.{IDENT}"
ACTIVE = {"NotStarted", "InProgress"}
TERMINAL = {"Completed", "Failed", "Cancelled", "Canceled", "Deduped"}


class MlvError(RuntimeError):
    """A technical readiness boundary was not proved."""


class UnsupportedCapability(MlvError):
    """The bounded adapter has no verified implementation of a requested feature."""


class SubmissionUncertain(MlvError):
    """An intent may have reached Fabric; never automatically resubmit it."""


def _json(value: object) -> str:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(text.encode("utf-8")) > MAX_STATE_BYTES:
        raise ValueError("MLV evidence exceeds its supported bound")
    return text


def _digest(value: object) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _name(value: str, pattern: str = TABLE) -> str:
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        raise ValueError("Invalid MLV identifier")
    return value


def _text(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 256:
        raise ValueError("A bounded explicit identity/evidence reference is required")
    return value


@dataclass(frozen=True)
class AggregateQuery:
    source: str
    groups: tuple[str, ...]
    sums: tuple[str, ...]
    query: str

    @property
    def columns(self) -> tuple[str, ...]:
        return ("data_scope", *self.groups, *self.sums, "source_row_count")


def parse_mlv_sql(sql: str, target: str) -> AggregateQuery:
    """Validate ONE supported query/DDL, without executing arbitrary metadata.

    Deliberately rejects comments, joins, filters, subqueries, UDFs, time travel,
    row-drop DDL and general constraint SQL. These require a different approved
    engine adapter, never a permissive regex fallback.
    """
    _name(target)
    if not isinstance(sql, str) or not 1 <= len(sql) <= 65536:
        raise ValueError("MLV SQL must be bounded")
    sql = sql.strip().removesuffix(";").strip()
    if ";" in sql or "--" in sql or "/*" in sql or "`" in sql:
        raise UnsupportedCapability("Only one uncommented supported MLV statement")
    if sql.upper().startswith("CREATE"):
        ddl = re.fullmatch(
            rf"CREATE (?:OR REPLACE )?MATERIALIZED LAKE VIEW "
            rf"(?:IF NOT EXISTS )?({TABLE})\s*"
            r"(?:TBLPROPERTIES\s*\(\s*'delta.enableChangeDataFeed'\s*=\s*'true'\s*\)\s*)?"
            r"AS\s+(SELECT\s+.+)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if (
            ddl is None
            or ddl[1] != target
            or re.search(r"OR REPLACE.*IF NOT EXISTS", sql, re.IGNORECASE)
        ):
            raise UnsupportedCapability("Unsupported or rebound MLV DDL")
        sql = ddl[2]
    match = re.fullmatch(
        rf"SELECT\s+(.+?)\s+FROM\s+({TABLE})\s+GROUP BY\s+(.+)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None or not match[2].split(".")[-1].startswith("fact_"):
        raise UnsupportedCapability("MLVs must aggregate one declared curated fact")
    groups = tuple(part.strip() for part in match[3].split(","))
    if not groups or len(set(groups)) != len(groups):
        raise ValueError("MLV grain must be distinct explicit columns")
    for column in groups:
        _name(column, IDENT)
    projection = match[1].strip()
    marker = re.match(
        r"CAST\(\s*'SYNTHETIC_PROVISIONAL'\s+AS\s+STRING\s*\)\s+AS\s+data_scope\s*,",
        projection,
        re.IGNORECASE,
    )
    if marker is None:
        raise UnsupportedCapability("Synthetic/provisional data_scope must be retained")
    expressions = [part.strip() for part in projection[marker.end() :].split(",")]
    if expressions[: len(groups)] != list(groups):
        raise UnsupportedCapability(
            "Projection must preserve the declared grouped grain"
        )
    remainder = expressions[len(groups) :]
    if (
        not remainder
        or re.fullmatch(
            r"COUNT\(\s*\*\s*\)\s+AS\s+source_row_count", remainder[-1], re.IGNORECASE
        )
        is None
    ):
        raise UnsupportedCapability("Explicit source_row_count is required")
    sums = []
    for expression in remainder[:-1]:
        part = re.fullmatch(
            rf"SUM\(\s*({IDENT})\s*\)\s+AS\s+({IDENT})", expression, re.IGNORECASE
        )
        if part is None or part[1] != part[2]:
            raise UnsupportedCapability("Only source-preserving SUM(column) aliases")
        sums.append(part[1])
    columns = ("data_scope", *groups, *sums, "source_row_count")
    if len(set(columns)) != len(columns):
        raise ValueError("Duplicate aggregate output columns")
    query = (
        "SELECT CAST('SYNTHETIC_PROVISIONAL' AS STRING) AS data_scope, "
        + ", ".join(
            [
                *groups,
                *[f"SUM({c}) AS {c}" for c in sums],
                "COUNT(*) AS source_row_count",
            ]
        )
        + f" FROM {match[2]} GROUP BY "
        + ", ".join(groups)
    )
    return AggregateQuery(match[2], groups, tuple(sums), query)


@dataclass(frozen=True)
class MlvDefinition:
    """Pinned metadata contract; approval is external and binds ``sha256``.

    Constraint dictionaries use the existing ConstraintSpec shape. Check
    definitions contain an existing RuleSpec (required_columns, version 1,
    error/fail); unique/relationship definitions use explicit column lists.
    Unsupported rules/enforcement are blocked, not executed or downgraded.
    """

    object_id: str
    target: str
    version: str
    mlv_query: str
    inputs: tuple[str, ...]
    output_columns: tuple[tuple[str, str], ...]
    grain: tuple[str, ...]
    constraints: tuple[dict[str, Any], ...] = ()
    refresh_policy: str = "Optimal"
    configuration_sha256: str | None = None

    def parsed(self) -> AggregateQuery:
        parsed = parse_mlv_sql(self.mlv_query, self.target)
        _text(self.object_id)
        _text(self.version)
        if self.configuration_sha256 is not None:
            _name(self.configuration_sha256, r"[0-9a-f]{64}")
        if self.refresh_policy not in {"Optimal", "Full"}:
            raise UnsupportedCapability("Only documented Optimal/Full refresh policies")
        if (
            tuple(c[0] for c in self.output_columns) != parsed.columns
            or self.grain != parsed.groups
            or len(set(self.inputs)) != len(self.inputs)
            or not self.inputs
            or any(not isinstance(c[1], str) or not c[1] for c in self.output_columns)
        ):
            raise ValueError("Explicit input/output/grain contract differs from SQL")
        required = {parsed.source}
        identities = set()
        for constraint in self.constraints:
            if (
                set(constraint)
                != {"id", "object_id", "kind", "enforcement", "definition"}
                or constraint["object_id"] != self.object_id
                or constraint["id"] in identities
            ):
                raise ValueError("Invalid or duplicate metadata constraint")
            identities.add(_text(constraint["id"]))
            if constraint["id"] == "declared_grain":
                raise ValueError("declared_grain is a reserved constraint identity")
            if constraint["enforcement"] != "runtime":
                raise UnsupportedCapability(
                    "Logical constraints are not engine-enforced; engine evidence unavailable"
                )
            kind, definition = constraint["kind"], constraint["definition"]
            if kind == "check":
                if (
                    set(definition)
                    != {"id", "handler", "version", "severity", "action", "parameters"}
                    or definition["handler"] != "required_columns"
                    or definition["version"] != "1"
                    or definition["severity"] != "error"
                    or definition["action"] != "fail"
                    or set(definition["parameters"]) != {"columns"}
                ):
                    raise UnsupportedCapability(
                        "Only existing required_columns v1 error/fail row rules are supported"
                    )
                columns = definition["parameters"]["columns"]
            elif kind == "unique" and set(definition) == {"columns"}:
                columns = definition["columns"]
            elif kind == "relationship" and set(definition) == {
                "columns",
                "reference_table",
                "reference_columns",
            }:
                columns = definition["columns"]
                _name(definition["reference_table"])
                required.add(definition["reference_table"])
                parents = definition["reference_columns"]
                if not isinstance(parents, list) or len(parents) != len(columns):
                    raise ValueError("Relationship columns differ in arity")
                for column in parents:
                    _name(column, IDENT)
            else:
                raise UnsupportedCapability(
                    "Unsupported metadata constraint definition"
                )
            if (
                not isinstance(columns, list)
                or not columns
                or len(set(columns)) != len(columns)
                or not set(columns) <= set(parsed.columns)
            ):
                raise ValueError("Constraint columns are not explicit output members")
        if required != set(self.inputs):
            raise ValueError(
                "MLV inputs must exactly cover fact and relationship references"
            )
        for table in self.inputs:
            _name(table)
            if table.split(".")[0] != self.target.split(".")[0]:
                raise UnsupportedCapability(
                    "Only one owned Gold Lakehouse is supported"
                )
        return parsed

    @property
    def sha256(self) -> str:
        value = asdict(self)
        if self.configuration_sha256 is None:
            # Preserve identity for pre-converter, explicitly constructed contracts.
            value.pop("configuration_sha256")
        value["mlv_query"] = self.parsed().query
        return _digest(value)

    def ddl(self, *, replace: bool) -> str:
        operation = "CREATE OR REPLACE" if replace else "CREATE"
        guard = "" if replace else " IF NOT EXISTS"
        return (
            f"{operation} MATERIALIZED LAKE VIEW{guard} {self.target}\n"
            "TBLPROPERTIES ('delta.enableChangeDataFeed'='true')\nAS "
            + self.parsed().query
        )


def _metadata_spark_type(column: Mapping[str, Any]) -> str:
    """Translate the existing metadata's scalar aliases, never arbitrary DDL."""
    kind = column.get("data_type")
    if kind == "decimal":
        precision, scale = column.get("precision"), column.get("scale")
        if (
            type(precision) is not int
            or type(scale) is not int
            or not 0 <= scale <= precision <= 38
            or precision == 0
        ):
            raise ValueError("MLV decimal columns require valid precision and scale")
        return f"decimal({precision},{scale})"
    aliases = {"long": "bigint", "varchar": "string", "bit": "boolean"}
    if not isinstance(kind, str):
        raise TypeError("MLV columns require explicit scalar data types")
    kind = aliases.get(kind, kind)
    if kind not in {
        "int",
        "bigint",
        "double",
        "string",
        "boolean",
        "date",
        "timestamp",
        "timestamp_ntz",
    }:
        raise UnsupportedCapability("Unsupported metadata MLV scalar type")
    return kind


def _metadata_columns(obj: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    columns = obj.get("columns")
    if not isinstance(columns, list) or not columns:
        raise ValueError("MLV inputs and outputs require ordered metadata columns")
    result = []
    seen = set()
    for column in columns:
        if not isinstance(column, dict) or type(column.get("nullable")) is not bool:
            raise ValueError("Malformed MLV metadata column")
        name = _name(column.get("name"), IDENT)
        if name.lower() in seen:
            raise ValueError("Ambiguous MLV metadata column")
        seen.add(name.lower())
        result.append((name, _metadata_spark_type(column)))
    return tuple(result)


def _metadata_target(obj: Mapping[str, Any]) -> str:
    return ".".join(
        _name(obj.get(field), IDENT) for field in ("lakehouse", "target_schema", "name")
    )


def definitions_from_metadata(
    configuration_dict: Mapping[str, Any],
    *,
    enabled_only: bool = True,
) -> tuple[MlvDefinition, ...]:
    """Convert an already approved, complete pinned configuration into contracts.

    The parent MUST first verify its pinned configuration hash and approval. This
    pure converter grants neither approval nor native capability. Each returned
    definition binds the complete canonical configuration hash, its SQL
    transformation version, declared dependency membership and result schema.
    No active-pointer lookup, mutable metadata read, Spark work or cloud call
    occurs. Object/column order is retained; ``long`` becomes Spark ``bigint``.
    Disabled objects are excluded unless explicitly requested for preview.
    """
    if not isinstance(configuration_dict, Mapping) or type(enabled_only) is not bool:
        raise ValueError("A pinned configuration and Boolean enabled_only are required")
    document = json.loads(_json(dict(configuration_dict)))
    _text(document.get("version_id"))
    configuration_sha256 = _digest(document)
    for collection in ("objects", "transformations", "dependencies", "constraints"):
        if not isinstance(document.get(collection, []), list) or any(
            not isinstance(row, dict) for row in document.get(collection, [])
        ):
            raise ValueError("Malformed MLV metadata collection")
    objects = document.get("objects", [])
    by_id = {}
    for obj in objects:
        identity = _text(obj.get("id"))
        if identity in by_id:
            raise ValueError("Duplicate metadata object identity")
        by_id[identity] = obj
    constraints = document.get("constraints", [])
    transformations = document.get("transformations", [])
    dependencies = document.get("dependencies", [])
    results, targets = [], set()
    for obj in objects:
        if obj.get("materialization") != "mlv":
            continue
        if type(obj.get("enabled")) is not bool:
            raise ValueError("MLV enabled state must be explicitly Boolean")
        if enabled_only and not obj["enabled"]:
            continue
        contract = obj.get("contract")
        if (
            obj.get("layer") != "gold"
            or obj.get("layout") is not None
            or not isinstance(contract, dict)
            or not {"mlv_query", "grain", "synthetic_only"} <= set(contract)
            or not set(contract)
            <= {"mlv_query", "grain", "synthetic_only", "refresh_policy", "inputs"}
            or contract["synthetic_only"] is not True
            or not isinstance(contract["grain"], list)
        ):
            raise UnsupportedCapability(
                "Unsupported or non-synthetic MLV object contract"
            )
        target = _metadata_target(obj)
        if target in targets:
            raise ValueError("Duplicate MLV serving target")
        targets.add(target)
        parsed = parse_mlv_sql(contract["mlv_query"], target)
        matches = [t for t in transformations if t.get("object_id") == obj["id"]]
        if len(matches) != 1:
            raise ValueError("MLV requires exactly one versioned SQL transformation")
        transform = matches[0]
        _text(transform.get("id"))
        version = _text(transform.get("version"))
        expected_transform = {
            "mlv_query": contract["mlv_query"],
            "dialect": "spark_sql",
        }
        if (
            transform.get("kind") != "sql"
            or transform.get("definition") != expected_transform
            or transform.get("definition_sha256") != _digest(expected_transform)
        ):
            raise ValueError(
                "MLV transformation SQL/hash differs from the object contract"
            )
        edges = [e for e in dependencies if e.get("downstream_id") == obj["id"]]
        inputs, input_objects = [], {}
        for edge in edges:
            upstream = by_id.get(edge.get("upstream_id"))
            if (
                upstream is None
                or upstream.get("layer") != "gold"
                or edge.get("condition") != "succeeded"
            ):
                raise ValueError(
                    "MLVs require explicit succeeded Gold input dependencies"
                )
            table = _metadata_target(upstream)
            inputs.append(table)
            input_objects[table] = upstream
        source = input_objects.get(parsed.source)
        if source is None or source.get("materialization") not in {
            "atomic_fact",
            "snapshot_fact",
        }:
            raise ValueError(
                "MLV SQL source is not its declared curated fact dependency"
            )
        if "inputs" in contract and contract["inputs"] != inputs:
            raise ValueError("MLV input contract differs from declared dependencies")
        # Validate just the referenced source columns: unrelated source attributes
        # can retain source-specific metadata types without entering this aggregate.
        used = {*parsed.groups, *parsed.sums}
        source_columns = source.get("columns")
        if not isinstance(source_columns, list):
            raise TypeError("MLV fact input lacks explicit columns")
        fields = dict(
            _metadata_columns(
                {
                    "columns": [
                        c
                        for c in source_columns
                        if isinstance(c, dict) and c.get("name") in used
                    ],
                }
            )
        )
        if set(fields) != used:
            raise ValueError("MLV query references undeclared source columns")
        expected_columns = [
            ("data_scope", "string"),
            *[(c, fields[c]) for c in parsed.groups],
        ]
        for column in parsed.sums:
            kind = fields[column]
            decimal = re.fullmatch(r"decimal\((\d+),(\d+)\)", kind)
            if decimal:
                kind = f"decimal({min(38, int(decimal[1]) + 10)},{decimal[2]})"
            elif kind in {"int", "bigint"}:
                kind = "bigint"
            elif kind != "double":
                raise UnsupportedCapability(
                    "SUM requires a supported numeric fact column"
                )
            expected_columns.append((column, kind))
        expected_columns.append(("source_row_count", "bigint"))
        columns = _metadata_columns(obj)
        if columns != tuple(expected_columns):
            raise ValueError(
                "MLV ordered output schema differs from aggregate result types"
            )
        selected_constraints = [
            c for c in constraints if c.get("object_id") == obj["id"]
        ]
        rules = document.get("rules", [])
        if not isinstance(rules, list) or any(
            not isinstance(rule, dict) for rule in rules
        ):
            raise ValueError("Malformed metadata rules")
        if any(rule.get("object_id") == obj["id"] for rule in rules):
            raise UnsupportedCapability(
                "MLV row rules must be declared as check constraints"
            )
        definition = MlvDefinition(
            object_id=obj["id"],
            target=target,
            version=version,
            mlv_query=contract["mlv_query"],
            inputs=tuple(inputs),
            output_columns=columns,
            grain=tuple(contract["grain"]),
            constraints=tuple(selected_constraints),
            refresh_policy=contract.get("refresh_policy", "Optimal"),
            configuration_sha256=configuration_sha256,
        )
        try:
            definition.parsed()
        except (KeyError, TypeError) as error:
            raise ValueError("Malformed MLV constraint schema") from error
        results.append(definition)
    return tuple(results)


class StateStore(Protocol):
    def read(self, key: str) -> dict[str, Any] | None: ...
    def write(self, key: str, value: dict[str, Any]) -> None: ...


class LocalStateStore:
    """Atomic local JSON persistence. Caller fencing supplies writer exclusion."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)

    def _path(self, key: str) -> Path:
        return self.directory / (_digest(key) + ".json")

    def read(self, key: str) -> dict[str, Any] | None:
        path = self._path(key)
        if not path.exists():
            return None
        if path.stat().st_size > MAX_STATE_BYTES:
            raise MlvError("Oversized MLV state")
        return json.loads(path.read_text(encoding="utf-8"))

    def write(self, key: str, value: dict[str, Any]) -> None:
        text = _json(value)
        self.directory.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(dir=self.directory)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self._path(key))
        finally:
            if os.path.exists(name):
                os.unlink(name)


class NotebookStateStore:
    """Durable notebookutils.fs transport; NOT a lock or cross-file transaction."""

    def __init__(self, fs: Any, directory: str) -> None:
        self.fs, self.directory = fs, directory.rstrip("/")

    def _path(self, key: str) -> str:
        return f"{self.directory}/{_digest(key)}.json"

    def read(self, key: str) -> dict[str, Any] | None:
        path = self._path(key)
        if not self.fs.exists(path):
            return None
        text = self.fs.head(path, MAX_STATE_BYTES + 1)
        if len(text.encode("utf-8")) > MAX_STATE_BYTES:
            raise MlvError("Oversized MLV state")
        return json.loads(text)

    def write(self, key: str, value: dict[str, Any]) -> None:
        self.fs.mkdirs(self.directory)
        if self.fs.put(self._path(key), _json(value), True) is False:
            raise MlvError("MLV evidence persistence failed")
        if self.read(key) != value:
            raise MlvError("MLV evidence persistence did not read back exactly")


class MlvRuntime(Protocol):
    def schema_enabled(self) -> bool: ...
    def definition(self, target: str) -> str | None: ...
    def validate(self, definition: MlvDefinition) -> None: ...
    def execute(self, ddl: str) -> None: ...
    def reference(self, table: str) -> dict[str, Any]: ...
    def source_settings(self, table: str) -> dict[str, Any]: ...
    def reconcile(
        self, definition: MlvDefinition, inputs: dict[str, Any], output: dict[str, Any]
    ) -> dict[str, Any]: ...


class MlvJobs(Protocol):
    def scope(self, definitions: Sequence[MlvDefinition]) -> dict[str, Any]: ...
    def schedules(self) -> list[dict[str, Any]]: ...
    def history(self) -> list[dict[str, Any]]: ...
    def submit(self) -> dict[str, Any]: ...
    def get(self, job_id: str) -> dict[str, Any]: ...


class FabricMlvJobs:
    """Inject an existing FabricClient and an existing selected-lineage definition.

    No visual-lineage endpoint is invented. ``extendedLineageExecutionContext``
    must select no upstream lakehouses: this bounded adapter refreshes only the
    declared local Gold MLVs, not an unowned upstream chain.
    """

    def __init__(
        self,
        client: Any,
        workspace_id: str,
        lakehouse_id: str,
        execution_definition_id: str,
        *,
        lakehouse_name: str,
    ) -> None:
        self.client = client
        self.workspace_id, self.lakehouse_id = workspace_id, lakehouse_id
        self.execution_definition_id = execution_definition_id
        self.lakehouse_name = _name(lakehouse_name, IDENT)

    def scope(self, definitions: Sequence[MlvDefinition]) -> dict[str, Any]:
        value = self.client.get_mlv_execution_definition(
            self.workspace_id, self.lakehouse_id, self.execution_definition_id
        )
        selected = value.get("currentLakehouseExecutionContext", {})
        upstream = value.get("extendedLineageExecutionContext", {})
        names = [d.target.split(".", 1)[1] for d in definitions]
        if (
            any(d.target.split(".")[0] != self.lakehouse_name for d in definitions)
            or selected.get("mode") != "Selected"
            or sorted(selected.get("selectedMlvs", [])) != sorted(names)
            or upstream.get("mode") != "Selected"
            or upstream.get("selectedLakehouses") != []
            or {d.refresh_policy for d in definitions}
            != {value.get("settings", {}).get("refreshMode")}
        ):
            raise UnsupportedCapability(
                "Execution definition scope/mode is not the owned set"
            )
        return value

    def schedules(self) -> list[dict[str, Any]]:
        return self.client.list_mlv_refresh_schedules(
            self.workspace_id, self.lakehouse_id
        )

    def history(self) -> list[dict[str, Any]]:
        history = self.client.list_mlv_refresh_jobs(
            self.workspace_id, self.lakehouse_id
        )
        for job in history:
            if (
                job.get("itemId") != self.lakehouse_id
                or job.get("jobType", "").lower() != "refreshmaterializedlakeviews"
            ):
                raise MlvError("History contains an unbound/non-MLV job")
        return history

    def submit(self) -> dict[str, Any]:
        return self.client.submit_mlv_refresh(
            self.workspace_id,
            self.lakehouse_id,
            execution_definition_id=self.execution_definition_id,
        )

    def get(self, job_id: str) -> dict[str, Any]:
        job = self.client.get_mlv_refresh_job(
            self.workspace_id, self.lakehouse_id, job_id
        )
        if (
            job.get("id") != job_id
            or job.get("itemId") != self.lakehouse_id
            or job.get("jobType", "").lower() != "refreshmaterializedlakeviews"
        ):
            raise MlvError("Job readback belongs to another submission/Lakehouse")
        return job


def _check_references(inputs: Mapping[str, Any]) -> None:
    for table, ref in inputs.items():
        _name(table)
        if (
            set(ref) != {"table", "location", "version"}
            or ref["table"] != table
            or type(ref["version"]) is not int
            or ref["version"] < 0
            or not isinstance(ref["location"], str)
            or not ref["location"]
            or any(c in ref["location"] for c in "`?#")
        ):
            raise ValueError("Invalid pinned Delta reference")


def exact_rows(
    expected: Sequence[tuple[object, ...]], actual: Sequence[tuple[object, ...]]
) -> dict[str, int]:
    """Bounded offline multiset comparator; Spark uses distributed exceptAll.

    No sorting, rounding, coercion or de-duplication. Decimal/null values and
    duplicate multiplicities are retained. NaN/nonfinite numeric data is blocked.
    """
    if len(expected) + len(actual) > 100000:
        raise ValueError("Use distributed Spark reconciliation for larger results")
    for row in (*expected, *actual):
        for value in row:
            if isinstance(value, float) and not Decimal(str(value)).is_finite():
                raise MlvError("Nonfinite aggregate values are not reconcilable")
            if isinstance(value, Decimal) and not value.is_finite():
                raise MlvError("Nonfinite aggregate values are not reconcilable")
    left = Counter(
        tuple((type(value).__name__, value) for value in row) for row in expected
    )
    right = Counter(
        tuple((type(value).__name__, value) for value in row) for row in actual
    )
    return {
        "expected_rows": len(expected),
        "actual_rows": len(actual),
        "expected_only": sum((left - right).values()),
        "actual_only": sum((right - left).values()),
    }


class SparkMlvRuntime:
    """Real Spark/Delta readback and reconciliation, never success-shaped stubs."""

    def __init__(self, spark: Any, *, schema_enabled: Callable[[], bool]) -> None:
        self.spark, self.schema_enabled = spark, schema_enabled

    def definition(self, target: str) -> str | None:
        _name(target)
        if not self.spark.catalog.tableExists(target):
            return None
        rows = (
            self.spark.sql(f"SHOW CREATE MATERIALIZED LAKE VIEW {target}")
            .limit(2)
            .collect()
        )
        if len(rows) != 1:
            raise UnsupportedCapability("Unknown SHOW CREATE MLV response shape")
        return str(rows[0][0])

    def validate(self, definition: MlvDefinition) -> None:
        definition.parsed()
        frame = self.spark.sql(definition.parsed().query)
        if [(f.name, f.dataType.simpleString()) for f in frame.schema] != list(
            definition.output_columns
        ):
            raise MlvError(
                "Analyzed Spark output schema differs from approved contract"
            )

    def execute(self, ddl: str) -> None:
        self.spark.sql(ddl)

    def reference(self, table: str) -> dict[str, Any]:
        _name(table)
        from delta.tables import DeltaTable

        detail = self.spark.sql(f"DESCRIBE DETAIL {table}").first()
        if detail["format"].lower() != "delta":
            raise UnsupportedCapability("MLV input/output is not Delta")
        location = detail["location"]
        version = DeltaTable.forPath(self.spark, location).history(1).first()["version"]
        return {"table": table, "location": location, "version": version}

    def source_settings(self, table: str) -> dict[str, Any]:
        _name(table)
        detail = self.spark.sql(f"DESCRIBE DETAIL {table}").first()
        return {
            "format": detail["format"],
            "cdf_enabled": str(
                detail["properties"].get("delta.enableChangeDataFeed", "false")
            ).lower()
            == "true",
        }

    def _read(self, reference: dict[str, Any]) -> Any:
        _check_references({reference["table"]: reference})
        return (
            self.spark.read.format("delta")
            .option("versionAsOf", reference["version"])
            .load(reference["location"])
        )

    def reconcile(
        self, definition: MlvDefinition, inputs: dict[str, Any], output: dict[str, Any]
    ) -> dict[str, Any]:
        from pyspark.sql import functions as F

        parsed = definition.parsed()
        fact = self._read(inputs[parsed.source])
        expected = (
            fact.groupBy(*parsed.groups)
            .agg(
                *[F.sum(F.col(c)).alias(c) for c in parsed.sums],
                F.count(F.lit(1)).alias("source_row_count"),
            )
            .withColumn("data_scope", F.lit("SYNTHETIC_PROVISIONAL"))
            .select(*parsed.columns)
        )
        actual = self._read(output)
        if [(f.name, f.dataType.simpleString()) for f in actual.schema] != list(
            definition.output_columns
        ) or [(f.name, f.dataType) for f in expected.schema] != [
            (f.name, f.dataType) for f in actual.schema
        ]:
            raise MlvError("Persisted aggregate schema differs from canonical facts")
        constraints = [
            {
                "id": "declared_grain",
                "kind": "unique",
                "enforcement": "runtime",
                "violations": actual.groupBy(*definition.grain)
                .count()
                .filter(F.col("count") > 1)
                .count(),
            }
        ]
        for constraint in definition.constraints:
            spec, kind = constraint["definition"], constraint["kind"]
            columns = (
                spec["parameters"]["columns"] if kind == "check" else spec["columns"]
            )
            nulls = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in columns])
            if kind == "check":
                violations = actual.filter(nulls).count()
            elif kind == "unique":
                violations = (
                    actual.filter(nulls).count()
                    + actual.groupBy(*columns)
                    .count()
                    .filter(F.col("count") > 1)
                    .count()
                )
            else:
                parent = self._read(inputs[spec["reference_table"]]).select(
                    *[
                        F.col(p).alias(c)
                        for c, p in zip(columns, spec["reference_columns"], strict=True)
                    ]
                )
                # Missing/null parents remain violations; never repair or drop rows.
                violations = actual.join(parent, columns, "left_anti").count()
            constraints.append(
                {
                    "id": constraint["id"],
                    "kind": kind,
                    "enforcement": "runtime",
                    "violations": violations,
                }
            )
        return {
            "expected_rows": expected.count(),
            "actual_rows": actual.count(),
            "expected_only": expected.exceptAll(actual).count(),
            "actual_only": actual.exceptAll(expected).count(),
            "constraints": constraints,
        }


@dataclass(frozen=True)
class RefreshObservation:
    """Actual per-view behavior from retained monitoring evidence, NOT CDF."""

    job_id: str
    target: str
    mode: str
    evidence_ref: str


class MlvLifecycle:
    """Synchronous bounded operations with durable recovery and injected I/O."""

    def __init__(
        self,
        store: StateStore,
        runtime: MlvRuntime,
        jobs: MlvJobs,
        *,
        ownership_check: Callable[[], None],
        release: Mapping[str, Any],
        release_check: Callable[[], None],
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store, self.runtime, self.jobs = store, runtime, jobs
        self.ownership_check, self.clock, self.sleep = ownership_check, clock, sleep
        self.release_check = release_check
        release = json.loads(_json(dict(release)))
        if (
            release.get("status") != "FACTS_READY"
            or release.get("execution_mode") != "synthetic_demo"
        ):
            raise MlvError("An explicit synthetic FACTS_READY release is required")
        _text(release["gold_release_id"])
        self.release_outputs = json.loads(release["output_manifest_json"])
        self.release_identity = {
            "gold_release_id": release["gold_release_id"],
            "contract_hash": release["contract_hash"],
            "inputs_sha256": _digest(json.loads(release["input_manifest_json"])),
            "outputs_sha256": _digest(self.release_outputs),
            "execution_mode": release["execution_mode"],
        }

    def _current(self, inputs: dict[str, Any]) -> None:
        self.ownership_check()
        self.release_check()
        _check_references(inputs)
        for table, ref in inputs.items():
            matches = [
                entry
                for entry in self.release_outputs.values()
                if entry.get("table") == table
            ]
            if (
                len(matches) != 1
                or {
                    field: matches[0].get(field)
                    for field in ("table", "location", "version")
                }
                != ref
            ):
                raise MlvError(
                    "An MLV input is not a pinned selected Gold release output"
                )
            if self.runtime.reference(table) != ref:
                raise MlvError(f"{table}: current Delta location/version changed")

    def _definition(self, definition: MlvDefinition) -> str | None:
        ddl = self.runtime.definition(definition.target)
        return parse_mlv_sql(ddl, definition.target).query if ddl is not None else None

    def _unscheduled(self) -> None:
        if any(s.get("enabled") is not False for s in self.jobs.schedules()):
            raise MlvError(
                "Scheduled refresh conflicts with the coordinated serving window"
            )

    def register(
        self,
        definition: MlvDefinition,
        inputs: dict[str, Any],
        *,
        approved_sha256: str,
        approval_id: str,
    ) -> dict[str, Any]:
        """Apply approved changes only; a lost DDL response is recovered by readback."""
        key = "definition:" + definition.target
        definition, inputs = deepcopy(definition), deepcopy(inputs)
        sha256, query = definition.sha256, definition.parsed().query
        if approved_sha256 != sha256:
            raise MlvError("MLV definition hash lacks matching approval")
        _text(approval_id)
        state = self.store.read(key)
        try:
            if set(inputs) != set(definition.inputs):
                raise ValueError("Registration input membership differs")
            self._current(inputs)
            self._unscheduled()
            if self.runtime.schema_enabled() is not True:
                raise UnsupportedCapability(
                    "A verified schema-enabled Lakehouse is required"
                )
            self.runtime.validate(definition)
            existing = self._definition(definition)
            if state and state["sha256"] == sha256:
                if existing != query:
                    raise MlvError(
                        "Recorded MLV definition is missing or drifted; explicit recovery required"
                    )
                self._current(inputs)
                state.update(status="REGISTERED", error_type=None)
                self.store.write(key, state)
                return state
            if state and state["status"] != "REGISTERED":
                raise SubmissionUncertain(
                    "Previous definition mutation must be reconciled first"
                )
            if existing is not None and (not state or existing != state["query"]):
                raise MlvError("Refusing to replace an unowned or drifted MLV")
            if state is not None and existing is None:
                raise MlvError("Recorded MLV disappeared; explicit recovery required")
            state = {
                "sha256": sha256,
                "query": query,
                "version": definition.version,
                "approval_id": approval_id,
                "status": "AUTHORING_UNCERTAIN",
                "inputs": inputs,
                "error_type": None,
            }
            self.store.write(key, state)  # Intent is durable BEFORE Spark DDL.
            self._current(inputs)
            self.runtime.execute(definition.ddl(replace=existing is not None))
            if self._definition(definition) != query:
                raise MlvError("MLV definition did not read back exactly")
            self._current(inputs)
            state["status"] = "REGISTERED"
            self.store.write(key, state)
            return state
        except Exception as error:
            if state is not None:
                state["error_type"] = type(error).__name__
                self.store.write(key, state)
            raise

    def _guard(
        self, definitions: Sequence[MlvDefinition], inputs: dict[str, Any]
    ) -> dict[str, Any]:
        self._current(inputs)
        self._unscheduled()
        for definition in definitions:
            state = self.store.read("definition:" + definition.target)
            if (
                not state
                or state["status"] != "REGISTERED"
                or state["sha256"] != definition.sha256
                or self._definition(definition) != definition.parsed().query
            ):
                raise MlvError(
                    "Refresh requires unchanged registered approved definitions"
                )
        return self.jobs.scope(definitions)

    def refresh(
        self,
        run_id: str,
        definitions: Sequence[MlvDefinition],
        inputs: dict[str, Any],
        *,
        observe_mode: Callable[[str, str], RefreshObservation],
        timeout_seconds: int = 1800,
        poll_seconds: int = 10,
        recovered_job_id: str | None = None,
        recovery_evidence_ref: str | None = None,
    ) -> dict[str, Any]:
        """Submit once or resume a recorded job; return technical readiness only.

        Unknown submissions query history and block. Adoption requires a caller's
        explicit job ID AND retained attribution evidence; a timestamp/singleton
        history match alone is not an idempotency key.
        """
        _text(run_id)
        if not 1 <= timeout_seconds <= 86400 or not 1 <= poll_seconds <= 300:
            raise ValueError("Refresh deadlines/poll intervals must be bounded")
        if (
            not definitions
            or len(definitions) > 50
            or len({d.target for d in definitions}) != len(definitions)
            or set(inputs) != {table for d in definitions for table in d.inputs}
        ):
            raise ValueError("Refresh membership must be nonempty, exact and bounded")
        definitions = deepcopy(tuple(definitions))
        identity = {
            "definitions": {d.target: d.sha256 for d in definitions},
            "inputs": inputs,
            "release": self.release_identity,
        }
        # Materialize immutable JSON identity before any callback can mutate inputs.
        identity = json.loads(_json(identity))
        inputs = identity["inputs"]
        key = "refresh:" + run_id
        state = self.store.read(key)
        if state is not None and state["identity"] != identity:
            raise MlvError(
                "A refresh attempt cannot be rebound to changed definitions/inputs"
            )
        if state is not None and state["status"] == "BLOCKED":
            raise MlvError(
                "A blocked pre-submission attempt requires a new run identity"
            )
        try:
            scope = self._guard(definitions, inputs)
            if state is None:
                history = self.jobs.history()
                if any(j.get("status") not in TERMINAL for j in history):
                    raise MlvError("An existing/unknown refresh job conflicts")
                state = {
                    "run_id": run_id,
                    "identity": identity,
                    "scope_sha256": _digest(scope),
                    "status": "SUBMISSION_UNCERTAIN",
                    "job_id": None,
                    "baseline_jobs": [j["id"] for j in history],
                    "intent_utc": datetime.fromtimestamp(self.clock(), UTC).isoformat(),
                    "deadline": self.clock() + timeout_seconds,
                    "error_type": None,
                    "source_settings": {
                        table: self.runtime.source_settings(table) for table in inputs
                    },
                }
                if any(
                    s.get("format") != "delta" or type(s.get("cdf_enabled")) is not bool
                    for s in state["source_settings"].values()
                ):
                    raise UnsupportedCapability(
                        "Delta format and explicit source CDF readback are required"
                    )
                self.store.write(key, state)
                self._current(inputs)
                receipt = self.jobs.submit()
                _text(receipt["job_id"])
                if (
                    type(receipt["retry_after"]) is not int
                    or receipt["retry_after"] < 0
                ):
                    raise MlvError("Invalid job submission receipt")
                state.update(
                    status="MONITORING",
                    job_id=receipt["job_id"],
                    receipt=receipt,
                    next_poll=self.clock() + receipt["retry_after"],
                )
                self.store.write(key, state)  # Job identity BEFORE any GET/poll.
            elif state["scope_sha256"] != _digest(scope):
                raise MlvError(
                    "Refresh execution definition changed during the attempt"
                )
            if state["job_id"] is None:
                intent = datetime.fromisoformat(state["intent_utc"])
                candidates = [
                    job
                    for job in self.jobs.history()
                    if job.get("id") not in state["baseline_jobs"]
                    and job.get("invokeType") == "Manual"
                    and job.get("startTimeUtc")
                    and datetime.fromisoformat(job["startTimeUtc"]) >= intent
                ]
                state["recovery_candidates"] = [job["id"] for job in candidates]
                self.store.write(key, state)
                if recovered_job_id not in state["recovery_candidates"]:
                    raise SubmissionUncertain(
                        "Query existing jobs; explicit attributed recovery is required"
                    )
                _text(recovery_evidence_ref)
                state.update(
                    job_id=recovered_job_id,
                    recovery_evidence_ref=recovery_evidence_ref,
                    status="MONITORING",
                    next_poll=self.clock(),
                )
                self.store.write(key, state)
            while True:
                wait = min(
                    max(0, state.get("next_poll", 0) - self.clock()),
                    max(0, state["deadline"] - self.clock()),
                )
                if wait:
                    self.sleep(wait)
                if self.clock() < state.get("next_poll", 0):
                    raise TimeoutError(
                        "Deadline elapsed before Retry-After; job identity retained"
                    )
                if _digest(self._guard(definitions, inputs)) != state["scope_sha256"]:
                    raise MlvError("Refresh scope changed")
                job = self.jobs.get(state["job_id"])
                state["last_job"] = {
                    field: job[field]
                    for field in (
                        "id",
                        "itemId",
                        "jobType",
                        "status",
                        "invokeType",
                        "startTimeUtc",
                        "endTimeUtc",
                    )
                    if field in job
                }
                failure = job.get("failureReason")
                if isinstance(failure, dict):
                    code = failure.get("errorCode")
                    if isinstance(code, str) and re.fullmatch(
                        r"[A-Za-z][A-Za-z0-9_]{0,127}", code
                    ):
                        state["last_job"]["failureReason"] = {"errorCode": code}
                state["next_poll"] = self.clock() + poll_seconds
                self.store.write(key, state)
                status = job.get("status")
                if status == "Completed":
                    break
                if status in TERMINAL:
                    state["status"] = "REFRESH_FAILED"
                    raise MlvError(f"MLV refresh ended {status}; readiness withheld")
                if status not in ACTIVE:
                    raise UnsupportedCapability("Unknown refresh status")
                if self.clock() >= state["deadline"]:
                    raise TimeoutError(
                        "MLV monitoring deadline elapsed; resume the existing job"
                    )
            outputs = {}
            for definition in definitions:
                observation = observe_mode(state["job_id"], definition.target)
                expected_constraints = {
                    "declared_grain": ("unique", "runtime"),
                    **{
                        c["id"]: (c["kind"], c["enforcement"])
                        for c in definition.constraints
                    },
                }
                if (
                    not isinstance(observation, RefreshObservation)
                    or observation.job_id != state["job_id"]
                    or observation.target != definition.target
                    or observation.mode not in {"Full", "Incremental", "NoRefresh"}
                    or not observation.evidence_ref
                ):
                    raise UnsupportedCapability(
                        "Actual per-view refresh mode evidence is missing"
                    )
                if definition.refresh_policy == "Full" and observation.mode != "Full":
                    raise MlvError(
                        "Observed mode differs from the explicit Full policy"
                    )
                settings = {
                    table: self.runtime.source_settings(table)
                    for table in definition.inputs
                }
                if settings != {
                    table: state["source_settings"][table]
                    for table in definition.inputs
                }:
                    raise MlvError("MLV source settings changed during refresh")
                if observation.mode == "Incremental" and any(
                    s.get("format") != "delta" or s.get("cdf_enabled") is not True
                    for s in settings.values()
                ):
                    raise MlvError(
                        "Incremental evidence conflicts with source prerequisites"
                    )
                output = self.runtime.reference(definition.target)
                _check_references({definition.target: output})
                evidence = self.runtime.reconcile(definition, inputs, output)
                outputs[definition.target] = {
                    "reference": output,
                    "mode": asdict(observation),
                    "source_settings": settings,
                    "reconciliation": evidence,
                    "capacity_impact": (
                        "Full recompute; capacity usage is unmeasured"
                        if observation.mode == "Full"
                        else "Capacity usage is unmeasured"
                    ),
                }
                state["outputs"] = outputs
                self.store.write(key, state)  # Retain failure counts BEFORE raising.
                reported_constraints = evidence.get("constraints", [])
                if (
                    any(
                        type(evidence.get(k)) is not int or evidence[k] < 0
                        for k in (
                            "expected_rows",
                            "actual_rows",
                            "expected_only",
                            "actual_only",
                        )
                    )
                    or evidence["expected_only"]
                    or evidence["actual_only"]
                    or evidence["expected_rows"] != evidence["actual_rows"]
                    or len(reported_constraints) != len(expected_constraints)
                    or {
                        c.get("id"): (c.get("kind"), c.get("enforcement"))
                        for c in reported_constraints
                    }
                    != expected_constraints
                    or any(
                        c.get("enforcement") != "runtime"
                        or type(c.get("violations")) is not int
                        or c["violations"] != 0
                        for c in reported_constraints
                    )
                ):
                    raise MlvError(
                        "Exact aggregate reconciliation or constraints failed"
                    )
            if _digest(self._guard(definitions, inputs)) != state["scope_sha256"]:
                raise MlvError("Refresh scope changed before readiness")
            for target, result in outputs.items():
                if self.runtime.reference(target) != result["reference"]:
                    raise MlvError("MLV output version changed during reconciliation")
            self.ownership_check()
            state.update(
                status="MLVS_VERIFIED",
                error_type=None,
                business_publication=False,
                native_gates_passed=False,
            )
            self.store.write(key, state)
            return state
        except Exception as error:
            if state is None:
                state = {"run_id": run_id, "identity": identity, "status": "BLOCKED"}
            state.update(
                error_type=type(error).__name__,
                business_publication=False,
                native_gates_passed=False,
            )
            if state["status"] == "MLVS_VERIFIED":
                state["status"] = "INVALIDATED"
            self.store.write(key, state)
            raise
