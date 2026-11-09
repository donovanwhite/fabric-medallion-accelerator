"""Fail-closed, shared physical-layout adapters (REQ-039..043, REQ-059).

No Spark import, discovery, network access or mutation occurs on import.
Call ``prewrite_plan`` immediately before *every* append/merge, under the
parent's writer ownership. For creation, prefer ``create_target`` with the
complete physical schema (including lineage), then append normally. Existing
targets never receive creation options or implicit layout changes.

Syntax sources (documentation is not runtime capability evidence):
* https://docs.delta.io/delta-clustering/
* https://docs.delta.io/delta-batch/
* https://docs.delta.io/delta-utility/
* https://learn.microsoft.com/en-us/fabric/data-engineering/liquid-clustering
* https://learn.microsoft.com/en-us/azure/databricks/tables/operations/table-details

The parent supplies runtime-specific, operation-specific capability evidence.
An external fenced writer lease is mandatory for maintenance. This module
cannot make SQL metadata, leases and Delta a cross-store transaction. There is
deliberately no migration/cutover executor, automatic retry, VACUUM, RESTORE,
overwrite, or replacement of historical tables.
"""

import hashlib
import json
import math
import re
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from typing import Any, Protocol

# Any is confined to optional, duck-typed Spark/Delta framework boundaries.
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_PRIMITIVES = {
    "string",
    "boolean",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "date",
    "timestamp",
    "timestamp_ntz",
}
_ALIASES = {"long": "bigint", "integer": "int", "short": "smallint", "byte": "tinyint"}
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)\Z")
_LAYOUT_KEYS = {"layout_strategy", "partition_columns", "cluster_by_columns"}


class LayoutError(ValueError):
    """Invalid or unverifiable layout; no ordinary write may proceed."""


class OperationRejected(LayoutError, RuntimeError):
    """An approved operation no longer matches its preconditions."""


class MigrationRequired(LayoutError, RuntimeError):
    """A separate, target-specific migration/cutover contract is required."""


class OperationIncomplete(RuntimeError):
    """A native operation may have committed; reconcile instead of retrying."""


def _identifier(value: object) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise LayoutError(f"Invalid SQL identifier: {value!r}")
    return value


def quote_table(table: str) -> str:
    """Quote a one-, two- or three-part catalog identifier; paths are not accepted."""
    if not isinstance(table, str) or not 1 <= len(table.split(".")) <= 3:
        raise LayoutError("Expected a one-, two- or three-part table identifier")
    return ".".join(f"`{_identifier(part)}`" for part in table.split("."))


def _ordered_columns(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)):
        raise LayoutError(f"{label} must be an ordered list, not null/string/set")
    columns = tuple(_identifier(c) for c in value)
    if len({c.casefold() for c in columns}) != len(columns):
        raise LayoutError(f"{label} contains duplicate columns")
    return columns


def _type_name(value: object) -> str:
    if not isinstance(value, str):
        raise LayoutError("Column data types must be explicit Spark SQL types")
    normalized = re.sub(r"\s+", "", value.lower())
    normalized = _ALIASES.get(normalized, normalized)
    if normalized in _PRIMITIVES:
        return normalized
    decimal = _DECIMAL.fullmatch(normalized)
    if decimal:
        precision, scale = map(int, decimal.groups())
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return f"decimal({precision},{scale})"
    raise LayoutError(
        f"Unsupported column type: {value!r}; use a validated schema adapter"
    )


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    nullable: bool = True


def schema_columns(schema: Any) -> tuple[Column, ...]:
    """Normalize Column sequences or a Spark StructType, never schema SQL text."""
    if hasattr(schema, "fields"):
        columns = tuple(
            Column(field.name, field.dataType.simpleString(), field.nullable)
            for field in schema.fields
        )
    elif isinstance(schema, (tuple, list)) and all(
        isinstance(c, Column) for c in schema
    ):
        columns = tuple(schema)
    else:
        raise LayoutError(
            "Schema must be a nonempty Column sequence or Spark StructType"
        )
    if not columns:
        raise LayoutError("An explicit nonempty physical schema is required")
    checked = []
    for column in columns:
        _identifier(column.name)
        if type(column.nullable) is not bool:
            raise LayoutError("Column nullable must be a Boolean")
        checked.append(
            Column(column.name, _type_name(column.data_type), column.nullable)
        )
    _ordered_columns([c.name for c in checked], "schema")
    return tuple(checked)


@dataclass(frozen=True)
class RuntimeCapabilities:
    """Parent-owned evidence from the selected engine, not a feature-version guess.

    Operations: liquid_write, liquid_create, liquid_configure, optimize,
    mlv_partitioned, mlv_liquid. MLV validation is NOT a table-DDL adapter.
    statistics_columns names columns whose Delta statistics coverage was verified.
    The parent must bind engine_id/evidence to its actual session/environment.
    """

    engine_id: str
    spark_version: str
    evidence: str
    operations: tuple[str, ...] = ()
    statistics_columns: tuple[str, ...] = ()

    def require(self, operation: str, spark: Any = None) -> None:
        if (
            not all(
                isinstance(v, str) and v.strip()
                for v in (self.engine_id, self.spark_version, self.evidence)
            )
            or not isinstance(self.operations, (tuple, list))
            or operation not in self.operations
        ):
            raise LayoutError(
                f"Explicit runtime capability evidence required: {operation}"
            )
        _ordered_columns(self.statistics_columns, "verified statistics_columns")
        if spark is not None and spark.version != self.spark_version:
            raise LayoutError("Capability evidence does not match the Spark runtime")


def _require(
    capabilities: RuntimeCapabilities | None, operation: str, spark: Any = None
) -> None:
    if not isinstance(capabilities, RuntimeCapabilities):
        raise LayoutError(f"Explicit runtime capability evidence required: {operation}")
    capabilities.require(operation, spark)


@dataclass(frozen=True)
class LayoutSpec:
    strategy: str
    columns: tuple[str, ...]
    materialization: str = "delta_table"

    def metadata(self) -> dict[str, object]:
        key = (
            "partition_columns"
            if self.strategy == "partitioned"
            else "cluster_by_columns"
        )
        return {"layout_strategy": self.strategy, key: list(self.columns)}


def validate_layout(
    metadata: Mapping[str, object] | None,
    schema: Any,
    *,
    materialization: str = "delta_table",
    capabilities: RuntimeCapabilities | None = None,
) -> LayoutSpec | None:
    """Validate a layout-only metadata projection. None means preserve, not create.

    Only the active ordered list may be nonempty. Unknown keys (including
    independent toggles and ZORDER) fail rather than being silently ignored.
    Scalar types are deliberately conservative; complex schemas need an adapter.
    """
    columns = schema_columns(schema)
    if materialization not in ("delta_table", "mlv"):
        raise LayoutError(
            "Raw files/views are not compatible managed Delta layout targets"
        )
    if metadata is None:
        metadata = {}
    if not isinstance(metadata, Mapping) or set(metadata) - _LAYOUT_KEYS:
        raise LayoutError(
            "Unknown layout options; use only the single layout_strategy selector"
        )
    partition = _ordered_columns(
        metadata.get("partition_columns", ()), "partition_columns"
    )
    cluster = _ordered_columns(
        metadata.get("cluster_by_columns", ()), "cluster_by_columns"
    )
    strategy = metadata.get("layout_strategy")
    if strategy is None and not partition and not cluster:
        return None
    if strategy not in ("partitioned", "liquid"):
        raise LayoutError(
            "layout_strategy must explicitly select partitioned or liquid"
        )
    active, inactive = (
        (partition, cluster) if strategy == "partitioned" else (cluster, partition)
    )
    if not active or inactive:
        raise LayoutError(
            "Active layout columns must be nonempty; inactive columns must be empty"
        )
    types = {c.name: c.data_type for c in columns}
    if set(active) - types.keys():
        raise LayoutError(
            "Layout columns must exactly match physical schema column names"
        )
    if materialization == "mlv":
        _require(capabilities, f"mlv_{strategy}")
    if strategy == "liquid":
        _require(capabilities, "liquid_write")
        if len(active) > 4 or any(types[c] == "boolean" for c in active):
            raise LayoutError(
                "Liquid layout supports at most four validated scalar columns"
            )
        if set(active) - set(capabilities.statistics_columns):
            raise LayoutError(
                "Liquid columns require verified Delta statistics coverage"
            )
    return LayoutSpec(strategy, active, materialization)


@dataclass(frozen=True)
class TargetLayout:
    table: str
    table_id: str
    version: int
    schema: tuple[Column, ...]
    partition_columns: tuple[str, ...]
    cluster_by_columns: tuple[str, ...]
    size_bytes: int
    table_features: tuple[str, ...] = ()


def _row_dict(row: Any) -> dict[str, Any]:
    return row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)


def _single(frame: Any, label: str) -> dict[str, Any]:
    rows = frame.limit(2).collect()
    if len(rows) != 1:
        raise LayoutError(f"Cannot verify {label}: expected exactly one metadata row")
    return _row_dict(rows[0])


def _version(spark: Any, table: str) -> int:
    row = _single(
        spark.sql(f"DESCRIBE HISTORY {quote_table(table)} LIMIT 1"), "Delta version"
    )
    version = row.get("version")
    if type(version) is not int or version < 0:
        raise LayoutError("Cannot verify Delta history version")
    return version


def inspect_target(spark: Any, table: str) -> TargetLayout | None:
    """Read authoritative detail/schema/history, or return None for an absent table.

    DESCRIBE DETAIL.clusteringColumns is the documented source for cluster keys.
    Older detail schemas may prove absence through the complete tableFeatures
    list. A missing/null column is NOT silently interpreted as unclustered.
    There is no speculative TBLPROPERTIES key/JSON fallback. Engine exceptions
    propagate: an unreadable target is never mistaken for a missing target.
    """
    quoted = quote_table(table)
    if not spark.catalog.tableExists(table):
        return None
    before = _version(spark, table)
    detail = _single(spark.sql(f"DESCRIBE DETAIL {quoted}"), "Delta detail")
    if (
        detail.get("format") != "delta"
        or not isinstance(detail.get("id"), str)
        or not detail["id"]
    ):
        raise LayoutError(
            "Target must be a verifiable Delta table with a stable table id"
        )
    partition = _ordered_columns(
        detail.get("partitionColumns"), "existing partitionColumns"
    )
    features = detail.get("tableFeatures", ())
    if not isinstance(features, (tuple, list)) or not all(
        isinstance(f, str) for f in features
    ):
        raise LayoutError("Cannot verify Delta tableFeatures")
    if detail.get("clusteringColumns") is not None:
        cluster = _ordered_columns(
            detail["clusteringColumns"], "existing clusteringColumns"
        )
    elif "tableFeatures" in detail and "clustering" not in {
        f.lower() for f in features
    }:
        cluster = ()
    else:
        raise LayoutError(
            "Cannot verify clusteringColumns; a capable detail adapter is required"
        )
    if partition and cluster:
        raise LayoutError(
            "Target reports incompatible partitioning and liquid clustering"
        )
    schema = schema_columns(spark.table(table).schema)
    names = {c.name for c in schema}
    if (set(partition) | set(cluster)) - names:
        raise LayoutError("Target detail references unknown physical columns")
    size = detail.get("sizeInBytes")
    if type(size) is not int or size < 0:
        raise LayoutError("Cannot verify target sizeInBytes")
    if before != _version(spark, table):
        raise LayoutError(
            "Target version drifted during layout inspection; retry under writer ownership"
        )
    return TargetLayout(
        table, detail["id"], before, schema, partition, cluster, size, tuple(features)
    )


def _compatible(
    existing: TargetLayout, requested: LayoutSpec | None, schema: tuple[Column, ...]
) -> None:
    if requested is None:
        return
    partition = requested.columns if requested.strategy == "partitioned" else ()
    cluster = requested.columns if requested.strategy == "liquid" else ()
    if (existing.partition_columns, existing.cluster_by_columns) != (
        partition,
        cluster,
    ):
        raise MigrationRequired(
            "Existing layout differs: explicit approved migration required before writes"
        )
    old_types = {c.name: c.data_type for c in existing.schema}
    new_types = {c.name: c.data_type for c in schema}
    if any(old_types.get(c) != new_types.get(c) for c in requested.columns):
        raise LayoutError("Layout column type differs from existing physical schema")


@dataclass(frozen=True)
class LayoutPlan:
    table: str
    schema: tuple[Column, ...]
    requested: LayoutSpec | None
    existing: TargetLayout | None
    adapter: str


def prewrite_plan(
    spark: Any,
    table: str,
    metadata: Mapping[str, object] | None,
    schema: Any,
    *,
    materialization: str = "delta_table",
    capabilities: RuntimeCapabilities | None = None,
) -> LayoutPlan:
    """Inspect before append/merge; no DDL or data writes. Never reuse across writes."""
    quote_table(table)
    columns = schema_columns(schema)
    requested = validate_layout(
        metadata, columns, materialization=materialization, capabilities=capabilities
    )
    if materialization != "delta_table":
        raise LayoutError(
            "MLV creation/inspection requires a separately validated engine adapter"
        )
    existing = inspect_target(spark, table)
    if existing is not None:
        if existing.cluster_by_columns or "clustering" in {
            f.lower() for f in existing.table_features
        }:
            _require(capabilities, "liquid_write", spark)
        _compatible(existing, requested, columns)
        return LayoutPlan(table, columns, requested, existing, "preserve")
    if requested is None:
        raise LayoutError("New managed targets require an explicit layout selection")
    if requested.strategy == "liquid":
        _require(capabilities, "liquid_create", spark)
    adapter = (
        "partitionBy" if requested.strategy == "partitioned" else "create_table_sql"
    )
    return LayoutPlan(table, columns, requested, None, adapter)


def creation_sql(
    table: str,
    schema: Any,
    requested: LayoutSpec | None,
    capabilities: RuntimeCapabilities | None = None,
) -> str:
    """Explicit-schema CREATE only; never CTAS, IF NOT EXISTS, replace or overwrite."""
    columns = schema_columns(schema)
    if requested is None:
        raise LayoutError("Creation requires an explicit layout selection")
    if requested.materialization != "delta_table":
        raise LayoutError("MLV DDL requires a separately validated engine adapter")
    spec = validate_layout(requested.metadata(), columns, capabilities=capabilities)
    if spec.strategy == "liquid":
        _require(capabilities, "liquid_create")
    fields = ", ".join(
        f"`{c.name}` {c.data_type.upper()}" + ("" if c.nullable else " NOT NULL")
        for c in columns
    )
    clause = "PARTITIONED BY" if spec.strategy == "partitioned" else "CLUSTER BY"
    keys = ", ".join(f"`{c}`" for c in spec.columns)
    return f"CREATE TABLE {quote_table(table)} ({fields}) USING DELTA {clause} ({keys})"


def _creation_check(
    spark: Any, plan: LayoutPlan, capabilities: RuntimeCapabilities | None
) -> None:
    if plan.existing is not None or spark.catalog.tableExists(plan.table):
        raise LayoutError(
            "Creation adapter cannot modify an existing or concurrently created target"
        )
    if plan.requested is None:
        raise LayoutError("Creation requires an explicit layout selection")
    creation_sql(plan.table, plan.schema, plan.requested, capabilities)
    if plan.requested.strategy == "liquid":
        _require(capabilities, "liquid_create", spark)


def prepare_creation_writer(spark: Any, writer: Any, plan: LayoutPlan) -> Any:
    """Return a creation-only DataFrameWriter; parent calls saveAsTable(plan.table).

    Pass df.schema to prewrite_plan so lineage fields are included. Never use
    this adapter on an existing target. For liquid, use create_target and append:
    generic DataFrameWriter.clusterBy is not assumed to be available.
    """
    if plan.requested is not None and plan.requested.strategy == "liquid":
        raise LayoutError(
            "DataFrameWriter.clusterBy is unproven; use explicit-schema create_target"
        )
    _creation_check(spark, plan, None)
    return (
        writer.format("delta")
        .mode("errorifexists")
        .partitionBy(*plan.requested.columns)
    )


def create_target(
    spark: Any, plan: LayoutPlan, *, capabilities: RuntimeCapabilities | None = None
) -> TargetLayout:
    """Create an absent table, inspect it, and reject unverifiable results.

    Parent holds creation ownership. A race fails CREATE, never replaces data.
    Failure after DDL may leave an empty table: reconcile, do not drop/retry.
    """
    _creation_check(spark, plan, capabilities)
    spark.sql(creation_sql(plan.table, plan.schema, plan.requested, capabilities))
    created = inspect_target(spark, plan.table)
    if created is None:
        raise LayoutError("Created table cannot be verified; reconcile before writing")
    _compatible(created, plan.requested, plan.schema)
    return created


def ensure_target_layout(
    spark: Any,
    target: str,
    frame: Any,
    layout: Mapping[str, object] | None,
    *,
    existing_target: bool,
    capabilities: RuntimeCapabilities | None = None,
) -> None:
    """Parent integration hook, immediately before merge/append under ownership.

    frame.schema must contain the complete physical projection, including lineage.
    Existing targets are inspected only. New generic targets must have a layout
    and are created empty before the caller merges/appends. A stale existence
    assertion is rejected instead of silently adopting a concurrently created
    table. Historical no-metadata flows can retain their old call path; once
    using this generic hook, null layout means preserve an existing table only.
    """
    if type(existing_target) is not bool:
        raise LayoutError("existing_target must be an explicit Boolean")
    plan = prewrite_plan(spark, target, layout, frame.schema, capabilities=capabilities)
    if existing_target != (plan.existing is not None):
        raise LayoutError("Target existence changed; reconcile under writer ownership")
    if plan.existing is None:
        create_target(spark, plan, capabilities=capabilities)


@dataclass(frozen=True)
class ContentComparison:
    schema_equal: bool
    left_only: int | None
    right_only: int | None

    @property
    def equivalent(self) -> bool:
        return self.schema_equal and self.left_only == 0 and self.right_only == 0


def compare_logical_content(left: Any, right: Any) -> ContentComparison:
    """Distributed multiset equality over ALL columns, including nulls/duplicates.

    exceptAll resolves columns by position, so align names first. No driver-side
    row collection, hash-only comparison, distinct, or count-only equivalence.
    """
    left_schema = schema_columns(left.schema)
    right_schema = schema_columns(right.schema)
    signature = lambda schema: sorted((c.name, c.data_type) for c in schema)
    if signature(left_schema) != signature(right_schema):
        return ContentComparison(False, None, None)
    names = [c.name for c in left_schema]
    aligned_left, aligned_right = left.select(*names), right.select(*names)
    return ContentComparison(
        True,
        aligned_left.exceptAll(aligned_right).count(),
        aligned_right.exceptAll(aligned_left).count(),
    )


@dataclass(frozen=True)
class ResourceEstimate:
    read_bytes: int
    estimated_write_bytes: int
    retained_history_bytes: int
    elapsed_seconds: float | None
    capacity_unit_seconds: float | None
    cost: float | None
    assumptions: tuple[str, ...]


def _positive_number(value: object, label: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise LayoutError(f"{label} must be a finite positive number")
    return float(value)


def _estimate(
    current: TargetLayout,
    rewrites: bool,
    throughput_bytes_per_second: float | None,
    capacity_units: float | None,
    cost_per_cu_second: float | None,
) -> ResourceEstimate:
    seconds = cu_seconds = cost = None
    if throughput_bytes_per_second is not None:
        throughput = _positive_number(throughput_bytes_per_second, "throughput")
        seconds = current.size_bytes / throughput if rewrites else 0.0
    if capacity_units is not None:
        units = _positive_number(capacity_units, "capacity_units")
        if seconds is not None:
            cu_seconds = seconds * units
    if cost_per_cu_second is not None:
        rate = _positive_number(cost_per_cu_second, "cost_per_cu_second")
        if cu_seconds is not None:
            cost = cu_seconds * rate
    return ResourceEstimate(
        current.size_bytes if rewrites else 0,
        current.size_bytes if rewrites else 0,
        current.size_bytes,
        seconds,
        cu_seconds,
        cost,
        (
            "Source size is compressed Delta size, not a measured rewrite bound.",
            "Worst-case full rewrite assumed for OPTIMIZE; no incremental-runtime guarantee.",
            "Output size assumed equal to input; retain old files/history and budget extra headroom.",
            "Throughput is caller-measured end-to-end source bytes/s; CU/rate are caller supplied.",
            "Unknown throughput/capacity/rate stays unknown; estimate is not execution approval.",
        ),
    )


def _token(plan: object) -> str:
    return hashlib.sha256(
        json.dumps(asdict(plan), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class MaintenancePlan:
    current: TargetLayout
    requested: LayoutSpec
    operation: str
    sql: str
    rewrites_data: bool
    estimate: ResourceEstimate
    capabilities: RuntimeCapabilities

    @property
    def token(self) -> str:
        return _token(self)


def plan_maintenance(
    spark: Any,
    table: str,
    metadata: Mapping[str, object],
    schema: Any,
    *,
    operation: str,
    capabilities: RuntimeCapabilities | None = None,
    throughput_bytes_per_second: float | None = None,
    capacity_units: float | None = None,
    cost_per_cu_second: float | None = None,
) -> MaintenancePlan:
    """Read-only plan: configure changes keys; optimize does the physical work.

    configure is limited to already-liquid tables. Enabling liquid on historical
    unclustered tables is a migration, not an ordinary metadata maintenance task.
    No FULL, ZORDER, arbitrary SQL, retention changes or VACUUM are exposed.
    """
    requested = validate_layout(metadata, schema, capabilities=capabilities)
    current = inspect_target(spark, table)
    if requested is None or current is None:
        raise LayoutError("Maintenance requires an existing target and explicit layout")
    if operation == "configure":
        if (
            requested.strategy != "liquid"
            or not current.cluster_by_columns
            or current.partition_columns
        ):
            raise MigrationRequired(
                "Configuring historical non-liquid tables requires a migration"
            )
        _require(capabilities, "liquid_configure", spark)
        old_types = {c.name: c.data_type for c in current.schema}
        new_types = {c.name: c.data_type for c in schema_columns(schema)}
        if any(old_types.get(c) != new_types.get(c) for c in requested.columns):
            raise LayoutError("Cluster column types do not match the target schema")
        keys = ", ".join(f"`{c}`" for c in requested.columns)
        sql = f"ALTER TABLE {quote_table(table)} CLUSTER BY ({keys})"
    elif operation == "optimize":
        _compatible(current, requested, schema_columns(schema))
        _require(capabilities, "optimize", spark)
        sql = f"OPTIMIZE {quote_table(table)}"
    else:
        raise LayoutError(
            "Only separate configure and optimize operations are supported"
        )
    return MaintenancePlan(
        current,
        requested,
        operation,
        sql,
        operation == "optimize",
        _estimate(
            current,
            operation == "optimize",
            throughput_bytes_per_second,
            capacity_units,
            cost_per_cu_second,
        ),
        capabilities,
    )


@dataclass(frozen=True)
class MigrationPreview:
    current: TargetLayout
    requested: LayoutSpec
    input_versions: tuple[tuple[str, int], ...]
    expected_target_version: int
    result_version: None
    rewrites_data: bool
    estimate: ResourceEstimate
    consumer_requirements: tuple[str, ...]

    @property
    def token(self) -> str:
        return _token(self)


def preview_migration(
    current: TargetLayout,
    requested: LayoutSpec,
    *,
    input_versions: Mapping[str, int],
    capabilities: RuntimeCapabilities | None = None,
    throughput_bytes_per_second: float | None = None,
    capacity_units: float | None = None,
    cost_per_cu_second: float | None = None,
) -> MigrationPreview:
    """Pure preview over an inspected target and caller-pinned inputs; no reads/writes.

    Input versions are declared pins, not proof those versions remain available.
    expected_target_version is the precondition; result_version is unknowable.
    There is deliberately no assertion that config rollback rewinds physical data.
    """
    validate_layout(
        requested.metadata(),
        current.schema,
        materialization=requested.materialization,
        capabilities=capabilities,
    )
    if requested.materialization != "delta_table":
        raise LayoutError(
            "MLV migration requires a validated engine-specific target contract"
        )
    if not input_versions:
        raise LayoutError("Migration preview requires pinned input versions")
    for table, version in input_versions.items():
        quote_table(table)
        if type(version) is not int or version < 0:
            raise LayoutError("Input versions must be explicit nonnegative integers")
    if input_versions.get(current.table) != current.version:
        raise LayoutError("Input pins must include the inspected target version")
    partition = requested.columns if requested.strategy == "partitioned" else ()
    cluster = requested.columns if requested.strategy == "liquid" else ()
    changed = (partition, cluster) != (
        current.partition_columns,
        current.cluster_by_columns,
    )
    return MigrationPreview(
        current,
        requested,
        tuple(sorted(input_versions.items())),
        current.version,
        None,
        changed,
        _estimate(
            current,
            changed,
            throughput_bytes_per_second,
            capacity_units,
            cost_per_cu_second,
        ),
        (
            "Define an isolated target identity/schema, fenced copy and atomic consumer cutover contract.",
            "Prove both-direction exceptAll equivalence over pinned full logical content.",
            "Validate SQL endpoint, Direct Lake/semantic model and MLV compatibility/refresh as applicable.",
            "Retain input versions, old table/files, Delta history and historical batch/release references.",
            "Record independent configuration, worker, data/release and physical-layout rollback choices.",
            "Measure capacity, storage headroom, cost and retention window before execution.",
        ),
    )


@dataclass(frozen=True)
class OperationApproval:
    plan_token: str
    operation_id: str
    configuration_version: str
    worker_version: str
    data_release: str
    rollback_reference: str
    consumer_proof: str
    preserve_history: bool = True


class WriterLease(Protocol):
    """Held by the parent until exit, excluding all writers and retention jobs."""

    def assert_owned(self) -> None:
        """Raise if lease/fencing ownership has expired or changed."""


def _approval_check(
    plan: MaintenancePlan | MigrationPreview, approval: OperationApproval | None
) -> None:
    if not isinstance(approval, OperationApproval) or approval.plan_token != plan.token:
        raise OperationRejected(
            "Explicit approval bound to this exact operation plan is required"
        )
    if approval.preserve_history is not True or not all(
        isinstance(value, str) and value.strip()
        for value in (
            approval.operation_id,
            approval.configuration_version,
            approval.worker_version,
            approval.data_release,
            approval.rollback_reference,
            approval.consumer_proof,
        )
    ):
        raise OperationRejected(
            "Approval requires history preservation and separate rollback/consumer proof"
        )


def _snapshot_check(spark: Any, expected: TargetLayout) -> None:
    actual = inspect_target(spark, expected.table)
    if actual != expected:
        raise OperationRejected(
            "Target identity, version, schema or physical layout drift; regenerate preview"
        )


@dataclass(frozen=True)
class MaintenanceResult:
    before: TargetLayout
    after: TargetLayout
    comparison: ContentComparison
    approval: OperationApproval


def execute_maintenance(
    spark: Any,
    plan: MaintenancePlan,
    *,
    approval: OperationApproval | None,
    ownership: AbstractContextManager[WriterLease],
    capabilities: RuntimeCapabilities | None = None,
) -> MaintenanceResult:
    """Execute one fenced approved command and prove pinned full-content equality.

    Approval represents externally obtained consent, not authorization inferred
    from metadata. Failed/uncertain operations are never retried or rolled back.
    Parent persists operation/result records; no cross-store atomicity is claimed.
    """
    _approval_check(plan, approval)
    if capabilities != plan.capabilities:
        raise OperationRejected("Runtime evidence must match the approved plan")
    # Regenerate SQL instead of executing a caller-supplied SQL string.
    regenerated = plan_maintenance(
        spark,
        plan.current.table,
        plan.requested.metadata(),
        plan.current.schema,
        operation=plan.operation,
        capabilities=capabilities,
    )
    if regenerated.sql != plan.sql:
        raise OperationRejected(
            "Maintenance SQL does not match the validated operation"
        )
    with ownership as lease:
        lease.assert_owned()
        _snapshot_check(spark, plan.current)
        try:
            spark.sql(regenerated.sql)
            lease.assert_owned()
            after = inspect_target(spark, plan.current.table)
            if (
                after is None
                or after.table_id != plan.current.table_id
                or after.version < plan.current.version
            ):
                raise LayoutError(
                    "Maintenance changed target identity or lost Delta history"
                )
            _compatible(after, plan.requested, plan.current.schema)
            before_frame = (
                spark.read.format("delta")
                .option("versionAsOf", plan.current.version)
                .table(plan.current.table)
            )
            after_frame = (
                spark.read.format("delta")
                .option("versionAsOf", after.version)
                .table(plan.current.table)
            )
            comparison = compare_logical_content(before_frame, after_frame)
            if not comparison.equivalent:
                raise LayoutError("Full logical content changed during maintenance")
            lease.assert_owned()
            _snapshot_check(spark, after)
        except Exception as error:
            raise OperationIncomplete(
                f"Operation {approval.operation_id} incomplete; reconcile Delta history and "
                f"lease against version {plan.current.version}; no automatic retry/rollback"
            ) from error
        return MaintenanceResult(plan.current, after, comparison, approval)


def execute_migration(
    spark: Any,
    preview: MigrationPreview,
    *,
    approval: OperationApproval | None,
    ownership: AbstractContextManager[WriterLease],
) -> None:
    """Explicitly gated, intentionally NOT an implementation of physical migration."""
    _approval_check(preview, approval)
    with ownership as lease:
        lease.assert_owned()
        _snapshot_check(spark, preview.current)
        raise MigrationRequired(
            "Physical migration is not implemented: supply an isolated target contract, "
            "fenced copy/cutover, retained history and downstream consumer proof; no writes performed"
        )
