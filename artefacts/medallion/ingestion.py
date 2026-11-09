"""Shared Fabric runtime, embedded into each generated source-specific notebook.

Run only against the selected attached Lakehouse. Source values are never
printed; rejected raw fields remain in source-isolated quarantine tables.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import re
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from functools import reduce
from pathlib import Path, PurePosixPath
from typing import Any

AUDIT_SCHEMAS = {
    "bronze_batch_manifest": (
        "source_path string, snapshot_hash string, observed_utc timestamp, source_rows long"
    ),
    "bronze_inventory": (
        "source_path string, row_count long, column_count int, status string, message string"
    ),
    "silver_load_log": (
        "source_path string, target_table string, source_rows long, silver_rows long, "
        "quarantine_rows long, status string, message string"
    ),
    "silver_file_manifest": (
        "source_path string, source_file_name string, source_rows long, silver_rows long, "
        "quarantine_rows long, status string"
    ),
    "silver_dq_rule_summary": "dq_rule string, failure_count long",
    "bronze_silver_validation": "check_name string, failed_count long, status string",
    "bronze_cleanup": (
        "source_path string, file_date date, size_bytes long, action string, status string, message string"
    ),
}
AUDIT_COMMON = (
    "source_code string, table_name string, pipeline_run_id string, processing_date date, "
    "contract_hash string, event_utc timestamp"
)
LINEAGE = [
    "ingest_batch_run_id",
    "ingest_processing_date",
    "ingest_extract_date",
    "ingest_loaded_utc",
    "ingest_source_system",
    "ingest_source_file_name",
    "ingest_contract_hash",
]


def _business_rule_handlers() -> dict[str, Any]:
    handlers = globals().get("_MEDALLION_RULE_HANDLERS")
    if handlers is None:
        location = globals().get("__file__")
        if location is None:
            raise RuntimeError("The packaged shared rule module is missing")
        path = Path(location).with_name("rules.py")
        spec = importlib.util.spec_from_file_location("medallion_packaged_rules", path)
        if spec is None or spec.loader is None:
            raise RuntimeError("The packaged shared rule module cannot be resolved")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        handlers = module._MEDALLION_RULE_HANDLERS
        globals()["_MEDALLION_RULE_HANDLERS"] = handlers
    return handlers


def processing_context(
    config: dict[str, Any],
    processing_date: str,
    pipeline_run_id: str,
    batch_started_utc: str = "",
) -> dict[str, Any]:
    """Validate bounded source/date scope before accessing any files or tables."""
    source = config["source_code"]
    bindings = _business_rule_handlers()["legacy_context_bindings"](source)
    for field in ("lakehouse", "bronze_root", "silver_schema", "quarantine_schema"):
        if config[field] != bindings[field]:
            raise ValueError(f"Unexpected historical source {field}")
    if "history_schema" in config:
        raise ValueError("Legacy dual-store contract; regenerate all source notebooks")
    value = processing_date.strip() or datetime.now(UTC).date().isoformat()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("processing_date must use yyyy-MM-dd")
    result = {
        **config,
        "processing_date": date.fromisoformat(value),
        "pipeline_run_id": pipeline_run_id.strip() or str(uuid.uuid4()),
        "batch_started_utc": None,
    }
    if batch_started_utc.strip():
        if not re.fullmatch(
            r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", batch_started_utc
        ):
            raise ValueError(
                "batch_started_utc must be an explicit UTC ISO timestamp ending Z"
            )
        result["batch_started_utc"] = datetime.fromisoformat(batch_started_utc)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", result["pipeline_run_id"]):
        raise ValueError("Invalid pipeline_run_id")
    for table in result["tables"]:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", table["name"]):
            raise ValueError("Invalid source table name")
    return result


def manifest_processing_context(
    context_json: str, expected_context_sha256: str, *, expected_lakehouse: str
) -> dict[str, Any]:
    """Validate a detached manifest/receipt context without relaxing legacy guards."""
    if (
        not isinstance(context_json, str)
        or len(context_json.encode("utf-8")) > 16 * 1024 * 1024
    ):
        raise ValueError("Manifest source context exceeds its supported bound")
    try:
        config = json.loads(context_json)
        canonical = json.dumps(
            config,
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        raise ValueError("Manifest source context must contain finite JSON") from None
    if (
        not isinstance(config, dict)
        or hashlib.sha256(canonical.encode("ascii")).hexdigest()
        != expected_context_sha256
    ):
        raise ValueError(
            "Manifest source context hash differs from its pinned reference"
        )
    fields = {
        "source_code",
        "lakehouse",
        "silver_schema",
        "quarantine_schema",
        "bronze_root",
        "contract_hash",
        "seed_snapshot_date",
        "tables",
        "metadata_version",
        "metadata_run_id",
        "source_feed_id",
        "source_batch_id",
        "source_type",
        "description_slug",
        "observed_utc",
        "processing_date",
        "pipeline_run_id",
        "raw_source_files",
        "raw_source_sizes",
        "raw_targets",
        "source_selection_complete",
        "required_source_tables",
        "selected_tables",
        "bronze_receipt_sha256",
        "source_rule_specs",
    }
    if set(config) != fields:
        raise ValueError(
            "Manifest source context fields differ from the supported contract"
        )
    for name in (
        "source_code",
        "lakehouse",
        "silver_schema",
        "quarantine_schema",
        "description_slug",
    ):
        if not isinstance(config[name], str) or not re.fullmatch(
            r"[a-z][a-z0-9_]{0,127}", config[name]
        ):
            raise ValueError("Invalid source namespace in manifest context")
    if config["lakehouse"] != expected_lakehouse:
        raise ValueError("Manifest source context differs from the worker Lakehouse")
    if config["source_type"] not in {"sql", "ftp", "sftp"}:
        raise ValueError("Invalid source transport in manifest context")
    source = config["source_code"]
    prefix = (
        f"Files/bronze/{source}_{config['description_slug']}_{config['source_type']}"
    )
    if config["bronze_root"] != prefix:
        raise ValueError("Manifest source Bronze prefix differs from its feed")
    if not isinstance(config["metadata_run_id"], str) or not re.fullmatch(
        r"[A-Za-z0-9_-]{1,128}", config["metadata_run_id"]
    ):
        raise ValueError("Invalid metadata run identity")
    for name in ("contract_hash", "source_batch_id", "bronze_receipt_sha256"):
        if not isinstance(config[name], str) or not re.fullmatch(
            r"[0-9a-f]{64}", config[name]
        ):
            raise ValueError("An explicit source/receipt SHA-256 is required")
    expected_batch = hashlib.sha256(
        json.dumps(
            {"feed_id": config["source_feed_id"], "run_id": config["metadata_run_id"]},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    if (
        config["source_batch_id"] != expected_batch
        or config["pipeline_run_id"] != expected_batch
    ):
        raise ValueError(
            "Source batch lineage cannot be replaced by a run or attempt identity"
        )
    if not isinstance(config["observed_utc"], str) or not re.fullmatch(
        r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", config["observed_utc"]
    ):
        raise ValueError("Manifest observation time must be an explicit UTC instant")
    observed = datetime.fromisoformat(config["observed_utc"])
    if config["processing_date"] != observed.date().isoformat():
        raise ValueError(
            "A manifest batch cannot be assigned a different processing date"
        )
    if type(config["source_selection_complete"]) is not bool:
        raise ValueError("Source completeness must be explicitly Boolean")
    tables = config["tables"]
    if not isinstance(tables, list) or not tables:
        raise ValueError("Manifest source requires explicit table contracts")
    names = []
    for table in tables:
        if not isinstance(table, dict) or not re.fullmatch(
            r"[a-z][a-z0-9_]*", str(table.get("name", ""))
        ):
            raise ValueError("Invalid source table contract")
        names.append(table["name"])
        hash_column_order(table)
    _business_rule_handlers()["validate_rule_specs"](
        config["source_rule_specs"], scope="source"
    )
    if len(set(names)) != len(names):
        raise ValueError("Duplicate source table identity")
    required, selected = config["required_source_tables"], config["selected_tables"]
    if (
        not isinstance(required, list)
        or not isinstance(selected, list)
        or any(not isinstance(name, str) for name in [*required, *selected])
        or set(required) != set(names)
        or len(required) != len(set(required))
        or not selected
        or not set(selected) <= set(required)
        or len(selected) != len(set(selected))
        or config["source_selection_complete"] != (set(selected) == set(required))
    ):
        raise ValueError(
            "Manifest source selection/completeness differs from its contracts"
        )
    for table in tables:
        if table["name"] in selected:
            _business_rule_handlers()["validate_rule_specs"](
                table.get("rule_specs"), scope="object"
            )
    mappings = [
        config[name] for name in ("raw_source_files", "raw_source_sizes", "raw_targets")
    ]
    if any(not isinstance(mapping, dict) for mapping in mappings):
        raise ValueError("Manifest raw membership must be structured")
    files, sizes, targets = mappings
    if (
        set(files) != set(sizes)
        or set(files) != set(targets)
        or not set(files) <= set(names)
        or not set(selected) <= set(files)
    ):
        raise ValueError("Manifest raw membership omits or invents source tables")
    batch_prefix = f"{prefix}/{observed:%Y%m%d}/{expected_batch}"
    all_paths = set()
    for name, paths in files.items():
        if (
            not isinstance(paths, list)
            or not 1 <= len(paths) <= 5000
            or not isinstance(sizes[name], list)
            or len(sizes[name]) != len(paths)
        ):
            raise ValueError("Invalid bounded raw-file membership")
        target = targets[name]
        if not isinstance(target, str) or not target.startswith(batch_prefix + "/"):
            raise ValueError("Raw target escapes its pinned source batch")
        for path, size in zip(paths, sizes[name], strict=True):
            if (
                not isinstance(path, str)
                or not re.fullmatch(r"Files/bronze/[A-Za-z0-9_./ -]{1,1950}", path)
                or not path.startswith(batch_prefix + "/")
                or any(part in {"", ".", ".."} for part in path.split("/"))
                or type(size) is not int
                or not 0 <= size <= 2**63 - 1
                or path in all_paths
            ):
                raise ValueError("Invalid or duplicate raw-file membership")
            if config["source_type"] == "sql" and (
                path != target or len(paths) != 1 or size == 0
            ):
                raise ValueError("SQL source requires one complete contracted CSV")
            if config["source_type"] != "sql" and not path.startswith(target + "/"):
                raise ValueError("File membership escapes its configured object folder")
            all_paths.add(path)
    return {
        **config,
        "processing_date": observed.date(),
        "batch_started_utc": observed,
        "metadata_mode": "manifest_v1",
    }


def source_path(context: dict[str, Any], table: dict[str, Any]) -> str:
    if context.get("metadata_mode") == "manifest_v1":
        target = context["raw_targets"].get(table["name"])
        if target is None:
            raise ValueError(
                "No selected immutable Bronze target exists for this table"
            )
        return target
    key = context["processing_date"].strftime("%Y%m%d")
    return f"{context['bronze_root']}/{key}/{context['pipeline_run_id']}/{table['name']}.csv"


def source_files(context: dict[str, Any], table: dict[str, Any]) -> tuple[str, ...]:
    if context.get("metadata_mode") != "manifest_v1":
        return (source_path(context, table),)
    paths = context["raw_source_files"].get(table["name"])
    if not paths:
        raise ValueError("No selected immutable Bronze files exist for this table")
    return tuple(paths)


def table_path(
    context: dict[str, Any], table: dict[str, Any], *, quarantine: bool = False
) -> str:
    schema = context["quarantine_schema" if quarantine else "silver_schema"]
    return f"{context['lakehouse']}.{schema}.{table['name']}"


def _column(name: str):
    from pyspark.sql import functions as F

    return F.col(f"`{name}`")


def _operation_errors():
    """Only expected engine/IO failures are aggregated; programming errors propagate."""
    from py4j.protocol import Py4JError
    from pyspark.errors import PySparkException

    return (OSError, ValueError, RuntimeError, csv.Error, Py4JError, PySparkException)


def _cast(column: dict[str, Any]):
    from pyspark.sql import functions as F

    name = f"`{column['name']}`"
    kind = column["data_type"]
    if kind in {"varchar", "uuid"}:
        return _column(column["name"])
    if kind == "datetime":
        if column.get("format") == "CSD_TIMESTAMP":
            expression = f"regexp_replace({name}, ':([0-9]{{3}})$', '.$1')"
        else:
            expression = name
        return F.expr(f"try_cast({expression} AS timestamp_ntz)")
    target = {
        "int": "int",
        "bigint": "bigint",
        "money": "decimal(19,4)",
        "float": "double",
        "bit": "boolean",
        "boolean": "boolean",
    }.get(kind)
    if kind == "decimal":
        target = f"decimal({column['precision']},{column['scale']})"
    return F.expr(f"try_cast({name} AS {target})")


def _invalid_value(column: dict[str, Any], parsed):
    from pyspark.sql import functions as F

    raw = _column(column["name"])
    kind = column["data_type"]
    invalid = F.lit(False)
    if kind not in {"varchar", "uuid"}:
        invalid = parsed.isNull()
    if kind in {"int", "bigint"}:
        invalid = invalid | ~raw.rlike(r"^-?[0-9]+$")
    elif kind in {"money", "decimal"}:
        scale = column.get("scale", 4 if kind == "money" else 2)
        invalid = invalid | ~raw.rlike(rf"^-?[0-9]+\.[0-9]{{{scale}}}$")
        if kind == "money":
            invalid = invalid | ~parsed.between(
                F.lit(Decimal("-922337203685477.5808")),
                F.lit(Decimal("922337203685477.5807")),
            )
    elif kind == "float":
        invalid = invalid | F.isnan(parsed) | parsed.isin(float("inf"), float("-inf"))
        if "perc" in column["name"]:
            invalid = invalid | ~parsed.between(0, 100)
    elif kind == "bit":
        invalid = invalid | ~raw.isin("0", "1")
    elif kind == "boolean":
        invalid = invalid | ~raw.isin("true", "false")
    elif kind == "uuid":
        invalid = ~raw.rlike(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
    elif kind == "datetime":
        if column.get("format") == "CSD_TIMESTAMP":
            valid_shape = raw.rlike(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d:\d{3}$") | (
                raw == "1900-01-01T00:00:00"
            )
        else:
            valid_shape = raw.rlike(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}$")
        invalid = invalid | ~valid_shape | raw.startswith("0000")
    for key, check in (
        ("max_length", lambda n: F.length(raw) > n),
        ("min_length", lambda n: F.length(raw) < n),
        ("fixed_length", lambda n: F.length(raw) != n),
        ("minimum", lambda n: parsed < F.lit(n).cast(parsed_type(column))),
        ("maximum", lambda n: parsed > F.lit(n).cast(parsed_type(column))),
    ):
        if column.get(key) is not None:
            invalid = invalid | check(column[key])
    if column.get("pattern"):
        invalid = invalid | ~raw.rlike("^(?:" + column["pattern"] + ")$")
    date_format = column.get("format")
    if date_format in {"YYYYMMDD", "YYYYMM"}:
        invalid = invalid | raw.startswith("0000")
    if date_format in {"YYYYMMDD", "YYYYMM", "HHMMSS"}:
        name = f"`{column['name']}`"
        if date_format == "YYYYMMDD":
            normalized = f"concat(substr({name},1,4),'-',substr({name},5,2),'-',substr({name},7,2))"
            parsed_date = F.expr(f"try_cast({normalized} AS date)")
            invalid = invalid | ~raw.rlike(r"^\d{8}$") | parsed_date.isNull()
        elif date_format == "YYYYMM":
            normalized = f"concat(substr({name},1,4),'-',substr({name},5,2),'-01')"
            invalid = (
                invalid
                | ~raw.rlike(r"^\d{6}$")
                | F.expr(f"try_cast({normalized} AS date)").isNull()
            )
        else:
            invalid = invalid | ~raw.rlike(r"^(?:[01]\d|2[0-3])[0-5]\d[0-5]\d$")
    return raw.isNotNull() & F.coalesce(invalid, F.lit(True))


def parsed_type(column: dict[str, Any]) -> str:
    if column["data_type"] == "decimal":
        return f"decimal({column['precision']},{column['scale']})"
    return {
        "money": "decimal(19,4)",
        "float": "double",
        "int": "int",
        "bigint": "bigint",
    }.get(column["data_type"], "string")


def prepare_rows(raw, table: dict[str, Any]):
    """Parse with native Spark expressions and retain raw rejected field values."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    columns = table["columns"]
    names = [c["name"] for c in columns]
    if "_corrupt_record" not in raw.columns:
        raw = raw.withColumn("_corrupt_record", F.lit(None).cast("string"))
    raw = raw.select(
        *[
            F.when(_column(n) == "", F.lit(None)).otherwise(_column(n)).alias(n)
            for n in names
        ],
        "_corrupt_record",
        *([F.col("_source_file_path")] if "_source_file_path" in raw.columns else []),
    )
    reasons = [
        F.when(_column("_corrupt_record").isNotNull(), F.lit("MALFORMED_CSV_RECORD"))
    ]
    expressions = []
    for column in columns:
        name = column["name"]
        parsed = _cast(column)
        expressions.append(
            parsed.alias(
                name,
                metadata={
                    key: column[key]
                    for key in ("classification", "source_type", "unit")
                    if column.get(key) is not None
                },
            )
        )
        if not column["nullable"] or name in table["keys"]:
            reasons.append(
                F.when(_column(name).isNull(), F.lit(f"REQUIRED_FIELD_NULL:{name}"))
            )
        reasons.append(
            F.when(_invalid_value(column, parsed), F.lit(f"INVALID_VALUE:{name}"))
        )
    typed = raw.select(
        *expressions,
        F.struct(*[_column(n) for n in names]).alias("_raw_record"),
        F.col("_corrupt_record").alias("ingest_corrupt_record"),
        F.filter(F.array(*reasons), lambda reason: reason.isNotNull()).alias(
            "_dq_reasons"
        ),
        *([F.col("_source_file_path")] if "_source_file_path" in raw.columns else []),
    )
    duplicates = Window.partitionBy(*[_column(k) for k in table["keys"]])
    typed = typed.withColumn(
        "_dq_reasons",
        F.when(
            F.count(F.lit(1)).over(duplicates) > 1,
            F.array_union("_dq_reasons", F.array(F.lit("DUPLICATE_BUSINESS_KEY"))),
        ).otherwise(F.col("_dq_reasons")),
    )
    row_rules = _business_rule_handlers()["row_rules"](table)
    if row_rules:
        typed = typed.withColumn(
            "_dq_reasons",
            F.array_union(
                "_dq_reasons",
                F.filter(
                    F.array(
                        *[
                            F.when(condition, F.lit(rule))
                            for condition, rule in row_rules
                        ]
                    ),
                    lambda reason: reason.isNotNull(),
                ),
            ),
        )
    if "rule_specs" in table:
        typed = _business_rule_handlers()["apply_rules"](typed, table)
    return typed


def blocking_dq_reasons(table: dict[str, Any]):
    return _business_rule_handlers()["blocking_reasons"](table)


def _audit(
    spark, context: dict[str, Any], name: str, rows: list[dict[str, Any]]
) -> None:
    if not rows:
        return
    common = {
        "source_code": context["source_code"],
        "pipeline_run_id": context["pipeline_run_id"],
        "processing_date": context["processing_date"],
        "contract_hash": context["contract_hash"],
        "event_utc": datetime.now(UTC),
    }
    schema = AUDIT_COMMON + ", " + AUDIT_SCHEMAS[name]
    frame = spark.createDataFrame([{**common, **row} for row in rows], schema=schema)
    frame.write.format("delta").mode("append").saveAsTable(
        f"{context['lakehouse']}.audit.{name}"
    )


def setup_namespaces(spark, context: dict[str, Any]) -> None:
    """Create explicit audit schemas before independent sources can append."""
    lakehouse = context["lakehouse"]
    for schema in (
        context["silver_schema"],
        context["quarantine_schema"],
        "audit",
    ):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {lakehouse}.{schema}")
    for name, fields in AUDIT_SCHEMAS.items():
        target = f"{lakehouse}.audit.{name}"
        ddl = AUDIT_COMMON + ", " + fields
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({ddl}) USING DELTA")
        expected = spark.createDataFrame([], ddl).schema
        actual = spark.table(target).schema
        if [(f.name, f.dataType) for f in expected] != [
            (f.name, f.dataType) for f in actual
        ]:
            raise RuntimeError(
                f"Incompatible audit schema: {target}; do not overwrite it"
            )


def read_raw(spark, fs, context: dict[str, Any], table: dict[str, Any]):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    paths = source_files(context, table)
    names = [c["name"] for c in table["columns"]]
    for index, path in enumerate(paths):
        if not fs.exists(path):
            raise FileNotFoundError(f"Missing immutable extraction batch: {path}")
        if context.get("metadata_mode") == "manifest_v1":
            entries = [
                entry
                for entry in fs.ls(str(PurePosixPath(path).parent))
                if entry.name == PurePosixPath(path).name
            ]
            if (
                len(entries) != 1
                or entries[0].size != context["raw_source_sizes"][table["name"]][index]
            ):
                raise RuntimeError(
                    "Bronze file membership changed after receipt sealing"
                )
        head = fs.head(path, 65536).removeprefix("\ufeff")
        first_line = head.splitlines()[0] if head else ""
        if next(csv.reader([first_line]), []) != names:
            raise ValueError(
                f"{table['name']}: source header/order differs from the contract"
            )
    schema = T.StructType(
        [T.StructField(n, T.StringType(), True) for n in names]
        + [T.StructField("_corrupt_record", T.StringType(), True)]
    )
    frame = (
        spark.read.schema(schema)
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("quote", '"')
        .option("escape", "\\")
        .option("nullValue", "")
        .csv(list(paths) if context.get("metadata_mode") == "manifest_v1" else paths[0])
    )
    if context.get("metadata_mode") == "manifest_v1":
        frame = frame.withColumn("_source_file_path", F.input_file_name())
    return frame


def snapshot_fingerprint(frame) -> tuple[str, int]:
    """Hash a logical record multiset with bounded, distributed hash-word sums."""
    from pyspark.sql import functions as F

    columns = [name for name in frame.columns if name != "_source_file_path"]
    hashed = frame.select(
        F.sha2(
            F.to_json(
                F.struct(*[_column(n) for n in columns]),
                {"ignoreNullFields": "false"},
            ),
            256,
        ).alias("_record_hash")
    )
    summary = hashed.agg(
        F.count("*").alias("rows"),
        *[
            F.sum(
                F.conv(F.substring("_record_hash", start, 16), 16, 10).cast(
                    "decimal(38,0)"
                )
            ).alias(f"word_{start}")
            for start in (1, 17, 33, 49)
        ],
    ).first()
    if summary["rows"] and any(summary[f"word_{n}"] is None for n in (1, 17, 33, 49)):
        raise RuntimeError("Extraction fingerprint aggregation overflow")
    payload = [
        columns,
        summary["rows"],
        *[str(summary[f"word_{n}"]) for n in (1, 17, 33, 49)],
    ]
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":")).encode()
    ).hexdigest(), summary["rows"]


def batch_manifest(spark, context: dict[str, Any], table: dict[str, Any]):
    from pyspark.sql import functions as F

    rows = (
        spark.table(f"{context['lakehouse']}.audit.bronze_batch_manifest")
        .filter(
            (F.col("source_code") == context["source_code"])
            & (F.col("pipeline_run_id") == context["pipeline_run_id"])
            & (F.col("table_name") == table["name"])
        )
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise RuntimeError("Duplicate immutable batch manifest")
    if not rows:
        return None
    result = rows[0].asDict()
    if (
        result["processing_date"] != context["processing_date"]
        or result["contract_hash"] != context["contract_hash"]
        or result["source_path"] != source_path(context, table)
    ):
        raise RuntimeError(
            "A sealed extraction batch cannot be relabelled or re-contracted"
        )
    observed = result["observed_utc"].astimezone(UTC)
    if (
        context["batch_started_utc"] is not None
        and context["batch_started_utc"] != observed
    ):
        raise RuntimeError("A sealed extraction batch timestamp cannot change")
    context["batch_started_utc"] = observed
    return result


def verify_batch(spark, fs, context: dict[str, Any], table: dict[str, Any]):
    manifest = batch_manifest(spark, context, table)
    if manifest is None:
        raise RuntimeError("Inventory must seal the extraction batch before processing")
    raw = read_raw(spark, fs, context, table)
    fingerprint, count = snapshot_fingerprint(raw)
    if fingerprint != manifest["snapshot_hash"] or count != manifest["source_rows"]:
        raise RuntimeError(
            "Sealed Bronze batch content changed; use a new extraction run"
        )
    return raw, manifest


def inventory(
    spark, fs, context: dict[str, Any], *, table_names: list[str] | None = None
) -> None:
    selected = (
        table_names
        if table_names is not None
        else (
            list(context["raw_source_files"])
            if context.get("metadata_mode") == "manifest_v1"
            else [table["name"] for table in context["tables"]]
        )
    )
    if (
        not selected
        or len(set(selected)) != len(selected)
        or not set(selected) <= {table["name"] for table in context["tables"]}
    ):
        raise ValueError("Inventory requires an exact selected table population")
    rows = []
    for table in context["tables"]:
        if table["name"] not in selected:
            continue
        row = {
            "table_name": table["name"],
            "source_path": source_path(context, table),
            "row_count": None,
            "column_count": None,
            "status": "SUCCESS",
            "message": "",
        }
        try:
            fingerprint, row["row_count"] = snapshot_fingerprint(
                read_raw(spark, fs, context, table)
            )
            row["column_count"] = len(table["columns"])
            if not row["row_count"] and not table["allow_empty"]:
                raise ValueError("Empty source snapshot is not allowed")
            manifest = batch_manifest(spark, context, table)
            if manifest is None:
                context["batch_started_utc"] = context[
                    "batch_started_utc"
                ] or datetime.now(UTC)
                _audit(
                    spark,
                    context,
                    "bronze_batch_manifest",
                    [
                        {
                            "table_name": table["name"],
                            "source_path": row["source_path"],
                            "snapshot_hash": fingerprint,
                            "source_rows": row["row_count"],
                            "observed_utc": context["batch_started_utc"],
                        }
                    ],
                )
            elif (
                manifest["snapshot_hash"] != fingerprint
                or manifest["source_rows"] != row["row_count"]
            ):
                raise RuntimeError(
                    "Sealed Bronze batch content changed; use a new extraction run"
                )
        except _operation_errors() as error:
            row.update(
                status="ERROR", message=f"Inventory failed: {type(error).__name__}"
            )
        rows.append(row)
    _audit(spark, context, "bronze_inventory", rows)
    failed = [row["table_name"] for row in rows if row["status"] != "SUCCESS"]
    if failed:
        raise RuntimeError(
            f"Bronze inventory failed for {failed}; inspect source-scoped audit"
        )


def add_foreign_key_rules(
    spark, fs, context: dict[str, Any], table: dict[str, Any], frame
):
    from pyspark.sql import functions as F

    specs = {t["name"]: t for t in context["tables"]}
    for index, relation in enumerate(table["foreign_keys"]):
        parent = specs[relation["table"]]
        parent_specs = {c["name"]: c for c in parent["columns"]}
        keys = (
            read_raw(spark, fs, context, parent)
            .select(
                *[
                    _cast(parent_specs[name]).alias(f"_parent_key_{position}")
                    for position, name in enumerate(relation["target_columns"])
                ]
            )
            .distinct()
            .withColumn("_parent_found", F.lit(True))
        )
        condition = reduce(
            lambda left, right: left & right,
            [
                _column(name).eqNullSafe(F.col(f"_parent_key_{position}"))
                for position, name in enumerate(relation["columns"])
            ],
        )
        frame = (
            frame.join(keys, condition, "left")
            .withColumn(
                "_dq_reasons",
                F.when(
                    F.col("_parent_found").isNull(),
                    F.array_union(
                        "_dq_reasons",
                        F.array(F.lit(f"ORPHAN_FOREIGN_KEY:{index}:{parent['name']}")),
                    ),
                ).otherwise(F.col("_dq_reasons")),
            )
            .drop(
                "_parent_found",
                *[f"_parent_key_{p}" for p in range(len(relation["columns"]))],
            )
        )
    return frame


def _with_lineage(frame, context: dict[str, Any], table: dict[str, Any]):
    from pyspark.sql import functions as F

    return frame.select(
        "*",
        F.lit(context["pipeline_run_id"]).alias("ingest_batch_run_id"),
        F.lit(context["processing_date"]).alias("ingest_processing_date"),
        F.lit(context["processing_date"]).alias("ingest_extract_date"),
        F.current_timestamp().alias("ingest_loaded_utc"),
        F.lit(context["source_code"]).alias("ingest_source_system"),
        (
            F.regexp_extract("_source_file_path", r"([^/\\]+)$", 1)
            if "_source_file_path" in frame.columns
            else F.lit(PurePosixPath(source_path(context, table)).name)
        ).alias("ingest_source_file_name"),
        F.lit(context["contract_hash"]).alias("ingest_contract_hash"),
        (F.size("_dq_reasons") == 0).alias("dq_passed"),
        F.when(F.size("_dq_reasons") > 0, F.concat_ws(";", "_dq_reasons")).alias(
            "dq_failure_reason"
        ),
        (
            F.when(F.size("_dq_warnings") > 0, F.concat_ws(";", "_dq_warnings"))
            if "_dq_warnings" in frame.columns
            else F.lit(None).cast("string")
        ).alias("dq_warning"),
    )


def hash_column_order(table: dict[str, Any]) -> tuple[list[str], list[str]]:
    observations = set(table.get("change_hash_exclusions", []))
    tracked = [c["name"] for c in table["columns"] if c["name"] not in observations]
    keys = table["keys"]
    policy = table.get("hash_policy")
    if policy is not None:
        if (
            not isinstance(policy, dict)
            or set(policy) != {"key_columns", "value_columns", "encoding"}
            or policy["encoding"] != "spark_json_struct_sha256_v1"
            or policy["key_columns"] != keys
            or not isinstance(policy["value_columns"], list)
            or any(not isinstance(name, str) for name in policy["value_columns"])
            or len(policy["value_columns"]) != len(set(policy["value_columns"]))
            or set(policy["value_columns"]) != set(tracked)
        ):
            raise ValueError(
                "Stable business hash policy differs from the table contract"
            )
        tracked = policy["value_columns"]
    if (
        not keys
        or len(keys) != len(set(keys))
        or not set(keys) <= {column["name"] for column in table["columns"]}
    ):
        raise ValueError("Business hash keys differ from the column contract")
    return list(keys), list(tracked)


def with_record_hashes(frame, table: dict[str, Any]):
    from pyspark.sql import functions as F

    keys, tracked = hash_column_order(table)
    return frame.select(
        "*",
        F.sha2(
            F.to_json(
                F.struct(*[_column(k) for k in keys]),
                {"ignoreNullFields": "false"},
            ),
            256,
        ).alias("_record_key_hash"),
        F.sha2(
            F.to_json(
                F.struct(*[_column(n) for n in tracked]), {"ignoreNullFields": "false"}
            ),
            256,
        ).alias("_record_hash"),
    )


def _record_join(table: dict[str, Any], prefix: str):
    from pyspark.sql import functions as F

    return reduce(
        lambda a, b: a & b,
        [
            _column(key).eqNullSafe(F.col(f"{prefix}{i}"))
            for i, key in enumerate(table["keys"])
        ],
    )


def eligible_silver(
    spark,
    context: dict[str, Any],
    table: dict[str, Any],
    *,
    include_current: bool = False,
    exclude_current: bool = False,
):
    """Return only committed versions, optionally including this staged batch."""
    from pyspark.sql import functions as F

    commits = (
        spark.table(f"{context['lakehouse']}.audit.bronze_silver_validation")
        .filter(
            (F.col("source_code") == context["source_code"])
            & (F.col("check_name") == "source_batch_committed")
            & (F.col("status") == "SUCCESS")
        )
        .select(
            F.col("pipeline_run_id").alias("_commit_run"),
            F.col("processing_date").alias("_commit_date"),
            F.col("contract_hash").alias("_commit_contract"),
        )
        .distinct()
        .withColumn("_committed", F.lit(True))
    )
    records = spark.table(table_path(context, table))
    current = (
        (F.col("ingest_batch_run_id") == context["pipeline_run_id"])
        & (F.col("ingest_processing_date") == F.lit(context["processing_date"]))
        & (F.col("ingest_contract_hash") == context["contract_hash"])
    )
    condition = (
        (F.col("ingest_batch_run_id") == F.col("_commit_run"))
        & (F.col("ingest_processing_date") == F.col("_commit_date"))
        & (F.col("ingest_contract_hash") == F.col("_commit_contract"))
    )
    eligible = F.coalesce(F.col("_committed"), F.lit(False))
    if include_current:
        eligible = eligible | current
    if exclude_current:
        eligible = eligible & ~current
    return (
        records.join(commits, condition, "left")
        .filter(eligible)
        .drop("_commit_run", "_commit_date", "_commit_contract", "_committed")
    )


def latest_silver(
    spark,
    context: dict[str, Any],
    table: dict[str, Any],
    *,
    include_current: bool = False,
    exclude_current: bool = False,
):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    return (
        eligible_silver(
            spark,
            context,
            table,
            include_current=include_current,
            exclude_current=exclude_current,
        )
        .withColumn(
            "_record_latest",
            F.row_number().over(
                Window.partitionBy(*table["keys"]).orderBy(
                    F.col("record_version").desc()
                )
            ),
        )
        .filter("_record_latest = 1")
        .drop("_record_latest")
    )


def silver_versions(
    frame, context: dict[str, Any], table: dict[str, Any], columns: list[str]
):
    from pyspark.sql import functions as F

    change_type = F.when(F.col("_previous_hash").isNull(), "INSERT").otherwise("UPDATE")
    event_id = F.sha2(
        F.to_json(
            F.struct(
                F.lit(context["source_code"]).alias("source"),
                F.lit(table["name"]).alias("table"),
                F.col("ingest_batch_run_id"),
                F.col("_record_key_hash"),
            )
        ),
        256,
    )
    return frame.select(
        *[_column(n) for n in columns],
        event_id.alias("record_id"),
        F.col("_record_key_hash").alias("record_key_hash"),
        F.col("_record_hash").alias("record_hash"),
        F.col("_previous_hash").alias("record_previous_hash"),
        (F.coalesce(F.col("_max_version"), F.lit(0)) + 1)
        .cast("long")
        .alias("record_version"),
        change_type.alias("record_change_type"),
        F.lit(context["batch_started_utc"]).alias("record_observed_utc"),
        F.lit(source_path(context, table)).alias("record_source_path"),
        F.lit(context["snapshot_hash"]).alias("record_batch_hash"),
    )


def delta_table(spark, target: str):
    """Resolve multipart catalog names through Spark rather than Delta's two-part parser."""
    from delta.tables import DeltaTable

    location = (
        spark.sql(f"DESCRIBE DETAIL {target}").select("location").first()["location"]
    )
    return DeltaTable.forPath(spark, location)


def delta_version(spark, target: str) -> int:
    return int(
        delta_table(spark, target).history(1).select("version").first()["version"]
    )


def append_silver(spark, context: dict[str, Any], table: dict[str, Any], valid) -> None:
    """Append changed business versions directly to the sole source Silver table."""
    from pyspark.sql import functions as F

    target = table_path(context, table)
    if spark.catalog.tableExists(target):
        physical = [
            field.name
            for field in spark.table(target).schema
            if field.name in valid.columns
        ]
        if set(physical) != set(valid.columns):
            raise RuntimeError(
                "Silver source fields changed; an explicit compatible migration is required"
            )
        valid = valid.select(*[_column(name) for name in physical])
    hashed = with_record_hashes(valid, table)
    empty = silver_versions(
        hashed.limit(0)
        .withColumn("_previous_hash", F.lit(None).cast("string"))
        .withColumn("_max_version", F.lit(0).cast("long")),
        context,
        table,
        valid.columns,
    )
    if context.get("metadata_mode") == "manifest_v1":
        layout_runtime = globals().get("_MEDALLION_LAYOUT")
        if layout_runtime is None:
            raise RuntimeError(
                "Generic Silver requires the packaged physical layout adapter"
            )
        settings = table.get("runtime_layout", {})
        capability = settings.get("capabilities")
        capability = (
            layout_runtime.RuntimeCapabilities(**capability)
            if capability is not None
            else None
        )
        existed = spark.catalog.tableExists(target)
        layout_runtime.ensure_target_layout(
            spark,
            target,
            empty,
            settings.get("layout"),
            existing_target=existed,
            capabilities=capability,
        )
        if not existed:
            spark.sql(
                f"ALTER TABLE {layout_runtime.quote_table(target)} SET TBLPROPERTIES "
                "('delta.appendOnly'='true','delta.enableChangeDataFeed'='true')"
            )
    if not spark.catalog.tableExists(target):
        empty.write.format("delta").mode("errorifexists").option(
            "delta.appendOnly", "true"
        ).option("delta.enableChangeDataFeed", "true").saveAsTable(target)
    if [(f.name, f.dataType) for f in spark.table(target).schema] != [
        (f.name, f.dataType) for f in empty.schema
    ]:
        raise RuntimeError(
            f"Silver version schema mismatch: {target}; explicit non-destructive migration required"
        )
    silver = delta_table(spark, target)
    if (
        silver.detail()
        .select("properties")
        .first()["properties"]
        .get("delta.appendOnly")
        != "true"
    ):
        raise RuntimeError(
            "Silver must be an append-only Delta table; explicit migration required"
        )
    previous = latest_silver(spark, context, table, exclude_current=True).select(
        *[_column(k).alias(f"_previous_key_{i}") for i, k in enumerate(table["keys"])],
        F.col("record_hash").alias("_previous_hash"),
    )
    versions = (
        spark.table(target)
        .groupBy(*table["keys"])
        .agg(F.max("record_version").alias("_max_version"))
        .select(
            *[
                _column(k).alias(f"_version_key_{i}")
                for i, k in enumerate(table["keys"])
            ],
            "_max_version",
        )
    )
    changes = (
        hashed.join(previous, _record_join(table, "_previous_key_"), "left")
        .join(versions, _record_join(table, "_version_key_"), "left")
        .filter(~F.col("_record_hash").eqNullSafe(F.col("_previous_hash")))
    )
    events = silver_versions(changes, context, table, valid.columns)
    if events.limit(1).count():
        silver.alias("tgt").merge(
            events.alias("src"), "tgt.record_id = src.record_id"
        ).whenNotMatchedInsertAll().execute()


def guard_batch_order(spark, context: dict[str, Any], table: dict[str, Any]) -> None:
    from pyspark.sql import functions as F

    newer = spark.table(f"{context['lakehouse']}.audit.bronze_batch_manifest").filter(
        (F.col("source_code") == context["source_code"])
        & (F.col("table_name") == table["name"])
        & (F.col("pipeline_run_id") != context["pipeline_run_id"])
        & (F.col("observed_utc") >= F.lit(context["batch_started_utc"]))
    )
    if newer.limit(1).count():
        raise RuntimeError(
            "An older or concurrent batch cannot overwrite newer source state"
        )


def reconcile_quarantine(
    spark,
    context: dict[str, Any],
    table: dict[str, Any],
    rejected,
    *,
    write_missing: bool,
) -> None:
    """Reconcile the exact rejected-row multiset for this source batch."""
    from pyspark.sql import functions as F

    raw_rejects = rejected.select(
        *[
            F.col(f"_raw_record.`{column['name']}`").alias(column["name"])
            for column in table["columns"]
        ],
        *LINEAGE,
        "ingest_corrupt_record",
        "dq_passed",
        "dq_failure_reason",
        "dq_warning",
    )
    target = table_path(context, table, quarantine=True)
    exists = spark.catalog.tableExists(target)
    if not exists:
        if write_missing:
            if raw_rejects.limit(1).count():
                raw_rejects.write.format("delta").mode("errorifexists").saveAsTable(
                    target
                )
        elif raw_rejects.limit(1).count():
            raise RuntimeError("Quarantine evidence is missing for the source batch")
        return

    existing = spark.table(target)
    expected_schema = {field.name: field.dataType for field in raw_rejects.schema}
    actual_schema = {field.name: field.dataType for field in existing.schema}
    if expected_schema != actual_schema:
        raise RuntimeError(
            "Quarantine schema differs from the source contract; migration is required"
        )
    comparable = [name for name in raw_rejects.columns if name != "ingest_loaded_utc"]
    expected = raw_rejects.select(*comparable)
    retained = existing.filter(
        F.col("ingest_batch_run_id") == context["pipeline_run_id"]
    ).select(*comparable)
    if retained.exceptAll(expected).limit(1).count():
        raise RuntimeError("Quarantine evidence differs from the source batch")
    missing = expected.exceptAll(retained)
    if missing.limit(1).count():
        if not write_missing:
            raise RuntimeError("Quarantine evidence is missing for the source batch")
        (
            missing.withColumn("ingest_loaded_utc", F.current_timestamp())
            .select(*existing.columns)
            .write.format("delta")
            .mode("append")
            .saveAsTable(target)
        )


def process_table(
    spark, fs, context: dict[str, Any], table: dict[str, Any], progress: dict[str, Any]
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    raw, manifest = verify_batch(spark, fs, context, table)
    guard_batch_order(spark, context, table)
    context["snapshot_hash"] = manifest["snapshot_hash"]
    frame = prepare_rows(raw, table)
    frame = add_foreign_key_rules(spark, fs, context, table, frame)
    frame = _with_lineage(frame, context, table).cache()
    try:
        source_rows = frame.count()
        if not source_rows and not table["allow_empty"]:
            raise ValueError("Unexpected empty snapshot")
        rejected = frame.filter(~F.col("dq_passed"))
        quarantine_rows = rejected.count()
        blocking_rows = frame.filter(F.size(blocking_dq_reasons(table)) > 0).count()
        progress.update(
            source_rows=source_rows,
            silver_rows=source_rows - quarantine_rows,
            quarantine_rows=quarantine_rows,
        )
        valid = frame.filter("dq_passed").drop(
            "_raw_record",
            "_dq_reasons",
            "ingest_corrupt_record",
            "_source_file_path",
            "_dq_warnings",
        )
        append_silver(spark, context, table, valid)
        reconcile_quarantine(spark, context, table, rejected, write_missing=True)
        has_warning_rules = any(
            rule["action"] == "warn" for rule in table.get("rule_specs", [])
        )
        if quarantine_rows or has_warning_rules:
            # Escape reserved prefixes only in summaries, not retained rejection reasons.
            reasons = F.transform(
                F.col("_dq_reasons"),
                lambda reason: F.when(
                    reason.startswith("warning:") | reason.startswith("failure:"),
                    F.concat(F.lit("failure:"), reason),
                ).otherwise(reason),
            )
            if has_warning_rules:
                warnings = F.transform(
                    F.col("_dq_warnings"),
                    lambda warning: F.concat(F.lit("warning:"), warning),
                )
                reasons = F.concat(reasons, warnings)
            counts = (
                frame.select(F.explode(reasons).alias("dq_rule"))
                .groupBy("dq_rule")
                .count()
            )
            _audit(
                spark,
                context,
                "silver_dq_rule_summary",
                [
                    {
                        "table_name": table["name"],
                        "dq_rule": row["dq_rule"],
                        "failure_count": row["count"],
                    }
                    for row in counts.toLocalIterator()
                ],
            )
        progress.update(
            status="SUCCESS" if blocking_rows == 0 else "QUARANTINED", message=""
        )
        return progress
    finally:
        frame.unpersist()


def load_silver(
    spark, fs, context: dict[str, Any], *, table_names: list[str] | None = None
) -> list[dict[str, Any]]:
    selected = (
        table_names
        if table_names is not None
        else (
            context["selected_tables"]
            if context.get("metadata_mode") == "manifest_v1"
            else [table["name"] for table in context["tables"]]
        )
    )
    if (
        not isinstance(selected, list)
        or not selected
        or any(not isinstance(name, str) for name in selected)
        or len(selected) != len(set(selected))
        or not set(selected) <= {table["name"] for table in context["tables"]}
    ):
        raise ValueError("Object-scoped Silver selection is missing or ambiguous")
    results = []
    for table in context["tables"]:
        if table["name"] not in selected:
            continue
        result = {
            "table_name": table["name"],
            "source_path": source_path(context, table),
            "target_table": table_path(context, table),
            "source_rows": None,
            "silver_rows": None,
            "quarantine_rows": None,
            "status": "ERROR",
            "message": "",
        }
        try:
            result = process_table(spark, fs, context, table, result)
        except _operation_errors() as error:
            result.update(
                status="ERROR",
                message=f"Silver processing failed: {type(error).__name__}",
            )
        results.append(result)
        _audit(spark, context, "silver_load_log", [result])
        _audit(
            spark,
            context,
            "silver_file_manifest",
            [
                {
                    key: result[key]
                    for key in (
                        "table_name",
                        "source_path",
                        "source_rows",
                        "silver_rows",
                        "quarantine_rows",
                        "status",
                    )
                }
                | {"source_file_name": PurePosixPath(result["source_path"]).name}
            ],
        )
    failed = [r["table_name"] for r in results if r["status"] != "SUCCESS"]
    if failed:
        raise RuntimeError(
            f"Silver failed for {failed}; Bronze retained; inspect audit/quarantine"
        )
    print(
        json.dumps(
            {
                "source": context["source_code"],
                "tables": len(results),
                "accepted_rows": sum(r["silver_rows"] for r in results),
            }
        )
    )
    return results


def _run_audit(spark, context: dict[str, Any], name: str):
    from pyspark.sql import functions as F

    return spark.table(f"{context['lakehouse']}.audit.{name}").filter(
        (F.col("source_code") == context["source_code"])
        & (F.col("pipeline_run_id") == context["pipeline_run_id"])
        & (F.col("processing_date") == F.lit(context["processing_date"]))
        & (F.col("contract_hash") == context["contract_hash"])
    )


def source_commit_evidence(spark, context: dict[str, Any]) -> dict[str, Any]:
    """Pin one authoritative source commit to the audit Delta version read."""
    from pyspark.sql import functions as F

    target = f"{context['lakehouse']}.audit.bronze_silver_validation"
    version = delta_version(spark, target)
    frame = spark.read.format("delta").option("versionAsOf", version).table(target)
    markers = (
        frame.filter(
            (F.col("source_code") == context["source_code"])
            & (F.col("pipeline_run_id") == context["pipeline_run_id"])
            & (F.col("processing_date") == F.lit(context["processing_date"]))
            & (F.col("contract_hash") == context["contract_hash"])
            & (F.col("check_name") == "source_batch_committed")
            & (F.col("status") == "SUCCESS")
            & (F.col("failed_count") == 0)
        )
        .limit(2)
        .collect()
    )
    if len(markers) != 1:
        raise RuntimeError("Exactly one authoritative source commit is required")
    proof = {
        "location": target,
        "version": version,
        "source_code": context["source_code"],
        "source_batch_id": context["pipeline_run_id"],
        "contract_hash": context["contract_hash"],
        "processing_date": context["processing_date"].isoformat(),
    }
    return {
        "kind": "source_commit",
        "location": target,
        "version": version,
        "sha256": hashlib.sha256(
            json.dumps(
                proof, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            ).encode("ascii")
        ).hexdigest(),
    }


def source_business_checks(data: dict[str, Any], source: str):
    return _business_rule_handlers()["source_business_checks"](data, source)


def relationship_checks(spark, context: dict[str, Any], table: dict[str, Any], frame):
    """Validate retained current-state relationships as well as incoming rows."""
    from pyspark.sql import functions as F

    checks = []
    for index, relation in enumerate(table["foreign_keys"]):
        parent = next(t for t in context["tables"] if t["name"] == relation["table"])
        parent_frame = latest_silver(spark, context, parent, include_current=True)
        duplicates = (
            parent_frame.groupBy(*relation["target_columns"])
            .count()
            .filter("count > 1")
            .count()
        )
        parent_keys = parent_frame.select(
            *[
                _column(key).alias(f"_parent_key_{position}")
                for position, key in enumerate(relation["target_columns"])
            ]
        ).distinct()
        condition = reduce(
            lambda left, right: left & right,
            [
                _column(key).eqNullSafe(F.col(f"_parent_key_{position}"))
                for position, key in enumerate(relation["columns"])
            ],
        )
        checks.append((f"foreign_key_{index}_parent_unique", duplicates))
        checks.append(
            (
                f"foreign_key_{index}_resolved",
                frame.join(parent_keys, condition, "left_anti").count(),
            )
        )
    return checks


def silver_checks(spark, context: dict[str, Any], table: dict[str, Any]):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    records = spark.table(table_path(context, table))
    window = Window.partitionBy(*table["keys"]).orderBy("record_version")
    chain = eligible_silver(spark, context, table, include_current=True).withColumn(
        "_prior_hash", F.lag("record_hash").over(window)
    )
    valid_chain = F.when(
        F.col("_prior_hash").isNull(),
        F.col("record_previous_hash").isNull()
        & (F.col("record_change_type") == "INSERT"),
    ).otherwise(
        F.col("record_previous_hash").eqNullSafe(F.col("_prior_hash"))
        & (F.col("record_change_type") == "UPDATE")
        & ~F.col("record_hash").eqNullSafe(F.col("_prior_hash"))
    )
    hashed = with_record_hashes(records, table)
    required = [
        *LINEAGE,
        *table["keys"],
        "record_id",
        "record_key_hash",
        "record_hash",
        "record_version",
        "record_change_type",
        "record_observed_utc",
        "record_source_path",
        "record_batch_hash",
    ]
    invalid_hash = (
        ~F.col("record_key_hash").eqNullSafe(F.col("_record_key_hash"))
        | ~F.col("record_hash").eqNullSafe(F.col("_record_hash"))
        | reduce(lambda a, b: a | b, [_column(n).isNull() for n in required])
        | (F.col("record_version") <= 0)
    )
    return [
        (
            "record_id_uniqueness",
            records.groupBy("record_id").count().filter("count > 1").count(),
        ),
        (
            "record_version_uniqueness",
            records.groupBy(*table["keys"], "record_version")
            .count()
            .filter("count > 1")
            .count(),
        ),
        (
            "record_version_chain",
            chain.filter(~F.coalesce(valid_chain, F.lit(False))).count(),
        ),
        ("record_hash_integrity", hashed.filter(invalid_hash).count()),
    ]


def validate_source(spark, fs, context: dict[str, Any]) -> None:
    if (
        context.get("metadata_mode") == "manifest_v1"
        and context["source_selection_complete"] is not True
    ):
        raise RuntimeError(
            "Source commit requires a complete metadata source selection"
        )
    if context.get("metadata_mode") == "manifest_v1":
        _business_rule_handlers()["validate_rule_specs"](
            context["source_rule_specs"], scope="source"
        )
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    rows = []
    data = {}
    logs = {
        row["table_name"]: row.asDict()
        for row in _run_audit(spark, context, "silver_load_log")
        .withColumn(
            "_latest",
            F.row_number().over(
                Window.partitionBy("table_name").orderBy(F.col("event_utc").desc())
            ),
        )
        .filter("_latest = 1")
        .collect()
    }
    for table in context["tables"]:
        name = table["name"]
        try:
            raw, _ = verify_batch(spark, fs, context, table)
            guard_batch_order(spark, context, table)
            frame = latest_silver(spark, context, table, include_current=True)
            log = logs.get(name)
            if log is None or log["status"] != "SUCCESS":
                raise RuntimeError("No successful current source/table load")
            checks = [
                (
                    "key_uniqueness",
                    frame.groupBy(*table["keys"]).count().filter("count > 1").count(),
                ),
                (
                    "load_reconciliation",
                    int(
                        log["source_rows"]
                        != log["silver_rows"] + log["quarantine_rows"]
                    ),
                ),
            ]
            required = [*LINEAGE, *table["keys"]]
            nulls = reduce(lambda a, b: a | b, [_column(n).isNull() for n in required])
            checks.append(("required_lineage_keys", frame.filter(nulls).count()))
            checks.append(
                (
                    "passing_dq_only",
                    frame.filter(
                        ~F.coalesce(_column("dq_passed"), F.lit(False))
                    ).count(),
                )
            )
            prepared = add_foreign_key_rules(
                spark, fs, context, table, prepare_rows(raw, table)
            )
            checks.append(
                (
                    "blocking_rule_violations",
                    prepared.filter(F.size(blocking_dq_reasons(table)) > 0).count(),
                )
            )
            rejected = _with_lineage(prepared, context, table).filter(
                ~F.col("dq_passed")
            )
            reconcile_quarantine(spark, context, table, rejected, write_missing=False)
            expected = prepared.filter(F.size("_dq_reasons") == 0)
            data[name] = expected
            checks.append(
                ("accepted_batch_count", abs(expected.count() - log["silver_rows"]))
            )
            expected_hashes = with_record_hashes(expected, table).select(
                "_record_key_hash", "_record_hash"
            )
            actual_hashes = with_record_hashes(frame, table).select(
                "_record_key_hash", "_record_hash"
            )
            checks.append(
                (
                    "source_business_value_equivalence",
                    expected_hashes.join(
                        actual_hashes, ["_record_key_hash", "_record_hash"], "left_anti"
                    ).count(),
                )
            )
            checks.extend(relationship_checks(spark, context, table, frame))
            checks.extend(silver_checks(spark, context, table))
            for check, failed in checks:
                rows.append(
                    {
                        "table_name": name,
                        "check_name": check,
                        "failed_count": failed,
                        "status": "SUCCESS" if failed == 0 else "FAILED",
                    }
                )
        except _operation_errors() as error:
            rows.append(
                {
                    "table_name": name,
                    "check_name": f"table_validation_{type(error).__name__}",
                    "failed_count": 1,
                    "status": "FAILED",
                }
            )
    if len(data) == len(context["tables"]):
        try:
            for check, failed in source_business_checks(data, context["source_code"]):
                rows.append(
                    {
                        "table_name": "",
                        "check_name": check,
                        "failed_count": failed,
                        "status": "SUCCESS" if failed == 0 else "FAILED",
                    }
                )
        except _operation_errors() as error:
            rows.append(
                {
                    "table_name": "",
                    "check_name": f"source_rules_{type(error).__name__}",
                    "failed_count": 1,
                    "status": "FAILED",
                }
            )
    ready = bool(rows) and all(row["status"] == "SUCCESS" for row in rows)
    if ready:
        existing = (
            _run_audit(spark, context, "bronze_silver_validation")
            .filter(
                (F.col("check_name") == "source_batch_committed")
                & (F.col("status") == "SUCCESS")
            )
            .limit(2)
            .count()
        )
        if existing > 1:
            raise RuntimeError(
                "Source commit membership is duplicated; reconcile before replay"
            )
        if existing == 0:
            rows.append(
                {
                    "table_name": "",
                    "check_name": "source_batch_committed",
                    "failed_count": 0,
                    "status": "SUCCESS",
                }
            )
    rows.append(
        {
            "table_name": "",
            "check_name": "source_ready_for_cleanup",
            "failed_count": 0 if ready else 1,
            "status": "SUCCESS" if ready else "FAILED",
        }
    )
    _audit(spark, context, "bronze_silver_validation", rows)
    if not ready:
        failed = [r["check_name"] for r in rows if r["status"] != "SUCCESS"]
        raise RuntimeError(f"Source validation failed: {failed}; Bronze retained")
    print(
        json.dumps(
            {
                "source": context["source_code"],
                "validation": "SUCCESS",
                "checks": len(rows),
            }
        )
    )


def cleanup_snapshots(spark, fs, context: dict[str, Any]) -> None:
    """Keep the legacy activity name, but never delete immutable Bronze history."""
    from pyspark.sql import functions as F

    validation = (
        _run_audit(spark, context, "bronze_silver_validation")
        .filter("check_name = 'source_ready_for_cleanup'")
        .orderBy(F.col("event_utc").desc())
        .first()
    )
    if validation is None or validation["status"] != "SUCCESS":
        raise RuntimeError(
            "Cleanup requires successful validation of this source/run/contract"
        )
    for name in ("silver_load_log", "bronze_inventory"):
        latest = (
            _run_audit(spark, context, name)
            .agg(F.max("event_utc").alias("latest"))
            .first()["latest"]
        )
        if latest is None or latest > validation["event_utc"]:
            raise RuntimeError(
                "Source processing changed after validation; cleanup is not authorized"
            )
    rows = []
    for table in context["tables"]:
        row = {
            "table_name": table["name"],
            "source_path": source_path(context, table),
            "file_date": context["processing_date"],
            "size_bytes": None,
            "action": "RETAIN",
            "status": "RETAINED_IMMUTABLE",
            "message": "No automatic Bronze deletion; retention requires a separately approved policy",
        }
        try:
            verify_batch(spark, fs, context, table)
            guard_batch_order(spark, context, table)
            if any(failed for _, failed in silver_checks(spark, context, table)):
                raise RuntimeError(
                    "Silver version verification failed; batch must remain retained"
                )
        except _operation_errors() as error:
            row.update(
                status="ERROR",
                message=f"Retention verification failed: {type(error).__name__}",
            )
        rows.append(row)
    _audit(spark, context, "bronze_cleanup", rows)
    if any(r["status"] == "ERROR" for r in rows):
        raise RuntimeError(
            "Retention verification failed; no cleanup or deletion performed"
        )


def run_step(
    spark,
    notebookutils,
    config: dict[str, Any],
    step: str,
    *,
    processing_date: str = "",
    pipeline_run_id: str = "",
    batch_started_utc: str = "",
) -> None:
    """Notebook entry point with source-scoped side effects and explicit failures."""
    if step == "cleanup" and not processing_date.strip():
        raise ValueError("Cleanup requires an explicit processing_date")
    context = processing_context(
        config, processing_date, pipeline_run_id, batch_started_utc
    )
    attached = notebookutils.runtime.context.get("defaultLakehouseName")
    if attached != config["lakehouse"]:
        raise RuntimeError(
            f"Attach {config['lakehouse']} as the default Lakehouse before execution"
        )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if step == "setup":
        setup_namespaces(spark, context)
    elif step == "inventory":
        inventory(spark, notebookutils.fs, context)
    elif step == "silver":
        load_silver(spark, notebookutils.fs, context)
    elif step == "validation":
        validate_source(spark, notebookutils.fs, context)
    elif step == "cleanup":
        cleanup_snapshots(spark, notebookutils.fs, context)
    else:
        raise ValueError(f"Unknown notebook step: {step}")
