#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Source-backed Gold runtime embedded in the generated Fabric notebooks.

The shipped contracts are synthetic-demo contracts, not production key,
privacy, accounting-sign or business-publication certification.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import UTC, date, datetime
from functools import reduce
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any, TypedDict

MAX_GOLD_MANIFEST_BYTES = 16 * 1024 * 1024
GOLD_TASK_SCHEMA = (
    "gold_release_id string, input_sha256 string, object_name string, task_id string, "
    "attempt_id string, status string, result_json string, event_utc timestamp"
)


class GoldTableReference(TypedDict):
    table: str
    location: str
    version: int


class GoldPreparationReference(GoldTableReference):
    gold_release_id: str
    input_sha256: str


class GoldResultReference(GoldTableReference):
    gold_release_id: str
    task_id: str
    attempt_id: str
    result_sha256: str
    fence: int
    job_id: str


class RegisteredGoldTransformation(TypedDict):
    """A reviewed, packaged adapter; metadata cannot construct the callable."""

    kind: str
    version: str
    definition_sha256: str
    implementation_sha256: str
    approval_id: str
    execute: Callable[[dict[str, Any]], Any]


class GoldAttempt(TypedDict):
    attempt_id: str
    fence: int
    job_id: str


def _gold_json(value: Any) -> str:
    text = json.dumps(
        value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    )
    if len(text.encode("ascii")) > MAX_GOLD_MANIFEST_BYTES:
        raise ValueError("Gold metadata exceeds its supported manifest bound")
    return text


def _gold_digest(value: Any) -> str:
    return hashlib.sha256(_gold_json(value).encode("ascii")).hexdigest()


def _gold_token(value: Any, label: str, pattern: str = r"[a-z][a-z0-9_]{0,127}") -> str:
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        raise ValueError(f"Invalid {label}")
    return value


def _gold_reference(reference: Mapping[str, Any], expected_table: str) -> None:
    if (
        not isinstance(reference, Mapping)
        or reference.get("table") != expected_table
        or type(reference.get("version")) is not int
        or reference["version"] < 0
        or not isinstance(reference.get("location"), str)
        or not 1 <= len(reference["location"]) <= 4096
        or "?" in reference["location"]
        or "#" in reference["location"]
    ):
        raise RuntimeError("Invalid or rebound pinned Gold table reference")
    for name in expected_table.split("."):
        _gold_token(name, "pinned table namespace")
    if len(expected_table.split(".")) != 3:
        raise RuntimeError("Pinned Gold references require a three-part table name")


def gold_config_from_manifest(
    manifest: Mapping[str, Any],
    *,
    snapshot_sha256: str,
    code_sha256: str,
    worker_id: str = "gold-worker",
    lakehouse: str | None = None,
) -> dict[str, Any]:
    """Reconstruct Gold solely from an executable pinned run snapshot.

    Call after the shared ``read_run_manifest`` boundary. No source registry,
    seed file, active-version pointer, query execution or approval is performed
    here. The bootstrap's legacy mapping hash remains a business-rule reference;
    worker/configuration/task identities are recorded separately.
    """
    document = json.loads(_gold_json(manifest))
    _gold_token(snapshot_sha256, "run snapshot hash", r"[0-9a-f]{64}")
    if _gold_digest(document) != snapshot_sha256:
        raise RuntimeError("Gold run snapshot hash differs from its pinned reference")
    if (
        document.get("format_version") != 1
        or document.get("can_execute") is not True
        or document.get("blockers") != []
    ):
        raise RuntimeError("A preview or blocked manifest cannot execute Gold")
    configuration = document["configuration"]
    if (
        _gold_digest(configuration) != document.get("definition_sha256")
        or configuration["version_id"] != document["version_id"]
    ):
        raise RuntimeError("Gold configuration differs from the pinned run version")
    extension = configuration["extensions"]
    base = deepcopy(extension["gold_contract"])
    if lakehouse is None:
        # Pure compatibility callers derive from the pinned contract; deployed workers
        # always pass the actual attached Lakehouse and validate it below.
        lakehouse = base["target_lakehouse"]
    if (
        extension.get("synthetic_only") is not True
        or base.get("execution_mode") != "synthetic_demo"
    ):
        raise ValueError("Production Gold remains gated; metadata is not certification")
    if base["target_lakehouse"] != lakehouse:
        raise RuntimeError("Gold metadata differs from the parent Lakehouse binding")
    _gold_token(base["schema"], "Gold schema")
    _gold_token(lakehouse, "Gold Lakehouse")
    workers = [w for w in configuration["workers"] if w["id"] == worker_id]
    if (
        len(workers) != 1
        or workers[0]["lakehouse"] != lakehouse
        or workers[0]["code_sha256"] != code_sha256
    ):
        raise RuntimeError(
            "Gold worker code or Lakehouse differs from the pinned version"
        )
    _gold_token(code_sha256, "Gold worker hash", r"[0-9a-f]{64}")
    transforms = {}
    for transform in configuration["transformations"]:
        if (
            transform["object_id"] in transforms
            or _gold_digest(transform["definition"]) != transform["definition_sha256"]
        ):
            raise RuntimeError(
                "Gold transformation is ambiguous or its definition hash changed"
            )
        transforms[transform["object_id"]] = transform
    objects = {obj["id"]: obj for obj in configuration["objects"]}
    tasks = document["tasks"]
    if len({task["task_id"] for task in tasks}) != len(tasks):
        raise RuntimeError("Duplicate pinned Gold task identity")
    by_task = {task["task_id"]: task for task in tasks}
    selected = [task for task in tasks if task["layer"] == "gold"]
    if not selected:
        raise RuntimeError("No Gold work was selected")
    # The shipped release is complete, not a partial dimension/fact publication.
    membership = {
        *(spec["name"] for spec in base["dimensions"]),
        *(spec["name"] for spec in base["facts"]),
        "dim_date",
        "dim_fiscal_period",
    }
    if {objects[task["object_id"]]["name"] for task in selected} != membership:
        raise RuntimeError(
            "Gold release membership is partial or includes a separately gated MLV stage"
        )
    sources = []
    tables = {}
    source_codes = {spec["source"] for spec in [*base["dimensions"], *base["facts"]]}
    for code in sorted(
        source_codes,
        key=lambda value: list(
            dict.fromkeys(feed["source_code"] for feed in configuration["feeds"])
        ).index(value),
    ):
        feeds = [
            feed
            for feed in configuration["feeds"]
            if feed["source_code"] == code and feed["enabled"]
        ]
        if len(feeds) != 1 or feeds[0]["authority"] != "authoritative":
            raise RuntimeError("Gold source authority is missing or ambiguous")
        feed = feeds[0]
        source_tables = []
        silver = sorted(
            [
                obj
                for obj in configuration["objects"]
                if obj["feed_id"] == feed["id"]
                and obj["layer"] == "silver"
                and obj["enabled"]
            ],
            key=lambda obj: (obj["execution_priority"], obj["id"]),
        )
        if (
            not silver
            or len({obj["lakehouse"] for obj in silver}) != 1
            or any(obj["target_schema"] != feed["silver_schema"] for obj in silver)
        ):
            raise RuntimeError(
                "Gold requires explicit compatible Silver source objects"
            )
        for obj in silver:
            table = deepcopy(obj["contract"])
            table.update(
                name=obj["name"], keys=obj["business_keys"], columns=obj["columns"]
            )
            for field, default in (
                ("foreign_keys", []),
                ("allow_empty", False),
                ("change_hash_exclusions", []),
            ):
                table.setdefault(field, default)
            policy = extension["hash_policies"].get(obj["id"])
            if policy is None:
                raise RuntimeError("Gold source has no pinned business hash policy")
            table["hash_policy"] = deepcopy(policy)
            table["rule_specs"] = [
                {k: v for k, v in rule.items() if k != "object_id"}
                for rule in configuration["rules"]
                if rule["object_id"] == obj["id"]
                and rule["handler"] != "legacy.source_business_checks"
            ]
            source_tables.append(table)
            tables[code, obj["name"]] = table
        business_hash = feed.get("legacy_contract_hash")
        if business_hash is None:
            # Same fallback as source_runtime_config; never a new key/value hash.
            business_hash = _gold_digest(
                {
                    "source_code": code,
                    "silver_schema": feed["silver_schema"],
                    "tables": source_tables,
                }
            )
        _gold_token(
            business_hash, "pinned source business contract hash", r"[0-9a-f]{64}"
        )
        sources.append(
            {
                "source_code": code,
                "source_instance": feed["source_instance"],
                "source_feed_id": feed["id"],
                "source_type": feed["source_type"],
                "description_slug": feed["description_slug"],
                "lakehouse": silver[0]["lakehouse"],
                "silver_schema": feed["silver_schema"],
                "quarantine_schema": feed["quarantine_schema"],
                "bronze_root": feed.get("legacy_bronze_root"),
                "contract_hash": business_hash,
                "synthetic_only": True,
                "tables": source_tables,
            }
        )
    if len({source["lakehouse"] for source in sources}) != 1:
        raise RuntimeError("Gold requires the configured shared Silver Lakehouse")
    by_feed = {source["source_feed_id"]: source for source in sources}
    by_source = {source["source_code"]: source for source in sources}
    definitions = {}
    dimensions, facts = [], []
    for task in selected:
        obj = objects[task["object_id"]]
        if (
            not obj["enabled"]
            or obj["worker_id"] != worker_id
            or task["worker_id"] != worker_id
            or obj["lakehouse"] != lakehouse
            or obj["target_schema"] != base["schema"]
        ):
            raise RuntimeError("Disabled or incorrectly bound Gold object")
        if obj.get("layout") is not None and globals().get("_MEDALLION_LAYOUT") is None:
            raise RuntimeError(
                "Gold layout changes require the separate approved layout adapter"
            )
        name, kind = obj["name"], obj["materialization"]
        _gold_token(name, "Gold object name")
        if name in definitions or name in base["blocked"]:
            raise RuntimeError("Duplicate or unavailable Gold output")
        transform = transforms.get(obj["id"])
        if transform is None:
            raise RuntimeError("Gold object has no versioned transformation")
        spec = deepcopy(obj["contract"])
        if transform["kind"] == "pattern" and transform["definition"] != {
            "pattern": kind,
            "contract": spec,
        }:
            raise RuntimeError(
                "Gold pattern definition differs from its explicit object contract"
            )
        if kind not in {"scd2_dimension", "atomic_fact", "snapshot_fact", "calendar"}:
            raise RuntimeError(
                "Unsupported Gold materialization; MLV lifecycle is separate"
            )
        if kind != "calendar":
            if (
                spec.get("name") != name
                or (spec.get("source"), spec.get("table")) not in tables
            ):
                raise RuntimeError("Gold object has no explicit source/grain contract")
            table = tables[spec["source"], spec["table"]]
            fields = {column["name"] for column in table["columns"]}
            used = (
                set(spec["keys"])
                | set(spec["attributes"])
                | set(spec.get("measures", []))
            )
            if not used - {"_department_id"} <= fields:
                raise RuntimeError(
                    "Gold keys/attributes reference unknown source columns"
                )
            if kind == "scd2_dimension":
                if (
                    spec.get("source_instance")
                    != by_source[spec["source"]]["source_instance"]
                ):
                    raise RuntimeError(
                        "Gold dimension/source instance identity is inconsistent"
                    )
                dimensions.append(spec)
            else:
                expected_keys = list(table["keys"])
                if spec["period_field"] and spec["period_field"] not in expected_keys:
                    expected_keys.append(spec["period_field"])
                if spec["keys"] != expected_keys or spec["snapshot"] != (
                    kind == "snapshot_fact"
                ):
                    raise RuntimeError(
                        "Gold fact grain differs from its source contract"
                    )
                if not spec.get("grain"):
                    raise RuntimeError(
                        "Gold facts require an explicitly authored grain"
                    )
                facts.append(spec)
        target = f"{lakehouse}.{base['schema']}.{name}"
        if task["target_path"] != target:
            raise RuntimeError("Gold task target differs from its object contract")
        dependencies = []
        source_inputs = []
        for edge in task["dependencies"]:
            upstream = by_task.get(edge["task_id"])
            if upstream is None:
                raise RuntimeError("Gold task has a missing pinned dependency")
            parent = objects[upstream["object_id"]]
            if parent["layer"] == "gold":
                if edge["condition"] != "succeeded":
                    raise RuntimeError("Gold dependencies require object success")
                dependencies.append(parent["name"])
            elif parent["layer"] != "silver" or edge["condition"] != "source_committed":
                raise RuntimeError("Gold requires committed source dependencies")
            elif not parent["enabled"]:
                raise RuntimeError("A required Gold source dependency is disabled")
            else:
                source = by_feed.get(parent["feed_id"])
                if (
                    source is None
                    or (source["source_code"], parent["name"]) not in tables
                ):
                    raise RuntimeError(
                        "Gold source dependency has no pinned source object"
                    )
                source_inputs.append(
                    {"source": source["source_code"], "table": parent["name"]}
                )
        if kind != "calendar":
            primary = {"source": spec["source"], "table": spec["table"]}
            if primary not in source_inputs:
                raise RuntimeError(
                    "Gold definition is missing its declared committed source dependency"
                )
            if transform["kind"] == "pattern" and source_inputs != [primary]:
                raise RuntimeError(
                    "Additional Gold source joins require an explicitly registered transformation"
                )
        elif transform["kind"] == "pattern" and source_inputs:
            raise RuntimeError("Calendar patterns consume only declared fact outputs")
        definitions[name] = {
            "task_id": task["task_id"],
            "object_id": obj["id"],
            "name": name,
            "kind": kind,
            "target": target,
            "dependencies": dependencies,
            "source_inputs": source_inputs,
            "spec": spec,
            "transformation": deepcopy(transform),
        }
    dimension_names = {spec["name"] for spec in dimensions}
    fact_names = {spec["name"] for spec in facts}
    for name, task in definitions.items():
        required = (
            dimension_names
            if name in fact_names
            else fact_names
            if task["kind"] == "calendar"
            else set()
        )
        if not required <= set(task["dependencies"]):
            raise RuntimeError("Gold temporal/fact dependencies are incomplete")
    base.update(
        sources=sources,
        dimensions=dimensions,
        facts=facts,
        contract_hash=extension["legacy_gold_contract_hash"],
        runtime_sha256=code_sha256,
        # MLVs are retained definitions, never executed by this object DAG.
        mlvs={
            obj["name"]: obj["contract"]["mlv_query"]
            for obj in configuration["objects"]
            if obj["layer"] == "gold" and obj["materialization"] == "mlv"
        },
        _metadata={
            "run_id": document["run_id"],
            "version_id": document["version_id"],
            "snapshot_sha256": snapshot_sha256,
            "configuration_sha256": document["definition_sha256"],
            "worker_id": worker_id,
            "notebook_name": workers[0]["notebook_name"],
            "code_sha256": code_sha256,
            "tasks": definitions,
            "layouts": {
                objects[task["object_id"]]["name"]: objects[task["object_id"]].get(
                    "layout"
                )
                for task in selected
            },
            "layout_runtime": extension.get("layout_runtime"),
            "rules": [
                rule
                for rule in configuration["rules"]
                if rule["object_id"] in {task["object_id"] for task in selected}
            ],
            "constraints": [
                constraint
                for constraint in configuration.get("constraints", [])
                if constraint["object_id"] in {task["object_id"] for task in selected}
            ],
        },
    )
    rules = base["_metadata"]["rules"]
    if rules:
        handlers = globals().get("_MEDALLION_RULE_HANDLERS")
        if handlers is None or any(
            rule["action"] not in {"fail", "warn"} for rule in rules
        ):
            raise RuntimeError(
                "Gold DQ requires the registered runtime validator; row-drop policies are unsupported"
            )
        for rule in rules:
            handlers["validate_rule_specs"](
                [{key: value for key, value in rule.items() if key != "object_id"}],
                scope="gold",
            )
    for constraint in base["_metadata"]["constraints"]:
        if constraint["enforcement"] != "runtime":
            raise RuntimeError(
                "Engine-enforced Gold constraints require separate capability proof"
            )
    _gold_token(base["contract_hash"], "legacy Gold rule hash", r"[0-9a-f]{64}")
    _validate_gold_tasks(definitions)
    return base


def _validate_gold_tasks(tasks: Mapping[str, dict[str, Any]]) -> None:
    if not tasks or len(tasks) > 5000:
        raise RuntimeError("Gold task membership must be nonempty and bounded")
    identities, targets = set(), set()
    pending = set(tasks)
    for name, task in tasks.items():
        transformation = task["transformation"]
        if (
            task["kind"]
            not in {"scd2_dimension", "atomic_fact", "snapshot_fact", "calendar"}
            or task["name"] != name
            or transformation["kind"] not in {"pattern", "sql", "notebook"}
            or _gold_digest(transformation["definition"])
            != transformation["definition_sha256"]
            or not isinstance(transformation["version"], str)
            or not transformation["version"]
        ):
            raise RuntimeError(
                "Unsupported or changed Gold transformation/task contract"
            )
        if transformation["kind"] == "pattern" and transformation["definition"] != {
            "pattern": task["kind"],
            "contract": task["spec"],
        }:
            raise RuntimeError(
                "Gold pattern does not match its declared implementation contract"
            )
        if task["task_id"] in identities or task["target"] in targets:
            raise RuntimeError(
                "Gold DAG has duplicate identities or conflicting target writers"
            )
        identities.add(task["task_id"])
        targets.add(task["target"])
        if not set(task["dependencies"]) <= set(tasks) or name in task["dependencies"]:
            raise RuntimeError("Gold DAG has a missing dependency or cycle")
        if len(task["dependencies"]) != len(set(task["dependencies"])):
            raise RuntimeError("Gold DAG contains duplicate dependency edges")
    while pending:
        ready = {
            name for name in pending if not set(tasks[name]["dependencies"]) & pending
        }
        if not ready:
            raise RuntimeError("Gold DAG contains a dependency cycle")
        pending -= ready


def build_gold_dag(
    config: dict[str, Any],
    preparation: GoldPreparationReference,
    *,
    manifest_path: str,
    dispatch_path: str,
    dispatch_sha256: str,
    lakehouse: str,
    concurrency: int = 2,
    timeout_seconds: int = 3600,
    cell_timeout_seconds: int = 900,
) -> dict[str, Any]:
    """Build object-only native DAG data; the parent validates/runs it natively.

    Preparation and finalization are separate parent stages. A worker reloads the
    run manifest, layer dispatch and preparation reference, then independently
    reopens source and dependency Delta versions. No frame payloads are passed.
    """
    if type(concurrency) is not int or not 1 <= concurrency <= 16:
        raise ValueError("Gold DAG concurrency must be between 1 and 16")
    if any(
        type(value) is not int or not 1 <= value <= 86400
        for value in (timeout_seconds, cell_timeout_seconds)
    ):
        raise ValueError("Gold DAG timeouts must be bounded positive seconds")
    if config["target_lakehouse"] != lakehouse:
        raise RuntimeError("Gold DAG parent and child Lakehouse bindings differ")
    metadata = config["_metadata"]
    for path in (manifest_path, dispatch_path):
        if (
            not isinstance(path, str)
            or not path.startswith(f"Files/orchestration/{metadata['run_id']}/")
            or any(part in {"", ".", ".."} for part in path.split("/"))
        ):
            raise ValueError("Gold DAG references must stay in the pinned run folder")
    _gold_token(dispatch_sha256, "Gold dispatch hash", r"[0-9a-f]{64}")
    _gold_reference(preparation, _gold_target(config, "ctl_gold_release"))
    _gold_token(preparation["input_sha256"], "Gold preparation hash", r"[0-9a-f]{64}")
    tasks = metadata["tasks"]
    _validate_gold_tasks(tasks)
    names = {
        name: "task_" + hashlib.sha256(task["task_id"].encode()).hexdigest()
        for name, task in tasks.items()
    }
    activities = [
        {
            "name": names[name],
            "path": metadata["notebook_name"],
            "timeoutPerCellInSeconds": cell_timeout_seconds,
            "retry": 0,
            "args": {
                "run_id": metadata["run_id"],
                "version_id": metadata["version_id"],
                "manifest_path": manifest_path,
                "snapshot_sha256": metadata["snapshot_sha256"],
                "dispatch_path": dispatch_path,
                "dispatch_sha256": dispatch_sha256,
                "task_id": task["task_id"],
                "worker_id": metadata["worker_id"],
                "gold_preparation_json": _gold_json(preparation),
            },
            "dependencies": [names[parent] for parent in task["dependencies"]],
        }
        for name, task in tasks.items()
    ]
    return {
        "activities": activities,
        "timeoutInSeconds": timeout_seconds,
        "concurrency": concurrency,
    }


def gold_request(config: dict[str, Any], source_runs_json: str, release_id: str):
    if config["execution_mode"] != "synthetic_demo" or not all(
        source["synthetic_only"] is True for source in config["sources"]
    ):
        raise ValueError(
            "Production Gold is gated pending certified keys, mappings and source policies"
        )
    if not isinstance(release_id, str) or not re.fullmatch(
        r"[A-Za-z0-9_-]{1,128}", release_id
    ):
        raise ValueError("An explicit bounded gold_release_id is required")
    for name in (config["target_lakehouse"], config["schema"]):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Invalid Gold namespace")
    runs = json.loads(source_runs_json)
    if not isinstance(runs, dict) or set(runs) != {
        s["source_code"] for s in config["sources"]
    }:
        raise ValueError(
            "Select exactly one explicit committed run for every Gold source"
        )
    dates = set()
    for value in runs.values():
        if not isinstance(value, dict) or set(value) != {
            "pipeline_run_id",
            "processing_date",
        }:
            raise ValueError(
                "Source selections require pipeline_run_id and processing_date only"
            )
        if not isinstance(value["pipeline_run_id"], str) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,128}", value["pipeline_run_id"]
        ):
            raise ValueError("Invalid selected source run")
        if not isinstance(value["processing_date"], str) or not re.fullmatch(
            r"\d{4}-\d\d-\d\d", value["processing_date"]
        ):
            raise ValueError("Source processing dates must be ISO dates")
        dates.add(date.fromisoformat(value["processing_date"]))
    if len(dates) != 1:
        raise ValueError(
            "Demo Gold requires a common extraction date; mixed cutoffs need an approved policy"
        )
    return runs, dates.pop()


def _gold_target(config: dict[str, Any], name: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
        raise ValueError("Invalid Gold table identifier")
    return f"{config['target_lakehouse']}.{config['schema']}.{name}"


def _gold_delta(spark, table: str):
    from delta.tables import DeltaTable

    location = (
        spark.sql(f"DESCRIBE DETAIL {table}").select("location").first()["location"]
    )
    return DeltaTable.forPath(spark, location)


def _gold_hash(*columns):
    from pyspark.sql import functions as F

    return F.sha2(F.to_json(F.struct(*columns), {"ignoreNullFields": "false"}), 256)


def _gold_sk(column):
    from pyspark.sql import functions as F

    return (F.conv(F.substring(column, 1, 15), 16, 10).cast("long") + 1).cast("long")


def _assert_unique(frame, keys: list[str], label: str) -> None:
    from pyspark.sql import functions as F

    if (
        frame.filter(reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys]))
        .limit(1)
        .count()
    ):
        raise RuntimeError(f"{label}: null key")
    if frame.groupBy(*keys).count().filter("count > 1").limit(1).count():
        raise RuntimeError(f"{label}: duplicate key")


def _latest(frame, keys: list[str]):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    return (
        frame.withColumn(
            "_gold_latest",
            F.row_number().over(
                Window.partitionBy(*keys).orderBy(F.col("record_version").desc())
            ),
        )
        .filter("_gold_latest = 1")
        .drop("_gold_latest")
    )


def _gold_adapter():
    adapter = globals().get("_CASE_GOLD")
    if adapter is None and "__file__" in globals():
        from pathlib import Path

        from fabric_medallion_deployer.use_cases import (
            load_case_extension,
            load_use_case,
        )

        adapter = load_case_extension(
            load_use_case(Path(__file__).resolve().parents[2]), "gold_rules"
        )
        globals()["_CASE_GOLD"] = adapter
    if adapter is None:
        raise RuntimeError("The reviewed selected case Gold adapter must be injected")
    return adapter


def _department(frame, source: str, config: dict[str, Any]):
    adapter = _gold_adapter()
    function = getattr(adapter, "_department", None)
    return function(frame, source, config) if function else frame


def build_scd2(frame, spec: dict[str, Any]):
    """Derive deterministic, gap-safe observed-time versions without backdating."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    keys, attributes = spec["keys"], spec["attributes"]
    selected = (
        frame.filter(reduce(lambda a, b: a & b, [F.col(k).isNotNull() for k in keys]))
        .select(
            *keys,
            *attributes,
            F.col("record_observed_utc").alias("effective_from"),
            F.col("record_id").alias("source_record_id"),
        )
        .withColumn(
            "durable_key",
            _gold_hash(
                F.lit(spec.get("source_instance", spec["name"])).alias(
                    "source_instance"
                ),
                *[F.col(k) for k in keys],
            ),
        )
        .withColumn("attribute_hash", _gold_hash(*[F.col(k) for k in attributes]))
    )
    conflicts = (
        selected.groupBy("durable_key", "effective_from")
        .agg(F.countDistinct("attribute_hash").alias("states"))
        .filter("states > 1")
    )
    if conflicts.limit(1).count():
        raise RuntimeError(
            f"{spec['name']}: conflicting dimension states at the same observation time"
        )
    selected = selected.groupBy(
        *keys, *attributes, "durable_key", "attribute_hash", "effective_from"
    ).agg(F.min("source_record_id").alias("source_record_id"))
    window = Window.partitionBy("durable_key").orderBy("effective_from")
    changes = (
        selected.withColumn("_previous", F.lag("attribute_hash").over(window))
        .filter(~F.col("attribute_hash").eqNullSafe(F.col("_previous")))
        .drop("_previous")
    )
    output = (
        changes.withColumn("effective_to", F.lead("effective_from").over(window))
        .withColumn("is_current", F.col("effective_to").isNull())
        .withColumn(
            "dimension_sk",
            _gold_sk(
                _gold_hash(
                    F.lit(spec["name"]).alias("dimension"),
                    F.col("durable_key"),
                    F.col("effective_from"),
                )
            ),
        )
        .withColumn("time_basis", F.lit("source_observed_not_business_effective"))
        .withColumn(
            "semantic_basis",
            F.lit(spec.get("semantic_basis", "SYNTHETIC_SOURCE_MEMBER")),
        )
    )
    _assert_unique(output, ["dimension_sk"], spec["name"])
    unknown = output.sparkSession.createDataFrame(
        [
            {
                field.name: (
                    0
                    if field.name == "dimension_sk"
                    else "UNKNOWN"
                    if field.name in {"durable_key", "attribute_hash", "time_basis"}
                    else True
                    if field.name == "is_current"
                    else None
                )
                for field in output.schema
            }
        ],
        T.StructType([T.StructField(f.name, f.dataType, True) for f in output.schema]),
    )
    return output.unionByName(unknown)


def attach_dimension(frame, dimension, spec: dict[str, Any], output_key: str):
    from pyspark.sql import functions as F

    lookup_keys = getattr(_gold_adapter(), "dimension_lookup_keys", None)
    keys = (
        lookup_keys(frame, spec)
        if lookup_keys
        else spec.get("lookup_keys", spec["keys"])
    )
    if not set(keys) <= set(frame.columns):
        return frame.withColumn(output_key, F.lit(0).cast("long"))
    prefix = f"_lookup_{output_key}_"
    lookup = dimension.filter("dimension_sk <> 0").select(
        *[F.col(k).alias(f"{prefix}{i}") for i, k in enumerate(keys)],
        F.col("dimension_sk").alias(output_key),
        F.col("effective_from").alias(prefix + "start"),
        F.col("effective_to").alias(prefix + "end"),
    )
    condition = (
        reduce(
            lambda a, b: a & b,
            [F.col(k).eqNullSafe(F.col(f"{prefix}{i}")) for i, k in enumerate(keys)],
        )
        & (F.col("record_observed_utc") >= F.col(prefix + "start"))
        & (
            F.col(prefix + "end").isNull()
            | (F.col("record_observed_utc") < F.col(prefix + "end"))
        )
    )
    output = (
        frame.join(lookup, condition, "left")
        .withColumn(output_key, F.coalesce(F.col(output_key), F.lit(0)).cast("long"))
        .drop(*[n for n in lookup.columns if n != output_key])
    )
    return output


def source_age_band(column):
    function = getattr(_gold_adapter(), "source_age_band", None)
    if function is None:
        raise RuntimeError("Source age bands require an explicit case rule")
    return function(column)


def build_fact(
    frame,
    spec: dict[str, Any],
    dimensions: dict[str, Any],
    config: dict[str, Any],
    as_of: date,
    release_id: str,
):
    from pyspark.sql import functions as F

    if not spec["snapshot"]:
        frame = _latest(frame, spec["keys"])
    _assert_unique(frame, spec["keys"], spec["name"])
    frame = _department(frame, spec["source"], config)
    dimension_specs = {d["name"]: d for d in config["dimensions"]}
    adapter = _gold_adapter()
    attach_identity = getattr(adapter, "attach_supplier_identity", None)
    if attach_identity:
        frame = attach_identity(frame, config, assert_unique=_assert_unique)
    output_key = getattr(adapter, "dimension_output_key", lambda name: name + "_sk")
    for name, dimension in dimensions.items():
        frame = attach_dimension(
            frame, dimension, dimension_specs[name], output_key(name)
        )
    event_adapter = getattr(adapter, "fact_event_date", None)
    if event_adapter:
        event_date = event_adapter(frame, spec, as_of)
    elif spec["snapshot"]:
        event_date = F.lit(as_of)
    elif not spec["date_field"]:
        raise ValueError(f"{spec['name']}: atomic facts require a source event date")
    else:
        event_date = (
            F.to_date(F.col(spec["date_field"]).cast("string"), spec["date_format"])
            if spec.get("date_format")
            else F.to_date(F.col(spec["date_field"]))
        )
    period_field = spec.get("posting_period_field") or spec["period_field"]
    period = (
        F.col(period_field).cast("int")
        if period_field
        else F.date_format(event_date, "yyyyMM").cast("int")
    )
    date_roles = {
        name: (
            F.to_date(F.col(name))
            if kind == "timestamp"
            else F.to_date(F.col(name).cast("string"), "yyyyMMdd")
        )
        for name, kind in spec.get("date_roles", {}).items()
    }
    date_adapter = getattr(adapter, "fact_date_roles", None)
    if date_adapter:
        date_roles = date_adapter(spec)
    instance = next(
        s["source_instance"]
        for s in config["sources"]
        if s["source_code"] == spec["source"]
    )
    identity = _gold_hash(
        F.lit(spec["source"]).alias("source"),
        F.lit(instance).alias("source_instance"),
        F.lit(spec["table"]).alias("source_table"),
        *[F.col(k) for k in spec["keys"]],
        *([F.lit(as_of).alias("as_of_date")] if spec["snapshot"] else []),
    )
    dimension_columns = [n for n in frame.columns if n.endswith("_sk")]
    attrs = [n for n in spec["attributes"] if n not in dimension_columns]
    source_keys = F.to_json(F.struct(*[F.col(k) for k in spec["keys"]]))
    output = frame.select(
        _gold_sk(identity).alias("fact_sk"),
        identity.alias("business_event_id"),
        F.lit(spec["source"]).alias("source_system"),
        F.lit(
            next(
                s["source_instance"]
                for s in config["sources"]
                if s["source_code"] == spec["source"]
            )
        ).alias("source_instance_key"),
        F.lit(spec["table"]).alias("source_table"),
        source_keys.alias("source_business_key"),
        F.col("record_id").alias("source_record_id"),
        F.col("record_version").alias("source_record_version"),
        F.col("record_hash").alias("source_record_hash"),
        F.col("record_observed_utc").alias("source_observed_utc"),
        F.col("ingest_loaded_utc").alias("source_loaded_utc"),
        "ingest_batch_run_id",
        "ingest_processing_date",
        "ingest_contract_hash",
        F.lit(release_id).alias("gold_release_id"),
        F.lit(datetime.now(UTC)).alias("gold_loaded_utc"),
        F.lit(config["contract_hash"]).alias("mapping_version"),
        F.lit(config["contract_hash"]).alias("measure_rule_version"),
        F.lit("SYNTHETIC_PROVISIONAL").alias("dq_status"),
        F.lit("DEMO_CONTRACT_KEY_NOT_PRODUCTION_CERTIFIED").alias("grain_status"),
        F.lit("source_observed_not_business_effective").alias("dimension_time_basis"),
        (
            adapter.supplier_business_key(frame)
            if hasattr(adapter, "supplier_business_key")
            else F.lit(None).cast("string")
        ).alias("supplier_business_key"),
        event_date.alias("event_date"),
        F.date_format(event_date, "yyyyMMdd").cast("int").alias("event_date_sk"),
        period.alias("posting_month"),
        *(
            [
                F.lit(as_of).alias("as_of_date"),
                F.lit(int(as_of.strftime("%Y%m%d"))).alias("as_of_date_sk"),
            ]
            if spec["snapshot"]
            else []
        ),
        *[
            F.coalesce(F.date_format(value, "yyyyMMdd").cast("int"), F.lit(0)).alias(
                name + "_sk"
            )
            for name, value in date_roles.items()
        ],
        *dimension_columns,
        *[
            F.col(n)
            .cast(spec.get("measure_types", {}).get(n, "decimal(28,4)"))
            .alias(n)
            for n in spec["measures"]
        ],
        *attrs,
        *getattr(adapter, "fact_extra_columns", lambda frame, spec: [])(frame, spec),
    )
    _assert_unique(output, ["fact_sk"], spec["name"])
    if output.filter(F.col("event_date").isNull()).limit(1).count():
        raise RuntimeError(f"{spec['name']}: missing/invalid event date")
    return output


def calendar_frames(spark, dates, calendar: dict[str, Any] | None = None):
    # Omitted calendar is the legacy Python API only; compiled contracts declare it.
    if calendar is None:
        function = getattr(_gold_adapter(), "legacy_calendar_contract", None)
        if function is None:
            raise ValueError("The selected case must declare its fiscal calendar")
        calendar = function()
    start_month = calendar.get("fiscal_year_start_month")
    if type(start_month) is not int or not 1 <= start_month <= 12:
        raise ValueError(
            "Calendar requires an explicit fiscal year start month in 1..12"
        )
    basis = calendar.get("basis", "DECLARED_FISCAL_CALENDAR")
    from pyspark.sql import functions as F

    bounds = dates.agg(
        F.min("event_date").alias("start"), F.max("event_date").alias("end")
    ).first()
    if bounds["start"] is None:
        days = spark.createDataFrame([], "calendar_date date")
    else:
        length = (bounds["end"] - bounds["start"]).days + 1
        if length > 366 * 200:
            raise RuntimeError("Calendar range exceeds the supported demo window")
        days = spark.range(length).select(
            F.date_add(F.lit(bounds["start"]), F.col("id").cast("int")).alias(
                "calendar_date"
            )
        )
    days = days.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_sk"),
        "calendar_date",
        F.year("calendar_date").alias("calendar_year"),
        F.month("calendar_date").alias("calendar_month"),
        F.date_format("calendar_date", "yyyyMM").cast("int").alias("fiscal_period_sk"),
    )
    periods = (
        days.select("fiscal_period_sk", "calendar_year", "calendar_month")
        .distinct()
        .withColumn(
            "fiscal_year_start",
            F.col("calendar_year")
            - F.when(F.col("calendar_month") < start_month, 1).otherwise(0),
        )
        .withColumn(
            "fiscal_month", F.pmod(F.col("calendar_month") - start_month, F.lit(12)) + 1
        )
        .withColumn("calendar_basis", F.lit(basis))
    )
    days = days.unionByName(
        spark.createDataFrame(
            [(0, None, None, None, 0)],
            "date_sk int, calendar_date date, calendar_year int, calendar_month int, fiscal_period_sk int",
        )
    )
    periods = periods.unionByName(
        spark.createDataFrame(
            [(0, None, None, None, None, "UNKNOWN")],
            "fiscal_period_sk int, calendar_year int, calendar_month int, fiscal_year_start int, fiscal_month int, calendar_basis string",
        )
    )
    return {"dim_date": days, "dim_fiscal_period": periods}


def _pin(spark, name: str, manifest: dict[str, Any]):
    table = _gold_delta(spark, name)
    version = table.history(1).select("version").first()["version"]
    location = table.detail().select("location").first()["location"]
    manifest[name] = {"version": version, "location": location}
    return spark.read.format("delta").option("versionAsOf", version).load(location)


def _reload_gold_table(spark, reference, expected_table: str, *, current: bool = False):
    """Resolve the persisted location and reopen an exact Delta version."""
    _gold_reference(reference, expected_table)
    delta = _gold_delta(spark, expected_table)
    if delta.detail().select("location").first()["location"] != reference["location"]:
        raise RuntimeError("A pinned Gold input/output table location changed")
    if (
        current
        and delta.history(1).select("version").first()["version"]
        != reference["version"]
    ):
        raise RuntimeError(f"{expected_table}: output drift invalidates Gold readiness")
    return (
        spark.read.format("delta")
        .option("versionAsOf", reference["version"])
        .load(reference["location"])
    )


def manifest_gold_context(
    config: dict[str, Any],
    source: dict[str, Any],
    selected: dict[str, str],
    context_json: str,
    context_sha256: str,
    ingestion: Any,
) -> dict[str, Any]:
    """Adapt a sealed source context, without relaxing legacy path validation."""
    context = ingestion.manifest_processing_context(
        context_json, context_sha256, expected_lakehouse=source["lakehouse"]
    )
    if (
        context["source_code"] != source["source_code"]
        or context["contract_hash"] != source["contract_hash"]
        or context["pipeline_run_id"] != selected["pipeline_run_id"]
        or context["processing_date"].isoformat() != selected["processing_date"]
        or context["source_selection_complete"] is not True
        or context["source_feed_id"]
        != source.get("source_feed_id", context["source_feed_id"])
        or context["source_type"] != source.get("source_type", context["source_type"])
        or context["description_slug"]
        != source.get("description_slug", context["description_slug"])
    ):
        raise RuntimeError("Gold source context differs from its committed selection")
    metadata = config.get("_metadata")
    if metadata and context["metadata_version"] != metadata["version_id"]:
        raise RuntimeError(
            "Gold source rule/configuration version differs from the pinned run"
        )
    known = {table["name"]: table for table in source["tables"]}
    for table in context["tables"]:
        expected = known.get(table["name"])
        if expected is None or any(
            table.get(field) != expected.get(field)
            for field in ("keys", "columns", "hash_policy", "rule_specs")
        ):
            raise RuntimeError(
                "Gold source object/hash/rule contract differs from its manifest"
            )
    if set(known) != {table["name"] for table in context["tables"]}:
        raise RuntimeError("Gold source manifest omitted configured source objects")
    return context


def _gold_contexts(fs, config, runs, ingestion, references):
    if references is None:
        if "_metadata" in config:
            raise RuntimeError("Normal Gold requires pinned manifest source contexts")
        return None, {}
    if set(references) != set(runs):
        raise RuntimeError(
            "Gold source context membership differs from the selected sources"
        )
    contexts, serialized = {}, {}
    for source in config["sources"]:
        code = source["source_code"]
        reference = references[code]
        if set(reference) == {"context_json", "sha256"}:
            text = reference["context_json"]
        elif set(reference) == {"path", "sha256"}:
            text = fs.head(reference["path"], MAX_GOLD_MANIFEST_BYTES + 1)
        else:
            raise ValueError(
                "Gold source contexts require a pinned JSON/path and SHA-256"
            )
        if (
            not isinstance(text, str)
            or len(text.encode("utf-8")) > MAX_GOLD_MANIFEST_BYTES
        ):
            raise RuntimeError(
                "Gold source context transport is incomplete or oversized"
            )
        contexts[code] = manifest_gold_context(
            config, source, runs[code], text, reference["sha256"], ingestion
        )
        serialized[code] = {
            "context_json": _gold_json(json.loads(text)),
            "sha256": reference["sha256"],
        }
    return contexts, serialized


def _gold_row_observed_utc(value: datetime) -> datetime:
    """Normalize Spark's Python Row timestamp exactly like ingestion.

    TimestampType.fromInternal returns a naïve host-local datetime. Replacing its
    tzinfo would shift the observation on non-UTC hosts. This conversion does not
    reinterpret timestamp_ntz business dates or change the stored Silver hash.
    """
    return value.astimezone(UTC)


def _gold_bronze_reader(fs, context, bronze_base):
    """Qualify validated manifest paths against the explicit Silver storage root.

    Notebook workers are Gold-bound. Only the read adapter changes physical path
    qualification; the original relative paths, batch ID and hash policy remain
    in the pinned source context and sealed audit evidence.
    """
    if context.get("metadata_mode") != "manifest_v1":
        return fs, context
    if not isinstance(bronze_base, str) or not bronze_base.rstrip("/").endswith(
        "/Files/bronze"
    ):
        raise ValueError(
            "Manifest Gold requires an explicit source Lakehouse Files/bronze root"
        )
    bound = deepcopy(context)
    qualify = lambda path: (
        bronze_base.rstrip("/") + "/" + path.removeprefix("Files/bronze/")
    )
    bound["raw_source_files"] = {
        name: [qualify(path) for path in paths]
        for name, paths in context["raw_source_files"].items()
    }
    bound["raw_targets"] = {
        name: qualify(path) for name, path in context["raw_targets"].items()
    }
    # PurePosixPath in the existing reader collapses URI slashes for ls parents.
    parents = {
        str(PurePosixPath(path).parent): path.rsplit("/", 1)[0]
        for paths in bound["raw_source_files"].values()
        for path in paths
    }
    reader = SimpleNamespace(
        exists=fs.exists,
        head=fs.head,
        ls=lambda path: fs.ls(parents.get(path, path)),
    )
    return reader, bound


def _gold_inputs(
    spark,
    fs,
    config,
    runs,
    ingestion,
    bronze_base,
    resources,
    *,
    source_contexts=None,
    pinned_manifest=None,
    required=None,
):
    from pyspark.sql import functions as F

    manifest = (
        deepcopy(pinned_manifest)
        if pinned_manifest is not None
        else {"_cutoffs": {}, "_sealed": {}}
    )
    histories, snapshots = {}, {}

    def pin(name):
        if pinned_manifest is None:
            return _pin(spark, name, manifest)
        if name not in manifest:
            raise RuntimeError(
                "A required Silver/audit version is missing from the Gold manifest"
            )
        return _reload_gold_table(spark, {"table": name, **manifest[name]}, name)

    source_name = config["sources"][0]["lakehouse"]
    audit = pin(f"{source_name}.audit.bronze_silver_validation")
    batch_manifest = pin(f"{source_name}.audit.bronze_batch_manifest")
    required = (
        required
        if required is not None
        else {(f["source"], f["table"]) for f in config["facts"]}
        | {(d["source"], d["table"]) for d in config["dimensions"]}
    )
    for source in config["sources"]:
        code = source["source_code"]
        if not any(key[0] == code for key in required):
            continue
        selected = runs[code]
        context = (
            source_contexts[code]
            if source_contexts is not None
            else (
                source
                | selected
                | {
                    "processing_date": date.fromisoformat(selected["processing_date"]),
                    "bronze_root": f"{bronze_base}/{code}",
                }
            )
        )
        reader, read_context = _gold_bronze_reader(fs, context, bronze_base)
        commits = (
            audit.filter(
                (F.col("source_code") == code)
                & (F.col("check_name") == "source_batch_committed")
                & (F.col("status") == "SUCCESS")
            )
            .select("pipeline_run_id", "processing_date", "contract_hash")
            .distinct()
        )
        selected_commit = commits.filter(
            (F.col("pipeline_run_id") == selected["pipeline_run_id"])
            & (F.col("processing_date") == F.lit(context["processing_date"]))
            & (F.col("contract_hash") == source["contract_hash"])
        )
        if selected_commit.count() != 1:
            raise RuntimeError(
                f"{code}: the selected source batch is not committed under this contract"
            )
        for table in context["tables"]:
            key = (code, table["name"])
            if key not in required:
                continue
            sealed = (
                batch_manifest.filter(
                    (F.col("source_code") == code)
                    & (F.col("table_name") == table["name"])
                    & (F.col("pipeline_run_id") == selected["pipeline_run_id"])
                    & (F.col("processing_date") == F.lit(context["processing_date"]))
                    & (F.col("contract_hash") == source["contract_hash"])
                )
                .limit(2)
                .collect()
            )
            if len(sealed) != 1:
                raise RuntimeError(
                    f"{code}.{table['name']}: missing or duplicate sealed batch"
                )
            cutoff = sealed[0]["observed_utc"]
            identity = f"{code}.{table['name']}"
            evidence = {
                "observed_utc": cutoff.isoformat(),
                "snapshot_hash": sealed[0]["snapshot_hash"],
                "source_rows": sealed[0]["source_rows"],
                "source_path": sealed[0]["source_path"],
            }
            if pinned_manifest is not None and (
                manifest["_cutoffs"].get(identity) != cutoff.isoformat()
                or manifest["_sealed"].get(identity) != evidence
            ):
                raise RuntimeError(
                    "Gold sealed extraction evidence changed after preparation"
                )
            if source_contexts is not None and (
                _gold_row_observed_utc(cutoff)
                != context["batch_started_utc"].astimezone(UTC)
                or evidence["source_path"] != ingestion.source_path(context, table)
            ):
                raise RuntimeError(
                    "Gold sealed batch paths/observation differ from the manifest"
                )
            manifest["_cutoffs"][identity] = cutoff.isoformat()
            manifest["_sealed"][identity] = evidence
            original = pin(
                f"{source['lakehouse']}.{source['silver_schema']}.{table['name']}"
            )
            commit_keys = commits.select(
                F.col("pipeline_run_id").alias("_run"),
                F.col("processing_date").alias("_date"),
                F.col("contract_hash").alias("_contract"),
            )
            accepted = (
                original.join(
                    commit_keys,
                    (F.col("ingest_batch_run_id") == F.col("_run"))
                    & (F.col("ingest_processing_date") == F.col("_date"))
                    & (F.col("ingest_contract_hash") == F.col("_contract")),
                    "left_semi",
                )
                .filter(F.col("record_observed_utc") <= F.lit(cutoff))
                .cache()
            )
            resources.append(accepted)
            if (
                accepted.filter(~F.coalesce(F.col("dq_passed"), F.lit(False)))
                .limit(1)
                .count()
            ):
                raise RuntimeError("Rejected rows cannot enter Gold")
            _assert_unique(
                accepted, ["record_id"], f"{code}.{table['name']} representations"
            )
            histories[key] = _department(accepted, code, config)
            # Original batch membership is authoritative for snapshots and changed control metadata.
            raw = ingestion.read_raw(spark, reader, read_context, table)
            fingerprint, count = ingestion.snapshot_fingerprint(raw)
            if (
                fingerprint != sealed[0]["snapshot_hash"]
                or count != sealed[0]["source_rows"]
            ):
                raise RuntimeError(
                    f"{code}.{table['name']}: immutable Bronze content changed"
                )
            prepared = ingestion.prepare_rows(raw, table).cache()
            resources.append(prepared)
            if (
                prepared.filter(F.size(ingestion.blocking_dq_reasons(table)) > 0)
                .limit(1)
                .count()
            ):
                raise RuntimeError(
                    f"{code}.{table['name']}: invalid committed source batch"
                )
            source_members = prepared.filter(F.size("_dq_reasons") == 0)
            accepted_count = source_members.count()
            hashed = ingestion.with_record_hashes(source_members, table).alias("batch")
            current = _latest(accepted, table["keys"]).alias("slv")
            condition = reduce(
                lambda a, b: a & b,
                [
                    F.col(f"batch.`{k}`").eqNullSafe(F.col(f"slv.`{k}`"))
                    for k in table["keys"]
                ],
            ) & F.col("batch._record_hash").eqNullSafe(F.col("slv.record_hash"))
            members = hashed.join(current, condition, "left").select(
                *[
                    F.col(f"batch.`{c['name']}`").alias(c["name"])
                    for c in table["columns"]
                ],
                *[
                    F.col(f"slv.`{n}`").alias(n)
                    for n in original.columns
                    if n not in {c["name"] for c in table["columns"]}
                ],
            )
            if (
                members.filter("record_id IS NULL").limit(1).count()
                or members.count() != accepted_count
            ):
                raise RuntimeError(
                    f"{code}.{table['name']}: source membership does not reconcile to committed Silver"
                )
            snapshots[key] = _department(members, code, config).cache()
            resources.append(snapshots[key])
            prepared.unpersist()
    return manifest, histories, snapshots


def _merge_gold(
    spark,
    target: str,
    frame,
    key: str,
    *,
    as_of: date | None = None,
    enforce_layout: bool = False,
    layout=None,
    layout_capabilities=None,
):
    from pyspark.sql import functions as F

    _assert_unique(frame, [key], target)
    content = [
        n for n in frame.columns if n not in {"gold_release_id", "gold_loaded_utc"}
    ]
    frame = frame.withColumn(
        "_gold_content_hash", _gold_hash(*[F.col(n) for n in content])
    )
    if enforce_layout:
        layout_runtime = globals().get("_MEDALLION_LAYOUT")
        if layout_runtime is None:
            raise RuntimeError(
                "Generic Gold requires the packaged physical layout adapter"
            )
        capability = (
            layout_runtime.RuntimeCapabilities(**layout_capabilities)
            if layout_capabilities is not None
            else None
        )
        existed = spark.catalog.tableExists(target)
        layout_runtime.ensure_target_layout(
            spark,
            target,
            frame,
            layout,
            existing_target=existed,
            capabilities=capability,
        )
        if not existed:
            spark.sql(
                f"ALTER TABLE {layout_runtime.quote_table(target)} SET TBLPROPERTIES "
                "('delta.enableChangeDataFeed'='true')"
            )
    if not spark.catalog.tableExists(target):
        frame.write.format("delta").option("delta.enableChangeDataFeed", "true").mode(
            "errorifexists"
        ).saveAsTable(target)
    else:
        existing = spark.table(target)
        if [(f.name, f.dataType) for f in existing.schema] != [
            (f.name, f.dataType) for f in frame.schema
        ]:
            raise RuntimeError(
                f"Gold schema mismatch: {target}; explicit migration required"
            )
        _assert_unique(existing, [key], target)
        identity = (
            "business_event_id"
            if "business_event_id" in frame.columns
            else ("durable_key" if "durable_key" in frame.columns else None)
        )
        if identity:
            collisions = (
                existing.alias("before")
                .join(
                    frame.alias("after"),
                    F.col(f"before.{key}") == F.col(f"after.{key}"),
                    "inner",
                )
                .filter(
                    ~F.col(f"before.{identity}").eqNullSafe(F.col(f"after.{identity}"))
                )
            )
            if collisions.limit(1).count():
                raise RuntimeError(
                    f"{target}: surrogate collision; refuse identity overwrite"
                )
        if (
            as_of is None
            and existing.select(key)
            .join(frame.select(key), key, "left_anti")
            .limit(1)
            .count()
        ):
            raise RuntimeError(
                f"{target}: out-of-order input would remove an existing event or dimension version"
            )
        operation = (
            _gold_delta(spark, target)
            .alias("tgt")
            .merge(frame.alias("src"), f"tgt.{key} = src.{key}")
            .whenMatchedUpdateAll(
                condition="NOT (tgt._gold_content_hash <=> src._gold_content_hash)"
            )
            .whenNotMatchedInsertAll()
        )
        if as_of is not None:
            operation = operation.whenNotMatchedBySourceDelete(
                condition=f"tgt.as_of_date = DATE '{as_of.isoformat()}'"
            )
        operation.execute()


def _release_event(
    spark, config, release_id, status, as_of, inputs, outputs, message=""
):
    inputs = {
        key: value
        for key, value in inputs.items()
        if key not in {"_release_id", "_input_sha256"}
    }
    schema = (
        "gold_release_id string, status string, as_of_date date, contract_hash string, "
        "input_manifest_json string, output_manifest_json string, event_utc timestamp, message string"
    )
    row = (
        release_id,
        status,
        as_of,
        config["contract_hash"],
        json.dumps(inputs, sort_keys=True),
        json.dumps(outputs, sort_keys=True),
        datetime.now(UTC),
        message,
    )
    spark.createDataFrame([row], schema).write.format("delta").mode(
        "append"
    ).saveAsTable(_gold_target(config, "ctl_gold_release"))


def _ready_release(spark, config, release_id):
    from pyspark.sql import functions as F

    table = _gold_target(config, "ctl_gold_release")
    if not spark.catalog.tableExists(table):
        return None
    latest = (
        spark.table(table)
        .filter(F.col("gold_release_id") == release_id)
        .orderBy(F.col("event_utc").desc())
        .limit(1)
        .collect()
    )
    if not latest or latest[0]["status"] not in {"FACTS_READY", "MLVS_VERIFIED"}:
        return None
    rows = (
        spark.table(table)
        .filter(
            (F.col("gold_release_id") == release_id)
            & (F.col("status") == "FACTS_READY")
        )
        .orderBy(F.col("event_utc").desc())
        .limit(1)
        .collect()
    )
    return rows[0].asDict() if rows else None


def _gold_table_reference(spark, target: str, *, count: bool = False) -> dict[str, Any]:
    delta = _gold_delta(spark, target)
    reference = {
        "table": target,
        "version": delta.history(1).select("version").first()["version"],
        "location": delta.detail().select("location").first()["location"],
    }
    if count:
        reference["row_count"] = _reload_gold_table(spark, reference, target).count()
    return reference


def _gold_tasks(config):
    if "_metadata" in config:
        tasks = deepcopy(config["_metadata"]["tasks"])
    else:
        tasks = {}
        dimensions = [spec["name"] for spec in config["dimensions"]]
        facts = [spec["name"] for spec in config["facts"]]
        for spec in [*config["dimensions"], *config["facts"]]:
            name = spec["name"]
            kind = (
                "scd2_dimension"
                if name in dimensions
                else ("snapshot_fact" if spec["snapshot"] else "atomic_fact")
            )
            tasks[name] = {
                "task_id": hashlib.sha256(("legacy:" + name).encode()).hexdigest(),
                "name": name,
                "kind": kind,
                "target": _gold_target(config, name),
                "dependencies": [] if name in dimensions else dimensions,
                "source_inputs": [{"source": spec["source"], "table": spec["table"]}],
                "spec": deepcopy(spec),
                "transformation": {
                    "kind": "pattern",
                    "version": "1",
                    "definition": {"pattern": kind, "contract": deepcopy(spec)},
                    "definition_sha256": _gold_digest(
                        {"pattern": kind, "contract": spec}
                    ),
                },
            }
        for name in ("dim_date", "dim_fiscal_period"):
            tasks[name] = {
                "task_id": hashlib.sha256(("legacy:" + name).encode()).hexdigest(),
                "name": name,
                "kind": "calendar",
                "target": _gold_target(config, name),
                "dependencies": facts,
                "source_inputs": [],
                "spec": {},
                "transformation": {
                    "kind": "pattern",
                    "version": "1",
                    "definition": {"pattern": "calendar", "contract": {}},
                    "definition_sha256": _gold_digest(
                        {"pattern": "calendar", "contract": {}}
                    ),
                },
            }
    _validate_gold_tasks(tasks)
    known = {
        (source["source_code"], table["name"])
        for source in config["sources"]
        for table in source["tables"]
    }
    if not _gold_required_sources(tasks) <= known:
        raise RuntimeError("Gold task references an undeclared source input")
    return tasks


def _gold_required_sources(tasks):
    return {
        (reference["source"], reference["table"])
        for task in tasks.values()
        for reference in task.get(
            "source_inputs",
            (
                []
                if task["kind"] == "calendar"
                else [
                    {"source": task["spec"]["source"], "table": task["spec"]["table"]}
                ]
            ),
        )
    }


def _gold_source_references(config, inputs, source_inputs):
    sources = {source["source_code"]: source for source in config["sources"]}
    references = {}
    for selected in source_inputs:
        source = sources[selected["source"]]
        table = f"{source['lakehouse']}.{source['silver_schema']}.{selected['table']}"
        if table not in inputs:
            raise RuntimeError("A declared Gold source input version was not prepared")
        references[table] = {"table": table, **inputs[table]}
    return references


def _gold_execution(config):
    metadata = config.get("_metadata", {})
    return {
        "config_sha256": _gold_digest(config),
        "code_sha256": config["runtime_sha256"],
        "configuration_sha256": metadata.get("configuration_sha256"),
        "version_id": metadata.get("version_id"),
        "snapshot_sha256": metadata.get("snapshot_sha256"),
    }


def _release_record(spark, config, release_id):
    from pyspark.sql import functions as F

    target = _gold_target(config, "ctl_gold_release")
    if not spark.catalog.tableExists(target):
        return None
    rows = (
        spark.table(target)
        .filter(F.col("gold_release_id") == release_id)
        .orderBy(F.col("event_utc").desc())
        .limit(1)
        .collect()
    )
    return rows[0].asDict() if rows else None


def _validate_gold_outputs(spark, outputs):
    for entry in outputs.values():
        reference = dict(entry)
        # Historical release manifests predate explicit storage locations.
        if "location" not in reference:
            reference["location"] = _gold_table_reference(spark, entry["table"])[
                "location"
            ]
        _reload_gold_table(spark, reference, entry["table"], current=True)


def prepare_gold(
    spark: Any,
    fs: Any,
    config: dict[str, Any],
    source_runs_json: str,
    gold_release_id: str,
    ingestion: Any,
    bronze_base: str,
    *,
    source_contexts: Mapping[str, dict[str, str]] | None = None,
    ownership_check: Callable[[], None] | None = None,
) -> GoldPreparationReference:
    """Pin/validate source inputs and initialize controls before parallel workers.

    The returned reference points at a persisted Delta control version, not a
    frame or mutable metadata dictionary. Retries reuse that release's original
    versions, source observation and task/code identity, including failed runs.
    The caller owns/fences the complete target set through finalization and the
    separately coordinated MLV window; this is not a cross-table transaction.
    """
    from pyspark.sql import functions as F

    runs, as_of = gold_request(config, source_runs_json, gold_release_id)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    tasks, execution = _gold_tasks(config), _gold_execution(config)
    selection = {
        "runs": runs,
        "as_of_date": as_of.isoformat(),
        "contract_hash": config["contract_hash"],
    }
    contexts, serialized = _gold_contexts(fs, config, runs, ingestion, source_contexts)
    existing = _release_record(spark, config, gold_release_id)
    retained = json.loads(existing["input_manifest_json"]) if existing else None
    if retained is not None and (
        retained.get("_selection") != selection
        or retained.get("_execution") != execution
        or retained.get("_tasks") != tasks
        or retained.get("_contexts") != serialized
        or retained.get("_bronze_base") != bronze_base
    ):
        raise RuntimeError(
            "A Gold release, including a failed release, cannot be rebound to changed identity or inputs"
        )
    resources = []
    try:
        inputs, _histories, _snapshots = _gold_inputs(
            spark,
            fs,
            config,
            runs,
            ingestion,
            bronze_base,
            resources,
            source_contexts=contexts,
            pinned_manifest=retained,
            required=_gold_required_sources(tasks),
        )
    finally:
        for frame in resources:
            frame.unpersist()
    if retained is None:
        inputs.update(
            _selection=selection,
            _execution=execution,
            _tasks=tasks,
            _contexts=serialized,
            _bronze_base=bronze_base,
            _base_outputs={
                name: _gold_table_reference(spark, _gold_target(config, name))
                for name in [*tasks, "ctl_measure_availability"]
                if spark.catalog.tableExists(_gold_target(config, name))
            },
        )
    _gold_json(inputs)
    if ownership_check is not None:
        ownership_check()
    if existing is None:
        control = _gold_target(config, "ctl_gold_release")
        if spark.catalog.tableExists(control):
            latest = (
                spark.table(control)
                .filter(F.col("status") == "FACTS_READY")
                .orderBy(F.col("event_utc").desc())
                .limit(1)
                .collect()
            )
            if latest:
                previous = json.loads(latest[0]["input_manifest_json"]).get(
                    "_cutoffs", {}
                )
                if latest[0]["as_of_date"] > as_of or any(
                    inputs["_cutoffs"].get(key, "") < cutoff
                    for key, cutoff in previous.items()
                ):
                    raise RuntimeError(
                        "Older Gold cutoffs require a separately reviewed backfill"
                    )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {config['target_lakehouse']}.{config['schema']}"
        )
        task_table = _gold_target(config, "ctl_gold_task")
        if not spark.catalog.tableExists(task_table):
            spark.createDataFrame([], GOLD_TASK_SCHEMA).write.format("delta").mode(
                "errorifexists"
            ).saveAsTable(task_table)
        _release_event(spark, config, gold_release_id, "BUILDING", as_of, inputs, {})
    else:
        ready = _ready_release(spark, config, gold_release_id)
        if ready:
            _validate_gold_outputs(spark, json.loads(ready["output_manifest_json"]))
    return {
        **_gold_table_reference(spark, _gold_target(config, "ctl_gold_release")),
        "gold_release_id": gold_release_id,
        "input_sha256": _gold_digest(inputs),
    }


def load_gold_preparation(
    spark: Any,
    config: dict[str, Any],
    reference: GoldPreparationReference,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    control = _gold_target(config, "ctl_gold_release")
    frame = _reload_gold_table(spark, reference, control)
    rows = (
        frame.filter(F.col("gold_release_id") == reference["gold_release_id"])
        .orderBy(F.col("event_utc").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        raise RuntimeError("Persisted Gold preparation is missing")
    inputs = json.loads(rows[0]["input_manifest_json"])
    if (
        _gold_digest(inputs) != reference["input_sha256"]
        or inputs.get("_execution") != _gold_execution(config)
        or inputs.get("_tasks") != _gold_tasks(config)
    ):
        raise RuntimeError("Gold preparation code/configuration/task identity changed")
    return inputs


def _task_result_rows(spark, config, preparation, name, attempt_id, *, reference=None):
    from pyspark.sql import functions as F

    target = _gold_target(config, "ctl_gold_task")
    if not spark.catalog.tableExists(target):
        return []
    frame = (
        _reload_gold_table(spark, reference, target)
        if reference
        else spark.table(target)
    )
    task = preparation["_tasks"][name]
    return (
        frame.filter(
            (F.col("gold_release_id") == preparation["_release_id"])
            & (F.col("input_sha256") == preparation["_input_sha256"])
            & (F.col("object_name") == name)
            & (F.col("task_id") == task["task_id"])
            & (F.col("attempt_id") == attempt_id)
        )
        .select("status", "result_json")
        .distinct()
        .limit(2)
        .collect()
    )


def _prepared_for_task(spark, config, reference):
    inputs = load_gold_preparation(spark, config, reference)
    return {
        **inputs,
        "_release_id": reference["gold_release_id"],
        "_input_sha256": reference["input_sha256"],
    }


def gold_result_references(
    spark: Any,
    config: dict[str, Any],
    preparation: GoldPreparationReference,
    attempts: Mapping[str, str | GoldAttempt],
) -> dict[str, GoldResultReference]:
    """Resolve only the requested persisted attempts (usually dimension fan-in)."""
    inputs = _prepared_for_task(spark, config, preparation)
    if not set(attempts) <= set(inputs["_tasks"]):
        raise RuntimeError("Undeclared Gold result membership")
    target = _gold_target(config, "ctl_gold_task")
    pinned = _gold_table_reference(spark, target)
    references = {}
    for name, attempt in attempts.items():
        if isinstance(attempt, str):
            if "_metadata" in config:
                raise RuntimeError(
                    "Normal Gold result fan-in requires the fenced layer dispatch"
                )
            attempt_id = attempt
        else:
            attempt_id = attempt["attempt_id"]
        rows = _task_result_rows(
            spark, config, inputs, name, attempt_id, reference=pinned
        )
        if len(rows) != 1 or rows[0]["status"] != "SUCCEEDED":
            raise RuntimeError(
                "Required Gold object attempt is missing, ambiguous or failed"
            )
        document = json.loads(rows[0]["result_json"])
        if not isinstance(attempt, str) and (
            document["fence"] != attempt["fence"]
            or document["job_id"] != attempt["job_id"]
        ):
            raise RuntimeError(
                "Gold result differs from the prepared attempt/fence/job"
            )
        references[name] = {
            **pinned,
            "gold_release_id": preparation["gold_release_id"],
            "task_id": inputs["_tasks"][name]["task_id"],
            "attempt_id": attempt_id,
            "result_sha256": _gold_digest(document),
            "fence": document["fence"],
            "job_id": document["job_id"],
        }
    return references


def load_gold_result(
    spark: Any,
    config: dict[str, Any],
    preparation: GoldPreparationReference,
    object_name: str,
    reference: GoldResultReference,
) -> dict[str, Any]:
    inputs = _prepared_for_task(spark, config, preparation)
    if object_name not in inputs["_tasks"]:
        raise RuntimeError("Gold output was not declared in the prepared release")
    task = inputs["_tasks"][object_name]
    if (
        reference["gold_release_id"] != preparation["gold_release_id"]
        or reference["task_id"] != task["task_id"]
    ):
        raise RuntimeError("Gold result belongs to another release/task")
    rows = _task_result_rows(
        spark, config, inputs, object_name, reference["attempt_id"], reference=reference
    )
    if len(rows) != 1 or rows[0]["status"] != "SUCCEEDED":
        raise RuntimeError("Gold result is missing, ambiguous or failed")
    result = json.loads(rows[0]["result_json"])
    if (
        _gold_digest(result) != reference["result_sha256"]
        or result["input_sha256"] != preparation["input_sha256"]
        or result["execution"] != inputs["_execution"]
        or result["task"] != task
        or result["status"] != "SUCCEEDED"
        or result["gold_release_id"] != reference["gold_release_id"]
        or result["object_name"] != object_name
        or result["attempt_id"] != reference["attempt_id"]
        or result["fence"] != reference["fence"]
        or result["job_id"] != reference["job_id"]
    ):
        raise RuntimeError("Gold result implementation/configuration identity changed")
    _reload_gold_table(spark, result["output"], task["target"], current=True)
    return result


def dispatch_gold_transformation(
    transformation: Mapping[str, Any],
    implementations: Mapping[str, RegisteredGoldTransformation],
    context: dict[str, Any],
) -> Any:
    """Dispatch reviewed SQL/notebook adapters, never metadata eval/exec.

    SQL adapters must perform engine analysis and bind the supplied pinned inputs;
    notebook adapters must reopen persisted outputs, not pass frames from another
    interpreter. No generic arbitrary-SQL or notebook fallback is installed.
    """
    definition = transformation["definition"]
    if _gold_digest(definition) != transformation["definition_sha256"]:
        raise RuntimeError("Gold executable definition hash changed")
    kind = transformation["kind"]
    if kind not in {"sql", "notebook"}:
        raise RuntimeError("Unsupported specialist Gold transformation kind")
    implementation = implementations.get(transformation.get("id", ""))
    if implementation is None or any(
        (
            implementation["kind"] != kind,
            implementation["version"] != transformation["version"],
            implementation["definition_sha256"] != transformation["definition_sha256"],
            implementation["implementation_sha256"]
            != definition.get("implementation_sha256"),
            implementation["approval_id"] != definition.get("approval_id"),
            not callable(implementation["execute"]),
        )
    ):
        raise RuntimeError(
            "Gold specialist approval, version or implementation is unavailable"
        )
    _gold_token(
        definition.get("approval_id"),
        "Gold approval reference",
        r"[A-Za-z0-9_.-]{1,128}",
    )
    _gold_token(
        definition.get("implementation_sha256"),
        "Gold implementation hash",
        r"[0-9a-f]{64}",
    )
    if kind == "sql":
        if definition.get("dialect") != "spark_sql" or not definition.get("query"):
            raise RuntimeError("Gold SQL requires an approved Spark SQL query contract")
        _gold_token(
            definition.get("validation_sha256"),
            "Gold SQL engine validation evidence",
            r"[0-9a-f]{64}",
        )
    elif not definition.get("notebook_name"):
        raise RuntimeError(
            "Gold notebook requires a versioned implementation reference"
        )
    if definition.get("inputs") != sorted(context["input_references"]):
        raise RuntimeError(
            "Gold specialist input contract differs from pinned input references"
        )
    columns = definition.get("output_columns")
    if (
        not isinstance(columns, list)
        or not columns
        or any(
            not isinstance(column, dict) or set(column) != {"name", "data_type"}
            for column in columns
        )
    ):
        raise RuntimeError("Gold specialist output columns/types must be explicit")
    frame = implementation["execute"](context)
    if [(field.name, field.dataType.simpleString()) for field in frame.schema] != [
        (column["name"], column["data_type"]) for column in columns
    ]:
        raise RuntimeError(
            "Gold specialist returned undeclared output columns or types"
        )
    return frame


def _guard_gold_target(spark, inputs, name, target):
    before = inputs["_base_outputs"].get(name)
    if before is None:
        if spark.catalog.tableExists(target):
            raise RuntimeError(
                "An unrecorded target write requires explicit Gold recovery"
            )
    else:
        _reload_gold_table(spark, before, target, current=True)


def validate_gold_quality(spark, frame, config, object_name, input_references):
    """Evaluate declared runtime checks without filtering or changing business rows."""
    from pyspark.sql import functions as F

    metadata = config.get("_metadata", {})
    object_id = metadata.get("tasks", {}).get(object_name, {}).get("object_id")
    rules = [
        {key: value for key, value in rule.items() if key != "object_id"}
        for rule in metadata.get("rules", [])
        if rule["object_id"] == object_id
    ]
    evidence = []
    if rules:
        handlers = globals().get("_MEDALLION_RULE_HANDLERS")
        if handlers is None or any(
            rule["action"] not in {"fail", "warn"} for rule in rules
        ):
            raise RuntimeError(
                "Gold quality cannot discard rows or execute unregistered validators"
            )
        checked = handlers["apply_rules"](
            frame.withColumn("_dq_reasons", F.array().cast("array<string>")),
            {"rule_specs": rules},
            scope="gold",
        )
        for rule in rules:
            field = "_dq_warnings" if rule["action"] == "warn" else "_dq_reasons"
            count = checked.filter(
                F.array_contains(F.col(field), f"{rule['id']}@{rule['version']}")
            ).count()
            evidence.append(
                {
                    "id": rule["id"],
                    "version": rule["version"],
                    "action": rule["action"],
                    "violations": count,
                }
            )
            if count and rule["action"] == "fail":
                raise RuntimeError(f"Gold blocking quality rule failed: {rule['id']}")
    for constraint in metadata.get("constraints", []):
        if constraint["object_id"] != object_id:
            continue
        if constraint["enforcement"] != "runtime":
            raise RuntimeError("Engine-enforced Gold constraints are not inferred")
        definition, kind = constraint["definition"], constraint["kind"]
        if kind == "check":
            if (
                set(definition) != {"rule"}
                or definition["rule"].get("action") != "fail"
            ):
                raise ValueError(
                    "Gold check constraint requires one blocking versioned rule"
                )
            nested = {
                "_metadata": {
                    "tasks": {object_name: {"object_id": object_id}},
                    "rules": [dict(definition["rule"]) | {"object_id": object_id}],
                },
            }
            rows = validate_gold_quality(
                spark, frame, nested, object_name, input_references
            )
            violations = sum(row["violations"] for row in rows)
        else:
            columns = definition.get("columns")
            if (
                not isinstance(columns, list)
                or not columns
                or len(set(columns)) != len(columns)
            ):
                raise ValueError("Gold constraints require distinct declared columns")
            if not set(columns) <= set(frame.columns):
                raise ValueError("Gold constraint references absent output columns")
            if kind == "unique" and set(definition) == {"columns"}:
                nulls = reduce(
                    lambda left, right: left | right,
                    [F.col(column).isNull() for column in columns],
                )
                violations = (
                    frame.filter(nulls).count()
                    + frame.groupBy(*columns).count().filter(F.col("count") > 1).count()
                )
            elif kind in {"relationship", "reconciliation"} and set(definition) == {
                "columns",
                "reference_table",
                "reference_columns",
            }:
                references = [
                    reference
                    for reference in input_references.values()
                    if reference.get("table") == definition["reference_table"]
                ]
                if len(references) != 1 or len(columns) != len(
                    definition["reference_columns"]
                ):
                    raise RuntimeError(
                        "Gold constraint requires an explicitly pinned upstream reference"
                    )
                parent = _reload_gold_table(
                    spark, references[0], definition["reference_table"]
                )
                if not set(definition["reference_columns"]) <= set(parent.columns):
                    raise ValueError(
                        "Gold constraint references absent upstream columns"
                    )
                right = parent.select(
                    *[
                        F.col(old).alias(new)
                        for new, old in zip(
                            columns, definition["reference_columns"], strict=True
                        )
                    ]
                )
                left = frame.select(*columns)
                if kind == "relationship":
                    violations = left.join(right, columns, "left_anti").count()
                else:
                    violations = (
                        left.exceptAll(right).count() + right.exceptAll(left).count()
                    )
            else:
                raise RuntimeError("Unsupported Gold constraint contract")
        evidence.append(
            {
                "id": constraint["id"],
                "kind": kind,
                "enforcement": "runtime",
                "violations": violations,
            }
        )
        if violations:
            raise RuntimeError(f"Gold runtime constraint failed: {constraint['id']}")
    return evidence


def _gold_calendar(spark, config, inputs, dependencies):
    from pyspark.sql import functions as F

    dates = []
    for spec in config["facts"]:
        frame = _reload_gold_table(
            spark,
            dependencies[spec["name"]]["output"],
            _gold_target(config, spec["name"]),
            current=True,
        )
        dates.extend(
            [
                frame.select("event_date"),
                frame.filter(F.col("posting_month").isNotNull()).select(
                    F.to_date(
                        F.concat(F.col("posting_month").cast("string"), F.lit("01")),
                        "yyyyMMdd",
                    ).alias("event_date")
                ),
            ]
        )
        for role in spec.get("date_roles", {}):
            dates.append(
                frame.filter(F.col(role + "_sk") > 0).select(
                    F.to_date(F.col(role + "_sk").cast("string"), "yyyyMMdd").alias(
                        "event_date"
                    )
                )
            )
        if spec["snapshot"]:
            dates.append(frame.select(F.col("as_of_date").alias("event_date")))
    if "dim_date" in inputs["_base_outputs"]:
        dates.append(
            _reload_gold_table(
                spark,
                inputs["_base_outputs"]["dim_date"],
                _gold_target(config, "dim_date"),
            )
            .filter("date_sk <> 0")
            .select(F.col("calendar_date").alias("event_date"))
        )
    if not dates:
        dates.append(spark.createDataFrame([], "event_date date"))
    return calendar_frames(
        spark, reduce(lambda a, b: a.unionByName(b), dates), config.get("calendar")
    )


def _write_gold_result(spark, config, result):
    row = (
        result["gold_release_id"],
        result["input_sha256"],
        result["object_name"],
        result["task"]["task_id"],
        result["attempt_id"],
        result["status"],
        _gold_json(result),
        datetime.now(UTC),
    )
    spark.createDataFrame([row], GOLD_TASK_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(_gold_target(config, "ctl_gold_task"))
    return {
        **_gold_table_reference(spark, _gold_target(config, "ctl_gold_task")),
        "gold_release_id": result["gold_release_id"],
        "task_id": result["task"]["task_id"],
        "attempt_id": result["attempt_id"],
        "result_sha256": _gold_digest(result),
        "fence": result["fence"],
        "job_id": result["job_id"],
    }


def build_gold_object(
    spark: Any,
    fs: Any,
    config: dict[str, Any],
    preparation: GoldPreparationReference,
    object_name: str,
    ingestion: Any,
    *,
    attempt_id: str,
    fence: int,
    job_id: str,
    code_sha256: str,
    dependency_results: Mapping[str, GoldResultReference] | None = None,
    implementations: Mapping[str, RegisteredGoldTransformation] | None = None,
    ownership_check: Callable[[], None] | None = None,
) -> GoldResultReference:
    """Build one fenced object from persisted versions and persist its result."""
    _gold_token(attempt_id, "Gold attempt ID", r"[A-Za-z0-9_-]{1,128}")
    _gold_token(job_id, "Gold job ID", r"[A-Za-z0-9_-]{1,128}")
    if type(fence) is not int or not 1 <= fence <= 2**63 - 1:
        raise ValueError("Gold writes require a positive task fence")
    inputs = _prepared_for_task(spark, config, preparation)
    if code_sha256 != inputs["_execution"]["code_sha256"]:
        raise RuntimeError("Gold worker code differs from the pinned implementation")
    if object_name not in inputs["_tasks"]:
        raise RuntimeError("Gold object is not part of the prepared release")
    task = inputs["_tasks"][object_name]
    references = dict(dependency_results or {})
    if set(references) != set(task["dependencies"]):
        raise RuntimeError(
            "Gold object requires all explicit persisted dependency results"
        )
    dependencies = {
        name: load_gold_result(spark, config, preparation, name, reference)
        for name, reference in references.items()
    }
    existing = _task_result_rows(spark, config, inputs, object_name, attempt_id)
    if existing:
        if len(existing) != 1 or existing[0]["status"] != "SUCCEEDED":
            raise RuntimeError(
                "A failed or ambiguous Gold attempt requires explicit new-attempt recovery"
            )
        reference = gold_result_references(
            spark,
            config,
            preparation,
            {
                object_name: {
                    "attempt_id": attempt_id,
                    "fence": fence,
                    "job_id": job_id,
                }
            },
        )[object_name]
        result = load_gold_result(spark, config, preparation, object_name, reference)
        if (
            result["fence"] != fence
            or result["job_id"] != job_id
            or result["dependency_outputs"]
            != {name: value["output"] for name, value in dependencies.items()}
        ):
            raise RuntimeError(
                "A Gold attempt cannot be rebound to a changed dispatch/dependency identity"
            )
        return reference
    if _ready_release(spark, config, preparation["gold_release_id"]):
        raise RuntimeError(
            "A completed Gold release cannot acquire another object attempt"
        )
    resources = []
    result = {
        "gold_release_id": preparation["gold_release_id"],
        "input_sha256": preparation["input_sha256"],
        "object_name": object_name,
        "task": task,
        "execution": inputs["_execution"],
        "attempt_id": attempt_id,
        "fence": fence,
        "job_id": job_id,
        "dependency_outputs": {
            name: value["output"] for name, value in dependencies.items()
        },
        "status": "FAILED",
        "output": None,
        "error_code": None,
    }
    try:
        contexts, _serialized = _gold_contexts(
            fs,
            config,
            inputs["_selection"]["runs"],
            ingestion,
            inputs["_contexts"] or None,
        )
        as_of = date.fromisoformat(inputs["_selection"]["as_of_date"])
        source_inputs = task.get(
            "source_inputs",
            (
                []
                if task["kind"] == "calendar"
                else [
                    {"source": task["spec"]["source"], "table": task["spec"]["table"]}
                ]
            ),
        )
        if task["kind"] == "calendar":
            key = "date_sk" if object_name == "dim_date" else "fiscal_period_sk"
            input_references = {
                name: value["output"] for name, value in dependencies.items()
            }
            histories, snapshots = {}, {}
            if source_inputs:
                _manifest, histories, snapshots = _gold_inputs(
                    spark,
                    fs,
                    config,
                    inputs["_selection"]["runs"],
                    ingestion,
                    inputs["_bronze_base"],
                    resources,
                    source_contexts=contexts,
                    pinned_manifest=inputs,
                    required={(ref["source"], ref["table"]) for ref in source_inputs},
                )
                input_references.update(
                    _gold_source_references(config, inputs, source_inputs)
                )
            if task["transformation"]["kind"] == "pattern":
                frame = _gold_calendar(spark, config, inputs, dependencies)[object_name]
            else:
                frame = dispatch_gold_transformation(
                    task["transformation"],
                    implementations or {},
                    {
                        "spark": spark,
                        "config": config,
                        "spec": task["spec"],
                        "dependencies": dependencies,
                        "histories": histories,
                        "snapshots": snapshots,
                        "input_references": input_references,
                        "target": task["target"],
                        "as_of_date": as_of,
                        "gold_release_id": preparation["gold_release_id"],
                    },
                )
        else:
            spec = task["spec"]
            _manifest, histories, snapshots = _gold_inputs(
                spark,
                fs,
                config,
                inputs["_selection"]["runs"],
                ingestion,
                inputs["_bronze_base"],
                resources,
                source_contexts=contexts,
                pinned_manifest=inputs,
                required={(ref["source"], ref["table"]) for ref in source_inputs},
            )
            input_references = {
                **_gold_source_references(config, inputs, source_inputs),
                **{name: value["output"] for name, value in dependencies.items()},
            }
            if task["transformation"]["kind"] == "pattern":
                if task["kind"] == "scd2_dimension":
                    frame = build_scd2(histories[spec["source"], spec["table"]], spec)
                else:
                    dimensions = {
                        name: _reload_gold_table(
                            spark,
                            value["output"],
                            _gold_target(config, name),
                            current=True,
                        )
                        for name, value in dependencies.items()
                        if name in {d["name"] for d in config["dimensions"]}
                    }
                    frame = build_fact(
                        (snapshots if spec["snapshot"] else histories)[
                            spec["source"], spec["table"]
                        ],
                        spec,
                        dimensions,
                        config,
                        as_of,
                        preparation["gold_release_id"],
                    )
            else:
                frame = dispatch_gold_transformation(
                    task["transformation"],
                    implementations or {},
                    {
                        "spark": spark,
                        "config": config,
                        "spec": spec,
                        "histories": histories,
                        "snapshots": snapshots,
                        "dependencies": dependencies,
                        "input_references": input_references,
                        "target": task["target"],
                        "as_of_date": as_of,
                        "gold_release_id": preparation["gold_release_id"],
                    },
                )
            key = "dimension_sk" if task["kind"] == "scd2_dimension" else "fact_sk"
        frame = frame.cache()
        resources.append(frame)
        quality = validate_gold_quality(
            spark, frame, config, object_name, input_references
        )
        _guard_gold_target(spark, inputs, object_name, task["target"])
        if ownership_check is not None:
            ownership_check()
        _merge_gold(
            spark,
            task["target"],
            frame,
            key,
            as_of=as_of if task["kind"] == "snapshot_fact" else None,
            **(
                {
                    "enforce_layout": True,
                    "layout": config["_metadata"]["layouts"].get(object_name),
                    "layout_capabilities": config["_metadata"].get("layout_runtime"),
                }
                if "_metadata" in config
                else {}
            ),
        )
        output = _gold_table_reference(spark, task["target"], count=True)
        persisted = _reload_gold_table(spark, output, task["target"], current=True)
        if task["kind"] == "snapshot_fact":
            from pyspark.sql import functions as F

            persisted = persisted.filter(F.col("as_of_date") == F.lit(as_of))
        columns = [
            name
            for name in frame.columns
            if name not in {"gold_release_id", "gold_loaded_utc", "_gold_content_hash"}
        ]
        if (
            frame.select(*columns)
            .exceptAll(persisted.select(*columns))
            .limit(1)
            .count()
            or persisted.select(*columns)
            .exceptAll(frame.select(*columns))
            .limit(1)
            .count()
        ):
            raise RuntimeError(
                "Persisted Gold object does not reconcile to its pinned inputs"
            )
        result.update(
            status="SUCCEEDED",
            output=output,
            input_references=input_references,
            selected_row_count=frame.count(),
            validation="EXACT_BUSINESS_ROW_EQUIVALENCE",
            quality_evidence=quality,
        )
        if ownership_check is not None:
            ownership_check()
        return _write_gold_result(spark, config, result)
    except Exception as error:
        result.update(
            status="FAILED", error_code="GOLD_" + type(error).__name__.upper()
        )
        _write_gold_result(spark, config, result)
        _release_event(
            spark,
            config,
            preparation["gold_release_id"],
            "FAILED",
            date.fromisoformat(inputs["_selection"]["as_of_date"]),
            inputs,
            {},
            f"Gold object failed: {type(error).__name__}; inspect task-scoped controls",
        )
        raise
    finally:
        for frame in resources:
            frame.unpersist()


def fail_gold_release(
    spark: Any,
    config: dict[str, Any],
    preparation: GoldPreparationReference,
) -> None:
    """Persist native DAG/submission failure without implying partial readiness."""
    inputs = load_gold_preparation(spark, config, preparation)
    _release_event(
        spark,
        config,
        preparation["gold_release_id"],
        "FAILED",
        date.fromisoformat(inputs["_selection"]["as_of_date"]),
        inputs,
        {},
        "Gold DAG failed; technical release readiness is withheld",
    )


def finalize_gold(
    spark: Any,
    config: dict[str, Any],
    preparation: GoldPreparationReference,
    results: Mapping[str, GoldResultReference],
    *,
    ownership_check: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Reopen and validate the entire output set before technical FACTS_READY."""
    inputs = load_gold_preparation(spark, config, preparation)
    if set(results) != set(inputs["_tasks"]):
        raise RuntimeError(
            "Partial or undeclared Gold release membership cannot become ready"
        )
    try:
        verified = {
            name: load_gold_result(spark, config, preparation, name, reference)
            for name, reference in results.items()
        }
        for result in verified.values():
            if result["dependency_outputs"] != {
                name: verified[name]["output"]
                for name in result["task"]["dependencies"]
            }:
                raise RuntimeError(
                    "Gold dependency versions changed between object build and finalization"
                )
        ready = _ready_release(spark, config, preparation["gold_release_id"])
        if ready:
            if (
                _gold_digest(json.loads(ready["input_manifest_json"]))
                != preparation["input_sha256"]
            ):
                raise RuntimeError("Completed Gold release input identity changed")
            outputs = json.loads(ready["output_manifest_json"])
            _validate_gold_outputs(spark, outputs)
            return outputs
        target = _gold_target(config, "ctl_measure_availability")
        _guard_gold_target(spark, inputs, "ctl_measure_availability", target)
        if ownership_check is not None:
            ownership_check()
        availability = spark.createDataFrame(
            [(name, "BLOCKED", reason) for name, reason in config["blocked"].items()],
            "measure_name string, availability_status string, reason string",
        )
        _merge_gold(spark, target, availability, "measure_name")
        outputs = {
            name: {**result["output"], "result_reference": dict(results[name])}
            for name, result in verified.items()
        }
        outputs["ctl_measure_availability"] = _gold_table_reference(
            spark, target, count=True
        )
        _validate_gold_outputs(spark, outputs)
        if ownership_check is not None:
            ownership_check()
        _release_event(
            spark,
            config,
            preparation["gold_release_id"],
            "FACTS_READY",
            date.fromisoformat(inputs["_selection"]["as_of_date"]),
            inputs,
            outputs,
        )
        return outputs
    except Exception:
        fail_gold_release(spark, config, preparation)
        raise


def run_gold(
    spark, fs, config, source_runs_json, gold_release_id, ingestion, bronze_base
):
    """Legacy sequential wrapper; normal metadata runs must use the object DAG."""
    if "_metadata" in config:
        raise RuntimeError(
            "Normal Gold uses prepare_gold, object DAG workers and finalize_gold"
        )
    runs, as_of = gold_request(config, source_runs_json, gold_release_id)
    ready = _ready_release(spark, config, gold_release_id)
    if ready and "_execution" not in json.loads(ready["input_manifest_json"]):
        expected = {
            "runs": runs,
            "as_of_date": as_of.isoformat(),
            "contract_hash": config["contract_hash"],
        }
        if json.loads(ready["input_manifest_json"]).get("_selection") != expected:
            raise RuntimeError("A historical completed Gold release cannot be rebound")
        outputs = json.loads(ready["output_manifest_json"])
        _validate_gold_outputs(spark, outputs)
        return outputs
    preparation = prepare_gold(
        spark, fs, config, source_runs_json, gold_release_id, ingestion, bronze_base
    )
    tasks = load_gold_preparation(spark, config, preparation)["_tasks"]
    results = {}
    pending = set(tasks)
    while pending:
        eligible = [
            name
            for name in tasks
            if name in pending and set(tasks[name]["dependencies"]) <= set(results)
        ]
        for name in eligible:
            results[name] = build_gold_object(
                spark,
                fs,
                config,
                preparation,
                name,
                ingestion,
                attempt_id=hashlib.sha256(
                    ("legacy:" + gold_release_id).encode()
                ).hexdigest(),
                fence=1,
                job_id=gold_release_id,
                code_sha256=config["runtime_sha256"],
                dependency_results={
                    parent: results[parent] for parent in tasks[name]["dependencies"]
                },
            )
            pending.remove(name)
    return finalize_gold(spark, config, preparation, results)


def register_gold_mlvs(spark, config, gold_release_id):
    """Explicit SQL MLV authoring only; recurring refresh belongs to Lakehouse schedules."""
    if "_metadata" in config:
        raise RuntimeError(
            "Pinned metadata MLV execution must use the change-aware lifecycle and serving ownership contract"
        )
    if config["execution_mode"] != "synthetic_demo":
        raise ValueError(
            "Production MLV authoring requires separately certified Gold contracts"
        )
    ready = _ready_release(spark, config, gold_release_id)
    if not ready or ready["contract_hash"] != config["contract_hash"]:
        raise RuntimeError(
            "MLV creation requires an explicit matching FACTS_READY Gold release"
        )
    for name, entry in json.loads(ready["output_manifest_json"]).items():
        current = (
            _gold_delta(spark, entry["table"])
            .history(1)
            .select("version")
            .first()["version"]
        )
        if current != entry["version"]:
            raise RuntimeError(
                f"{name}: current serving tables no longer match the selected release"
            )
    outputs = json.loads(ready["output_manifest_json"])
    for name, statement in config["mlvs"].items():
        spark.sql(statement)
        target = _gold_target(config, name)
        if not spark.catalog.tableExists(target):
            raise RuntimeError(f"MLV registration did not materialize {name}")
        expected = spark.sql("SELECT " + statement.split("\nAS SELECT ", 1)[1])
        actual = spark.table(target).select(*expected.columns)
        if (
            expected.exceptAll(actual).limit(1).count()
            or actual.exceptAll(expected).limit(1).count()
        ):
            raise RuntimeError(
                f"{name}: materialized aggregate does not reconcile to canonical facts"
            )
        outputs[name] = {
            "table": target,
            "version": _gold_delta(spark, target)
            .history(1)
            .select("version")
            .first()["version"],
            "row_count": actual.count(),
        }
    _release_event(
        spark,
        config,
        gold_release_id,
        "MLVS_VERIFIED",
        ready["as_of_date"],
        json.loads(ready["input_manifest_json"]),
        outputs,
        "Technical demo aggregates only; not business publication",
    )
