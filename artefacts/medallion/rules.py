"""Reusable versioned rule engine; business rules are injected case-owned code."""

from __future__ import annotations

import json
import math
import re
from functools import reduce
from typing import Any


def _column(name: str):
    from pyspark.sql import functions as F

    return F.col(f"`{name}`")


def _case_rules():
    adapter = globals().get("_CASE_RULES")
    if adapter is None and "__file__" in globals():
        # Local compatibility wrapper follows the catalog, never a business default.
        from pathlib import Path

        from fabric_medallion_deployer.use_cases import (
            load_case_extension,
            load_use_case,
        )

        adapter = load_case_extension(
            load_use_case(Path(__file__).resolve().parents[2]), "source_rules"
        )
        globals()["_CASE_RULES"] = adapter
    if adapter is None:
        raise RuntimeError("The reviewed selected case source rules must be injected")
    return adapter


def source_business_checks(data: dict[str, Any], source: str):
    return _case_rules().source_business_checks(data, source)


def row_rules(table: dict[str, Any]):
    return _case_rules().row_rules(table)


def legacy_context_bindings(source: str) -> dict[str, str]:
    function = getattr(_case_rules(), "legacy_context_bindings", None)
    if function is None:
        raise ValueError(
            "Selected case has no legacy raw-path contract; use pinned manifests"
        )
    return function(source)


def validate_rule_specs(specs: object, *, scope: str) -> tuple[dict[str, Any], ...]:
    if (
        scope not in {"object", "source", "gold"}
        or not isinstance(specs, list)
        or not specs
    ):
        raise ValueError("Explicit versioned rule specifications are required")
    required = (
        {"legacy.prepare_rows", "legacy.relationship_checks", "legacy.silver_checks"}
        if scope == "object"
        else {"legacy.source_business_checks"}
        if scope == "source"
        else set()
    )
    generic = {
        "value_range",
        "required_columns",
        "equals_columns",
        "business_key_unique",
        "allowed_values",
    }
    allowed = required | (generic if scope != "source" else set())
    identities, handlers, result = set(), [], []
    for spec in specs:
        if not isinstance(spec, dict) or set(spec) != {
            "id",
            "handler",
            "version",
            "severity",
            "action",
            "parameters",
        }:
            raise ValueError("Rule fields differ from the supported runtime contract")
        if (
            not isinstance(spec["id"], str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", spec["id"])
            or spec["id"] in identities
            or spec["handler"] not in allowed
            or spec["version"] != "1"
        ):
            raise ValueError("Unknown, duplicate or unsupported rule version")
        identities.add(spec["id"])
        handlers.append(spec["handler"])
        if spec["severity"] not in {"error", "warning"} or spec["action"] not in {
            "fail",
            "quarantine",
            "warn",
        }:
            raise ValueError("Unsupported rule severity or action")
        if (spec["severity"] == "warning") != (spec["action"] == "warn"):
            raise ValueError("Rule warning severity and action must agree")
        if spec["handler"].startswith("legacy.") and (
            spec["severity"] != "error" or spec["action"] != "fail"
        ):
            raise ValueError(
                "Required baseline checks must retain their blocking behavior"
            )
        parameters = spec["parameters"]
        if not isinstance(parameters, dict):
            raise TypeError("Rule parameters must be structured")
        kind = spec["handler"]
        if kind.startswith("legacy."):
            if not set(parameters) <= {
                "scope",
                "legacy_contract_hash",
                "runtime_sha256",
                "source_code",
                "objects",
            }:
                raise ValueError("Unknown baseline rule parameter")
        else:
            fields = {
                "value_range": {"column", "minimum", "maximum", "allow_null"},
                "required_columns": {"columns"},
                "equals_columns": {"left", "right"},
                "business_key_unique": {"columns"},
                "allowed_values": {"column", "values", "allow_null"},
            }[kind]
            if not set(parameters) <= fields:
                raise ValueError(
                    "Unknown generic rule parameter; executable expressions are prohibited"
                )
            names = (
                parameters.get("columns")
                if kind in {"required_columns", "business_key_unique"}
                else [parameters.get("left"), parameters.get("right")]
                if kind == "equals_columns"
                else [parameters.get("column")]
            )
            if (
                not isinstance(names, list)
                or not names
                or len(set(map(str, names))) != len(names)
                or any(
                    not isinstance(name, str)
                    or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name)
                    for name in names
                )
            ):
                raise ValueError(
                    "Generic rules require distinct validated column names"
                )
            if (
                "allow_null" in parameters
                and type(parameters["allow_null"]) is not bool
            ):
                raise ValueError("Rule null behavior must be explicitly Boolean")
            if kind == "value_range":
                low, high = parameters.get("minimum"), parameters.get("maximum")
                if (
                    (low is None and high is None)
                    or any(
                        value is not None
                        and (
                            type(value) not in (int, float) or not math.isfinite(value)
                        )
                        for value in (low, high)
                    )
                    or (low is not None and high is not None and low > high)
                ):
                    raise ValueError("Range bounds must be finite ordered numbers")
            if kind == "allowed_values":
                values = parameters.get("values")
                if (
                    not isinstance(values, list)
                    or not 1 <= len(values) <= 1000
                    or any(
                        type(value) not in (str, int, float, bool) for value in values
                    )
                ):
                    raise ValueError(
                        "Allowed values must be a bounded explicit scalar list"
                    )
        result.append(json.loads(json.dumps(spec, allow_nan=False)))
    if not required <= set(handlers) or any(
        handlers.count(handler) != 1 for handler in required
    ):
        raise ValueError("Required baseline rule handlers are missing or duplicated")
    return tuple(result)


def apply_rules(frame, table: dict[str, Any], *, scope: str = "object"):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    profile = validate_rule_specs(table["rule_specs"], scope=scope)
    output = frame.withColumn("_dq_warnings", F.array().cast("array<string>"))
    for spec in profile:
        kind, parameters = spec["handler"], spec["parameters"]
        if kind.startswith("legacy."):
            continue
        columns = (
            parameters.get("columns")
            if kind in {"required_columns", "business_key_unique"}
            else [parameters["left"], parameters["right"]]
            if kind == "equals_columns"
            else [parameters["column"]]
        )
        if not set(columns) <= set(frame.columns):
            raise ValueError("Generic rule references an absent contracted column")
        if kind == "value_range":
            value = _column(parameters["column"])
            invalid = (
                value.isNull()
                if not parameters.get("allow_null", False)
                else F.lit(False)
            )
            if parameters.get("minimum") is not None:
                invalid = invalid | (value < F.lit(parameters["minimum"]))
            if parameters.get("maximum") is not None:
                invalid = invalid | (value > F.lit(parameters["maximum"]))
        elif kind == "required_columns":
            invalid = reduce(
                lambda left, right: left | right,
                [_column(name).isNull() for name in columns],
            )
        elif kind == "equals_columns":
            invalid = ~_column(parameters["left"]).eqNullSafe(
                _column(parameters["right"])
            )
        elif kind == "business_key_unique":
            invalid = (
                F.count(F.lit(1)).over(
                    Window.partitionBy(*[_column(name) for name in columns])
                )
                > 1
            )
        else:
            value = _column(parameters["column"])
            invalid = ~value.isin(parameters["values"])
            invalid = (
                invalid | value.isNull()
                if not parameters.get("allow_null", False)
                else invalid
            )
        label = f"{spec['id']}@{spec['version']}"
        target = "_dq_warnings" if spec["action"] == "warn" else "_dq_reasons"
        output = output.withColumn(
            target,
            F.when(
                F.coalesce(invalid, F.lit(False)),
                F.array_union(_column(target), F.array(F.lit(label))),
            ).otherwise(_column(target)),
        )
    return output


def blocking_reasons(table: dict[str, Any]):
    from pyspark.sql import functions as F

    quarantined = []
    if "rule_specs" in table:
        quarantined = [
            f"{spec['id']}@{spec['version']}"
            for spec in validate_rule_specs(table["rule_specs"], scope="object")
            if spec["action"] == "quarantine"
        ]
    reasons = _column("_dq_reasons")
    if not quarantined:
        return reasons
    return F.filter(reasons, lambda reason: ~reason.isin(quarantined))


_MEDALLION_RULE_HANDLERS = {
    "source_business_checks": source_business_checks,
    "row_rules": row_rules,
    "validate_rule_specs": validate_rule_specs,
    "apply_rules": apply_rules,
    "blocking_reasons": blocking_reasons,
    "legacy_context_bindings": legacy_context_bindings,
}
