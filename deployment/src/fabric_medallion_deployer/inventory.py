"""Reusable inventory and explicit source prerequisites for the selected case."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .use_cases import UseCaseProfile, load_use_case

MLV_NOTEBOOK = "nb_gold_mlv_definitions"
COPY_PIPELINE = "pl_copy_bronze_batch"
MASTER_PIPELINE = "pl_bronze_silver_gold"


class SourceBinding(Protocol):
    code: str
    database: str
    connection: str
    lakehouse: str
    gold_lakehouse: str


@dataclass(frozen=True)
class RuntimeSourceBinding:
    """Publication prerequisites, not a synthetic source/CSV importer."""

    code: str
    database: str
    connection: str
    lakehouse: str
    gold_lakehouse: str


def runtime_source_bindings(
    root: Path, use_case: str | None = None
) -> tuple[SourceBinding, ...]:
    from .contracts import load_sources
    from .orchestrator import bootstrap_configuration

    profile = load_use_case(root, use_case)
    if profile.adapter == "snapshot_bootstrap":
        return load_sources(root, profile.case_id)
    document = bootstrap_configuration(root, use_case=profile.case_id).to_document()
    connections = {item["id"]: item for item in document["connections"]}
    bindings = {}
    for feed in document["feeds"]:
        connection = connections[feed["connection_id"]]
        # Only Fabric SQL resolves an existing workspace SQLDatabase item.
        binding = RuntimeSourceBinding(
            feed["source_code"],
            feed["source_instance"]
            if connection["connector_type"] == "FabricSql"
            else "",
            connection["name"],
            profile.resources["bronze_silver"],
            profile.resources["gold"],
        )
        previous = bindings.get(binding.code)
        if previous is not None and previous != binding:
            raise ValueError(
                "One source code must have unambiguous publication prerequisites"
            )
        bindings[binding.code] = binding
    return tuple(bindings.values())


def reusable_inventory(
    sources: tuple[SourceBinding, ...] = (), *, profile: UseCaseProfile | None = None
) -> dict[str, str]:
    from .orchestrator import METADATA_NOTEBOOKS

    if profile is None:
        # Historical Python caller compatibility only; modern callers pass their profile.
        if not sources:
            raise ValueError(
                "A selected profile or explicit resource bindings are required"
            )
        resources = {
            "control_database": "db_orchestrator",
            "bronze_silver": sources[0].lakehouse,
            "gold": sources[0].gold_lakehouse,
        }
        names, mlv, pipelines = (
            METADATA_NOTEBOOKS,
            MLV_NOTEBOOK,
            (COPY_PIPELINE, MASTER_PIPELINE),
        )
    else:
        resources = profile.resources
        names, mlv, pipelines = (
            profile.notebook_names(),
            profile.mlv_notebook,
            profile.pipeline_names(),
        )
    return {
        resources["control_database"]: "SQLDatabase",
        resources["bronze_silver"]: "Lakehouse",
        resources["gold"]: "Lakehouse",
        **dict.fromkeys(names.values(), "Notebook"),
        mlv: "Notebook",
        **dict.fromkeys(pipelines, "DataPipeline"),
    }
