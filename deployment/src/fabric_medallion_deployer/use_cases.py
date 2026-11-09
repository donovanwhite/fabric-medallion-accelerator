"""Case ownership and trusted extension discovery, separate from shared architecture."""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

_CASE = re.compile(r"[a-z][a-z0-9_]{0,62}\Z")
_ITEM = re.compile(r"[a-z][a-z0-9_]{0,127}\Z")


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path}: expected an object")
    return value


def selected_use_case_id(root: Path, use_case: str | None = None) -> str:
    catalog = _read(root / "use_cases" / "catalog.json")
    if (
        set(catalog) != {"schema_version", "default_use_case", "use_cases"}
        or catalog["schema_version"] != 1
    ):
        raise ValueError("Unsupported use-case catalog contract")
    cases = catalog["use_cases"]
    if (
        not isinstance(cases, list)
        or not cases
        or any(not isinstance(case, str) or not _CASE.fullmatch(case) for case in cases)
        or len(set(cases)) != len(cases)
        or catalog["default_use_case"] not in cases
    ):
        raise ValueError(
            "Use-case catalog must declare distinct case IDs and a valid default"
        )
    identity = use_case if use_case is not None else catalog["default_use_case"]
    if identity not in cases:
        raise ValueError(
            f"Unknown use case: {identity!r}; register its descriptor before generation"
        )
    return identity


@dataclass(frozen=True)
class UseCaseProfile:
    root: Path
    case_id: str
    directory: Path
    adapter: str
    resources: dict[str, str]
    inputs: dict[str, str | None]
    documents: dict[str, str]
    baseline_version: str

    @property
    def generated(self) -> Path:
        path = self.directory / "generated"
        if not path.resolve().is_relative_to(self.directory.resolve()):
            raise ValueError("Generated products cannot escape their owning case")
        return path

    def path(self, relative: str) -> Path:
        if (
            not isinstance(relative, str)
            or "\\" in relative
            or not relative
            or Path(relative).is_absolute()
        ):
            raise ValueError(
                "Case references must use bounded relative POSIX-style paths"
            )
        path = (self.directory / relative).resolve()
        if ".." in Path(relative).parts or not path.is_relative_to(
            self.directory.resolve()
        ):
            raise ValueError("Case reference escapes its owning use case")
        return path

    def input_path(self, name: str) -> Path:
        value = self.inputs.get(name)
        if value is None:
            raise ValueError(f"{self.case_id}: input {name!r} is not configured")
        return self.path(value)

    def notebook_names(self) -> dict[str, str]:
        prefix = self.resources["item_prefix"]
        stem = f"nb_{prefix}_" if prefix else "nb_"
        return {
            "prepare": stem + "prepare_medallion_run",
            "silver": stem + "orchestrate_silver",
            "worker": stem + "silver_worker",
            "gold": stem + "orchestrate_gold",
            "gold_worker": stem + "gold_worker",
        }

    @property
    def mlv_notebook(self) -> str:
        prefix = self.resources["item_prefix"]
        return (
            f"nb_{prefix}_gold_mlv_definitions" if prefix else "nb_gold_mlv_definitions"
        )

    def pipeline_names(self) -> tuple[str, str]:
        prefix = self.resources["item_prefix"]
        stem = f"pl_{prefix}_" if prefix else "pl_"
        return stem + "copy_bronze_batch", stem + "bronze_silver_gold"


def load_use_case(root: Path, use_case: str | None = None) -> UseCaseProfile:
    identity = selected_use_case_id(root, use_case)
    directory = root / "use_cases" / identity
    if not directory.resolve().is_relative_to((root / "use_cases").resolve()):
        raise ValueError("Use-case directory escapes the registered repository area")
    document = _read(directory / "use_case.json")
    fields = {
        "schema_version",
        "id",
        "adapter",
        "baseline_version",
        "resources",
        "inputs",
        "documents",
    }
    if (
        set(document) != fields
        or document["schema_version"] != 1
        or document["id"] != identity
    ):
        raise ValueError("Use-case descriptor fields/identity differ from the catalog")
    if document["adapter"] not in {"snapshot_bootstrap", "compiled"}:
        raise ValueError("Unknown use-case adapter")
    resources = document["resources"]
    if (
        not isinstance(resources, dict)
        or set(resources)
        != {"bronze_silver", "gold", "control_database", "item_prefix"}
        or any(
            not isinstance(resources[key], str) or not _ITEM.fullmatch(resources[key])
            for key in ("bronze_silver", "gold", "control_database")
        )
        or not isinstance(resources["item_prefix"], str)
        or (resources["item_prefix"] and not _CASE.fullmatch(resources["item_prefix"]))
        or len(
            {
                resources["bronze_silver"],
                resources["gold"],
                resources["control_database"],
            }
        )
        != 3
    ):
        raise ValueError("Case resource names must be distinct safe identifiers")
    inputs = document["inputs"]
    documents = document["documents"]
    if not isinstance(inputs, dict) or set(inputs) != {
        "source_registry",
        "gold_profile",
        "source_rules",
        "gold_rules",
        "compiled_metadata",
    }:
        raise ValueError("Use-case input fields differ from the supported contract")
    if document["adapter"] == "compiled" and inputs != {
        "source_registry": None,
        "gold_profile": None,
        "source_rules": "generated/extensions/source_rules.py",
        "gold_rules": "generated/extensions/gold_rules.py",
        "compiled_metadata": "generated/metadata/definition.json",
    }:
        raise ValueError(
            "Compiled cases must use their compiler-owned generated contract/adapter paths"
        )
    if not isinstance(documents, dict) or set(documents) != {
        "requirements",
        "sources",
        "gold",
        "plan",
    }:
        raise ValueError("Use-case document fields differ from the supported contract")
    if not isinstance(document["baseline_version"], str) or not re.fullmatch(
        r"\d+\.\d+\.\d+", document["baseline_version"]
    ):
        raise ValueError("A baseline semantic version is required")
    profile = UseCaseProfile(
        root.resolve(),
        identity,
        directory.resolve(),
        document["adapter"],
        resources,
        inputs,
        documents,
        document["baseline_version"],
    )
    for value in [*inputs.values(), *documents.values()]:
        if value is not None:
            profile.path(value)
    return profile


def load_case_extension(profile: UseCaseProfile, name: str) -> ModuleType:
    """Load reviewed case-owned code; never evaluate Python extracted from Markdown."""
    path = profile.input_path(name)
    if path.suffix != ".py" or not path.is_file():
        raise ValueError(f"{profile.case_id}: missing reviewed {name} extension")
    identity = f"accelerator_case_{profile.case_id}_{name}"
    spec = importlib.util.spec_from_file_location(identity, path)
    if spec is None or spec.loader is None:
        raise ValueError("Cannot load the declared case extension")
    module = importlib.util.module_from_spec(spec)
    sys.modules[identity] = module
    spec.loader.exec_module(module)
    return module
