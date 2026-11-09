"""Resolve declared use-case assets without directory searches or domain defaults."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .use_cases import load_use_case


def local_asset_path(root: Path, relative: str) -> Path:
    """Resolve a portable, repository-relative path without escaping its root."""
    if (
        not isinstance(relative, str)
        or not relative
        or relative.replace("\\", "/").startswith("/")
        or Path(relative).is_absolute()
        or Path(relative).drive
    ):
        raise ValueError(f"Asset path must be relative to its root: {relative!r}")
    path = (root / Path(*relative.replace("\\", "/").split("/"))).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Asset path escapes its root: {relative!r}")
    return path


def use_case_descriptor(root: Path, use_case: str | None = None) -> dict[str, Any]:
    """Load selected profile bindings and its explicit provenance sidecar, if any.

    All sidecar asset paths are repository-relative. The bootstrap registry itself
    is selected only by the catalog/profile; a sidecar cannot replace that choice.
    """
    profile = load_use_case(root, use_case)
    relative = profile.inputs["source_registry"]
    registry = profile.path(relative) if relative is not None else None
    bindings: dict[str, Any] = {}
    if registry is not None:
        sidecar = registry.with_name("provenance_rebaseline.json")
        if sidecar.is_file():
            provenance = json.loads(sidecar.read_text(encoding="utf-8"))
            if provenance["schema_version"] != 1:
                raise ValueError("Unsupported asset provenance sidecar version")
            if provenance.get("use_case", profile.case_id) != profile.case_id:
                raise ValueError("Asset provenance sidecar belongs to another use case")
            bindings = provenance.get("asset_bindings", {})
    return {
        **bindings,
        "id": profile.case_id,
        "resources": profile.resources,
        "source_registry": (
            registry.relative_to(root.resolve()).as_posix()
            if registry is not None else None
        ),
    }


def resolve_asset_path(
    root: Path, relative: str, use_case: str | None = None
) -> Path:
    """Resolve explicit logical aliases; reject competing physical authorities."""
    descriptor = use_case_descriptor(root, use_case)
    logical = relative.replace("\\", "/").rstrip("/")
    targets = set()
    for old, new in descriptor.get("asset_aliases", {}).items():
        old = old.replace("\\", "/").rstrip("/")
        if logical == old or logical.startswith(old + "/"):
            targets.add(new.replace("\\", "/").rstrip("/") + logical[len(old):])
    if len(targets) > 1:
        raise ValueError(f"Ambiguous asset aliases: {relative}")
    physical = next(iter(targets), logical)
    path = local_asset_path(root, physical)
    # Include inverse aliases so callers using either spelling get the same check.
    candidates = {path, local_asset_path(root, logical)}
    for old, new in descriptor.get("asset_aliases", {}).items():
        new = new.replace("\\", "/").rstrip("/")
        if physical == new or physical.startswith(new + "/"):
            candidates.add(local_asset_path(root, old + physical[len(new):]))
    present = [candidate for candidate in candidates if candidate.exists()]
    if len(present) > 1:
        raise ValueError(f"Ambiguous asset authorities: {relative}")
    return present[0] if present else path


def _declared_path(root: Path, name: str, use_case: str | None) -> Path:
    descriptor = use_case_descriptor(root, use_case)
    if not descriptor.get(name):
        raise ValueError(f"Use case {descriptor['id']} does not declare {name}")
    return resolve_asset_path(root, descriptor[name], descriptor["id"])


def source_registry_path(root: Path, use_case: str | None = None) -> Path:
    """Return the selected bootstrap registry, checking explicit historical aliases."""
    return _declared_path(root, "source_registry", use_case)


def source_registry_root(root: Path, use_case: str | None = None) -> Path:
    """Return the directory containing the selected bootstrap registry."""
    return source_registry_path(root, use_case).parent


def source_specifications_root(root: Path, use_case: str | None = None) -> Path:
    """Return the declared Docling evidence root, never an arbitrary search result."""
    return _declared_path(root, "source_specifications", use_case)


def department_profile_path(root: Path, use_case: str | None = None) -> Path:
    """Return an explicitly declared department profile, if this case has one."""
    return _declared_path(root, "department_profile", use_case)


def synthetic_data_root(root: Path, use_case: str | None = None) -> Path:
    """Return the declared local synthetic fixture root, if this case has one."""
    return _declared_path(root, "synthetic_data", use_case)
