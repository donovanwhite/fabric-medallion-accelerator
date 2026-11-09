# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Case-owned Gold contracts and reusable notebook packaging."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from .contracts import Source
from .definitions import _code_cell, json_text, part, validate_deployable_definition
from .use_cases import load_case_extension, load_use_case

# Historical Python API aliases, not deployment inventory.
GOLD_NOTEBOOKS = {"mlvs": "nb_gold_mlv_definitions"}
GOLD_PIPELINE = "pl_silver_to_gold"


def _profile_module(root: Path, use_case: str | None = None):
    return load_case_extension(load_use_case(root, use_case), "gold_profile")


def __getattr__(name: str):
    """Compatibility for older semantic callers; no domain dictionaries in core."""
    if name in {"FACTS", "BLOCKED"}:
        return getattr(_profile_module(Path(__file__).resolve().parents[3]), name)
    raise AttributeError(name)


def dimension_specs(
    root: Path | None = None, use_case: str | None = None
) -> list[dict]:
    root = root or Path(__file__).resolve().parents[3]
    profile = load_use_case(root, use_case)
    if profile.adapter == "compiled":
        return gold_config(root, use_case=profile.case_id)["dimensions"]
    return _profile_module(root, profile.case_id).dimension_specs()


def bootstrap_gold_config(
    root: Path, sources: tuple[Source, ...], use_case: str | None = None
) -> dict[str, Any]:
    profile = load_use_case(root, use_case)
    if profile.adapter != "snapshot_bootstrap":
        from .orchestrator import bootstrap_configuration

        config = bootstrap_configuration(root, use_case=profile.case_id)
        contract = deepcopy(config.to_document()["extensions"]["gold_contract"])
        contract["contract_hash"] = config.to_document()["extensions"][
            "legacy_gold_contract_hash"
        ]
        contract["use_case_id"] = profile.case_id
        contract["runtime_sha256"] = next(
            worker.code_sha256
            for worker in config.workers
            if worker.id == "gold-worker"
        )
        contract["mlvs"] = {
            obj["name"]: obj["contract"]["mlv_query"]
            for obj in config.to_document()["objects"]
            if obj["layer"] == "gold" and obj["materialization"] == "mlv"
        }
        return contract
    extension = _profile_module(root, profile.case_id)
    from .asset_paths import department_profile_path

    config = {
        "version": 1,
        "runtime_sha256": hashlib.sha256(
            (root / "artefacts" / "gold" / "gold_runtime.py")
            .read_text(encoding="utf-8")
            .encode("utf-8")
        ).hexdigest(),
        "execution_mode": "synthetic_demo",
        "target_lakehouse": profile.resources["gold"],
        "schema": extension.GOLD_SCHEMA,
        "sources": [
            s.runtime_config() | {"synthetic_only": True, "source_instance": s.database}
            for s in sources
        ],
        **extension.domain_specs(
            root,
            sources,
            department_profile_resolver=lambda selected_root: department_profile_path(
                selected_root, profile.case_id
            ),
        ),
    }
    config["mlvs"] = mlv_definitions(config, root=root, use_case=profile.case_id)
    config["contract_hash"] = hashlib.sha256(
        json.dumps(config, sort_keys=True).encode()
    ).hexdigest()
    return config


def gold_config(
    root: Path, sources: tuple[Source, ...] = (), use_case: str | None = None
) -> dict[str, Any]:
    return bootstrap_gold_config(root, sources, use_case)


def mlv_definitions(
    config: dict[str, Any], *, root: Path | None = None, use_case: str | None = None
) -> dict[str, str]:
    """Assemble SQL generically; candidate business mappings belong to the case."""
    root = root or Path(__file__).resolve().parents[3]
    profile = load_use_case(root, use_case or config.get("use_case_id"))
    if profile.adapter == "compiled":
        return dict(config.get("mlvs", {}))
    target = f"{config['target_lakehouse']}.{config['schema']}"
    candidates = config.get("mlv_candidates")
    if candidates is None:
        candidates = _profile_module(root, profile.case_id).MLV_CANDIDATES
    facts = {f["name"] for f in config["facts"]}
    result = {}
    for name, fact, groups, measures in candidates:
        if fact not in facts:
            continue
        columns = [
            "CAST('SYNTHETIC_PROVISIONAL' AS STRING) AS data_scope",
            *groups,
            *[f"SUM({m}) AS {m}" for m in measures],
            "COUNT(*) AS source_row_count",
        ]
        result[name] = (
            f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {target}.{name}\n"
            "TBLPROPERTIES ('delta.enableChangeDataFeed'='true')\nAS SELECT "
            + ", ".join(columns)
            + f"\nFROM {target}.{fact}\nGROUP BY "
            + ", ".join(groups)
        )
    return result


def gold_notebook_document(
    root: Path, sources: tuple[Source, ...], step: str, use_case: str | None = None
) -> dict[str, Any]:
    from .orchestrator import metadata_notebook_document

    profile = load_use_case(root, use_case)
    if step != "mlvs":
        raise ValueError("Gold builds use the reusable pinned metadata Gold worker")
    document = metadata_notebook_document(root, "gold", profile.case_id)
    document["cells"][0]["source"] = [
        f"# {profile.mlv_notebook}\n",
        "\nPreview approved pinned MLV definitions; no recreation or refresh on ordinary data runs.\n",
        "\nAuthor/refresh execution uses GOLD_CONTROL.mlv_for_release with an authoritative ownership checker and retained mode evidence.\n",
        "\nNo schema capability, native ownership or incremental refresh support is inferred from this notebook.\n",
    ]
    parameters = next(cell for cell in document["cells"] if cell["id"] == "parameters")
    parameters["source"] = (
        "".join(parameters["source"])
        .replace('operation = "prepare"', 'operation = "preview_mlvs"')
        .splitlines(keepends=True)
    )
    document["cells"][-1] = _code_cell(
        "execute",
        "source_fs = GOLD_CONTROL.source_filesystem(notebookutils.fs, source_workspace_id, source_lakehouse_id)\n"
        f"if notebookutils.runtime.context['defaultLakehouseName'] != {profile.resources['gold']!r}:\n"
        "    raise RuntimeError('Attach the selected case Gold Lakehouse before MLV preview')\n"
        "manifest = ORCHESTRATION.read_run_manifest(source_fs, manifest_path, run_id=run_id,\n"
        "    version_id=version_id, expected_snapshot_sha256=snapshot_sha256,\n"
        "    load_configuration=META.load_configuration,\n"
        "    export_column='snapshot_json' if manifest_path.endswith('.csv') else None)\n"
        "if manifest.to_document()['configuration']['extensions'].get('use_case_id') != CASE_ID:\n"
        "    raise RuntimeError('Pinned manifest belongs to another use case; MLV operations are blocked')\n"
        "if operation != 'preview_mlvs':\n"
        "    raise RuntimeError('Author/refresh requires an explicit serving coordinator with authoritative ownership and actual-mode evidence; preview cannot grant execution')\n"
        "definitions = MLV.definitions_from_metadata(manifest.to_document()['configuration'], enabled_only=False)\n"
        "notebookutils.notebook.exit(json.dumps({'run_id': run_id, 'operation': operation,\n"
        "    'definitions': [{'object_id': item.object_id, 'target': item.target,\n"
        "                     'definition_sha256': item.sha256, 'version': item.version}\n"
        "                    for item in definitions]}))\n",
        [],
    )
    return document


def gold_notebook_definition(
    root: Path,
    sources: tuple[Source, ...],
    step: str,
    *,
    workspace_id: str,
    gold_lakehouse_id: str,
    source_lakehouse_id: str,
    document: dict[str, Any] | None = None,
    use_case: str | None = None,
) -> dict[str, Any]:
    profile = load_use_case(root, use_case)
    path = profile.generated / "notebooks" / f"{profile.mlv_notebook}.ipynb"
    expected = gold_notebook_document(root, sources, step, profile.case_id)
    if document is None:
        if not path.is_file():
            raise ValueError(
                f"Stale generated Gold notebook: {path.name}; run deployment/build.py"
            )
        document = json.loads(path.read_text(encoding="utf-8"))
    else:
        document = deepcopy(document)
    if document != expected:
        raise ValueError(f"Stale generated Gold notebook: {path.name}")
    document["cells"].insert(
        2,
        _code_cell(
            "resolved-bindings",
            f"source_workspace_id = {workspace_id!r}\nsource_lakehouse_id = {source_lakehouse_id!r}\n",
            [],
        ),
    )
    document["metadata"]["dependencies"] = {
        "lakehouse": {
            "default_lakehouse": gold_lakehouse_id,
            "default_lakehouse_name": profile.resources["gold"],
            "default_lakehouse_workspace_id": workspace_id,
            "known_lakehouses": [
                {"id": gold_lakehouse_id},
                {"id": source_lakehouse_id},
            ],
        }
    }
    definition = {
        "format": "ipynb",
        "parts": [part("artifact.content.ipynb", json_text(document))],
    }
    validate_deployable_definition(definition)
    return definition


def gold_pipeline_definition(
    sources: tuple[Source, ...], *, workspace_id: str, notebook_id: str
) -> dict[str, Any]:
    raise ValueError(
        "Per-case wrapper pipelines are retired; use the reusable metadata master pipeline"
    )
