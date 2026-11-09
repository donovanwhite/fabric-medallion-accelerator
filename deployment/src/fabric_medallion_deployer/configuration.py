"""Bind immutable metadata and export reviewed, parameterized lifecycle operations.

Binding never approves a capability or activates a version. Lifecycle exports
must be executed separately by the corresponding SQL role after review.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .definitions import part, validate_deployable_definition
from .metadata import (
    Configuration,
    MetadataError,
    canonical_json,
    load_configuration,
    metadata_digest,
)


def _uuid(value: object, label: str) -> str:
    try:
        if not isinstance(value, str) or str(uuid.UUID(value)) != value:
            raise ValueError
    except ValueError:
        raise MetadataError(f"{label} requires a resolved canonical UUID") from None
    return value


def bind_configuration(
    configuration: Configuration,
    *,
    workspace_id: str,
    source_database_ids: Mapping[str, str],
    connection_ids: Mapping[str, str],
    version_id: str | None = None,
) -> Configuration:
    """Create a new bound draft without changing historical business contracts."""
    workspace = _uuid(workspace_id, "Workspace")
    document = configuration.to_document()
    feeds = document["feeds"]
    codes = {feed["source_code"] for feed in feeds}
    by_id = {connection["id"]: connection for connection in document["connections"]}
    fabric_codes = {
        feed["source_code"]
        for feed in feeds
        if by_id[feed["connection_id"]]["connector_type"] == "FabricSql"
    }
    if set(source_database_ids) != fabric_codes or set(connection_ids) != codes:
        raise MetadataError("Binding requires exact configured source membership")
    if any(feed["source_type"] != "sql" for feed in feeds):
        raise MetadataError("Binding accepts supported SQL sources only")
    databases = {
        code: _uuid(value, "Source database")
        for code, value in source_database_ids.items()
    }
    connections = {
        code: _uuid(value, "Source connection")
        for code, value in connection_ids.items()
    }
    if len(set(databases.values())) != len(fabric_codes) or len(
        set(connections.values())
    ) != len(codes):
        raise MetadataError("Source database and connection bindings must be distinct")
    if len(by_id) != len(feeds):
        raise MetadataError("Binding requires one distinct managed connection per feed")
    seen = set()
    for feed in feeds:
        connection = by_id[feed["connection_id"]]
        if feed["connection_id"] in seen or (
            connection["connector_type"],
            connection["authentication_type"],
        ) not in {("FabricSql", "OAuth2"), ("SqlServer", "Basic")}:
            raise MetadataError(
                "Binding requires distinct supported SQL managed connections"
            )
        seen.add(feed["connection_id"])
        connection["fabric_connection_id"] = connections[feed["source_code"]]
        feed["geometry"] = (
            {
                "workspace_id": workspace,
                "artifact_id": databases[feed["source_code"]],
            }
            if connection["connector_type"] == "FabricSql"
            else None
        )
    extensions = document["extensions"]
    extensions["bootstrap_state"] = "draft_bound"
    # Version identity is not part of business key/value hash policy.
    document["version_id"] = "bound-content"
    identity = metadata_digest(document)
    document["version_id"] = version_id or f"medallion-bound-{identity[:32]}"
    result = load_configuration(document)
    if result.definition_sha256 == configuration.definition_sha256:
        raise MetadataError("Binding must create a distinct immutable draft")
    return result


def load_configuration_file(path: Path) -> Configuration:
    import json

    return load_configuration(json.loads(path.read_text(encoding="utf-8")))


def lifecycle_pipeline_definition(
    configuration: Configuration,
    *,
    workspace_id: str,
    control_database_id: str,
    control_connection_id: str,
    operation: str,
    validation_evidence: Mapping[str, Any] | None = None,
    validator_sha256: str | None = None,
    expected_active_version: str | None = None,
    activation_approval: str | None = None,
) -> dict[str, Any]:
    """Compile exactly one SQL lifecycle operation; never submit it automatically."""
    for label, identity in (
        ("Workspace", workspace_id),
        ("Control database", control_database_id),
        ("Control connection", control_connection_id),
    ):
        _uuid(identity, label)
    parameters: dict[str, Any] = {"version_id": configuration.version_id}
    if operation == "register":
        procedure = "cfg.usp_register_version"
        parameters.update(
            definition_json=configuration.definition_json,
            definition_sha256=configuration.definition_sha256,
        )
    elif operation == "validate":
        if (
            not isinstance(validation_evidence, Mapping)
            or validation_evidence.get("passed") is not True
            or validation_evidence.get("definition_sha256")
            != configuration.definition_sha256
            or not isinstance(validator_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", validator_sha256) is None
        ):
            raise MetadataError(
                "Validation requires successful evidence for the exact definition and validator"
            )
        procedure = "cfg.usp_validate_version"
        parameters.update(
            expected_hash=configuration.definition_sha256,
            validator_hash=validator_sha256,
            validation_json=canonical_json(dict(validation_evidence)),
        )
    elif operation in {"approve", "activate", "rollback"}:
        if not activation_approval or not re.fullmatch(
            r"[A-Za-z0-9_.-]{1,128}", activation_approval
        ):
            raise MetadataError(
                "Approval/activation requires an attributable approval reference"
            )
        procedure = (
            "cfg.usp_approve_version"
            if operation == "approve"
            else "cfg.usp_activate_version"
        )
        parameters["expected_hash"] = configuration.definition_sha256
        if operation != "approve":
            parameters["expected_active_version"] = expected_active_version
    elif operation == "preview":
        procedure = "cfg.usp_preview_version"
    else:
        raise MetadataError("Unknown metadata lifecycle operation")
    document = {
        "properties": {
            "description": f"Explicit reviewed metadata operation: {operation}; never auto-submitted",
            "activities": [
                {
                    "name": f"metadata_{operation}",
                    "type": "Script",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.00:15:00",
                        "retry": 0,
                        "secureInput": True,
                        "secureOutput": True,
                    },
                    "connectionSettings": {
                        "name": "conn_orchestrator_sql",
                        "properties": {
                            "type": "FabricSqlDatabase",
                            "typeProperties": {
                                "workspaceId": workspace_id,
                                "artifactId": control_database_id,
                            },
                            "externalReferences": {"connection": control_connection_id},
                        },
                    },
                    "typeProperties": {
                        "scripts": [
                            {
                                "type": "Query",
                                "text": f"EXEC {procedure} "
                                + ",".join("@" + name for name in parameters)
                                + ";",
                                "parameters": [
                                    {
                                        "name": name,
                                        "type": "String",
                                        "direction": "Input",
                                        "value": value,
                                    }
                                    for name, value in parameters.items()
                                ],
                            }
                        ],
                        "scriptBlockExecutionTimeout": "00:10:00",
                    },
                }
            ],
        }
    }
    definition = {"parts": [part("pipeline-content.json", canonical_json(document))]}
    validate_deployable_definition(definition)
    return definition


def configuration_receipt(configuration: Configuration) -> dict[str, Any]:
    """Safe detached identity used to compare registration/approval readback."""
    return {
        "version_id": configuration.version_id,
        "definition_sha256": configuration.definition_sha256,
        "bytes": len(configuration.definition_json.encode("ascii")),
        "binding_sha256": metadata_digest(
            {
                "connections": configuration.to_document()["connections"],
                "feeds": configuration.to_document()["feeds"],
            }
        ),
        "validator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
