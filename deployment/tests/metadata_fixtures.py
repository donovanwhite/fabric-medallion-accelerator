"""Compact, credential-free metadata used by isolated orchestration tests."""

from __future__ import annotations

from typing import Any


def metadata_document() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "version_id": "test-version-1",
        "owner": "data_engineering",
        "connections": [
            {
                "id": "conn-bas",
                "name": "conn_bas_finance_sql",
                "connector_type": "FabricSql",
                "authentication_type": "OAuth2",
                "fabric_connection_id": "11111111-1111-4111-8111-111111111111",
                "gateway_id": None,
            }
        ],
        "feeds": [
            {
                "id": "bas-finance-sql",
                "source_code": "bas",
                "source_instance": "db_bas_finance",
                "source_type": "sql",
                "description_slug": "finance",
                "description": "Synthetic finance",
                "connection_id": "conn-bas",
                "silver_schema": "slv_bas",
                "quarantine_schema": "quarantine_bas",
                "authority": "authoritative",
                "enabled": True,
                "legacy_contract_hash": "a" * 64,
                "legacy_bronze_root": "Files/bronze/bas",
                "geometry": {
                    "workspace_id": "44444444-4444-4444-8444-444444444444",
                    "artifact_id": "55555555-5555-4555-8555-555555555555",
                },
            }
        ],
        "workers": [
            {
                "id": "silver-worker",
                "notebook_name": "nb_silver_worker",
                "lakehouse": "lh_brz_slv_treasury",
                "code_sha256": "b" * 64,
            }
        ],
        "objects": [
            {
                "id": "bas-bronze-postings",
                "feed_id": "bas-finance-sql",
                "layer": "bronze",
                "name": "postings",
                "target_schema": None,
                "lakehouse": "lh_brz_slv_treasury",
                "materialization": "files",
                "enabled": True,
                "execution_priority": 10,
                "business_keys": ["id"],
                "columns": [
                    {"name": "id", "data_type": "int", "nullable": False},
                    {
                        "name": "amount",
                        "data_type": "decimal",
                        "nullable": False,
                        "precision": 19,
                        "scale": 4,
                    },
                ],
                "contract": {
                    "source_schema": "bas",
                    "source_object": "postings",
                    "projection_sql": "[id],CONVERT(varchar(50),CAST([amount] AS decimal(19,4))) AS [amount]",
                },
                "worker_id": None,
                "layout": None,
                "required_capabilities": ["dynamic-fabric-sql"],
                "existing_target": False,
            },
            {
                "id": "bas-silver-postings",
                "feed_id": "bas-finance-sql",
                "layer": "silver",
                "name": "postings",
                "target_schema": "slv_bas",
                "lakehouse": "lh_brz_slv_treasury",
                "materialization": "append_versions",
                "enabled": True,
                "execution_priority": 20,
                "business_keys": ["id"],
                "columns": [
                    {"name": "id", "data_type": "int", "nullable": False},
                    {
                        "name": "amount",
                        "data_type": "decimal",
                        "nullable": False,
                        "precision": 19,
                        "scale": 4,
                    },
                ],
                "contract": {
                    "keys": ["id"],
                    "change_hash_exclusions": [],
                    "allow_empty": False,
                },
                "worker_id": "silver-worker",
                "layout": None,
                "required_capabilities": [],
                "existing_target": True,
            },
        ],
        "dependencies": [
            {
                "upstream_id": "bas-bronze-postings",
                "downstream_id": "bas-silver-postings",
                "condition": "succeeded",
            }
        ],
        "rules": [
            {
                "id": "postings-keys",
                "object_id": "bas-silver-postings",
                "handler": "business_key_unique",
                "version": "1",
                "severity": "error",
                "action": "fail",
                "parameters": {"columns": ["id"]},
            },
            *[
                {
                    "id": f"postings-{handler}",
                    "object_id": "bas-silver-postings",
                    "handler": f"legacy.{handler}",
                    "version": "1",
                    "severity": "error",
                    "action": "fail",
                    "parameters": {"scope": "object"},
                }
                for handler in ("prepare_rows", "relationship_checks", "silver_checks")
            ],
            {
                "id": "bas-source-rules",
                "object_id": "bas-silver-postings",
                "handler": "legacy.source_business_checks",
                "version": "1",
                "severity": "error",
                "action": "fail",
                "parameters": {
                    "scope": "source",
                    "source_code": "bas",
                    "objects": ["bas-silver-postings"],
                },
            },
        ],
        "transformations": [],
        "constraints": [],
        "capabilities": [
            {
                "id": "dynamic-fabric-sql",
                "verified": False,
                "evidence_sha256": None,
            }
        ],
        "extensions": {
            "use_case_id": "treasury",
            "hash_policies": {
                "bas-silver-postings": {
                    "key_columns": ["id"],
                    "value_columns": ["id", "amount"],
                    "encoding": "spark_json_struct_sha256_v1",
                }
            },
        },
    }
