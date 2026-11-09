---
title: New case source contract template
description: Declare source authority, exact schema, business identity and quality decisions
ms.date: 2026-11-09
ms.topic: reference
---

## Required decisions

Replace all placeholders and confirm every column's physical representation,
null/type/precision/date meaning, key, value-hash order, rule and relationship.
The sample type/transport is illustrative, not inferred from a source name.
See `inventory_example` for a complete supported contract.

Existing documentation may be extracted with Docling and used by an agent
to draft this contract. Retain evidence references and review the schema,
keys and assumptions before compilation.

This is the case's source-schema authoring contract, not a new connector
implementation. Reuse the shared
[connector contract/template](../../../artefacts/pipeline/connectors/README.md).
The approved metadata database and pinned run snapshot supply actual runtime
configuration, columns/projections and managed connection/authentication
references. Keep credentials in Fabric-managed connections, not this file
or metadata rows.

Choose `connector_type: FabricSql` with `authentication_type: OAuth2` for
Fabric SQL Database, or `connector_type: SqlServer` with
`authentication_type: Basic` for SQL Server. Both use `source_type: sql`.
SQL Server resolves its server/database through the managed connection,
not Fabric source workspace/artifact IDs. Native acceptance remains gated.

Standard technical DQ and the Bronze-to-Silver processing path are inherited
from the shared engine. The empty `dq_rules` and `specialist_rules` lists
below are slots for additional business checks, not switches for mandatory
schema/type, required/key, uniqueness or reconciliation checks. Do not copy
the shared ingestion implementation into this case.

```json
{
  "schema_version": 1,
  "document_type": "sources",
  "use_case_id": "<case_id>",
  "baseline_version": "1.1.0",
  "sources": [
    {
      "code": "<source_code>",
      "description": "<human_description>",
      "description_slug": "<stable_slug>",
      "source_instance": "<source_database>",
      "source_schema": "<source_schema>",
      "connection_name": "<managed_connection_name>",
      "source_type": "sql",
      "connector_type": "FabricSql",
      "authentication_type": "OAuth2",
      "silver_schema": "<silver_schema>",
      "quarantine_schema": "<quarantine_schema>",
      "specialist_rules": [],
      "tables": [
        {
          "name": "<source_table>",
          "grain": "<one_record_means>",
          "keys": ["<business_key>"],
          "tracked_columns": ["<tracked_column>"],
          "columns": [
            {"name": "<column_name>", "data_type": "varchar", "max_length": 120, "nullable": false}
          ],
          "dq_rules": [],
          "foreign_keys": [],
          "allow_empty": false,
          "layout": {
            "layout_strategy": "<partitioned_or_liquid>",
            "partition_columns": ["<declared_partition_column_if_selected>"],
            "cluster_by_columns": []
          }
        }
      ]
    }
  ]
}
```
