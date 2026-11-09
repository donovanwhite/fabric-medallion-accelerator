---
title: Inventory example source contracts
description: Explicit synthetic SQL product and movement schemas, keys, rules and layouts
ms.date: 2026-11-09
ms.topic: reference
---

## Input meaning

`products` is one product master row per product ID. `movements` is one
correctable signed event per movement ID, not an on-hand balance.
The integer movement date is explicitly YYYYMMDD, with no inferred timezone.
Example layouts are declared test decisions, not production capacity advice.

```json
{
  "schema_version": 1,
  "document_type": "sources",
  "use_case_id": "inventory_example",
  "baseline_version": "1.1.0",
  "sources": [
    {
      "code": "inventory",
      "description": "Fictional inventory product and movement feed",
      "description_slug": "warehouse",
      "source_instance": "db_inventory_example",
      "source_schema": "inventory",
      "connection_name": "conn_inventory_example",
      "source_type": "sql",
      "connector_type": "FabricSql",
      "authentication_type": "OAuth2",
      "silver_schema": "slv_inventory",
      "quarantine_schema": "quarantine_inventory",
      "specialist_rules": [],
      "tables": [
        {
          "name": "products",
          "grain": "One product master record per product_id",
          "keys": ["product_id"],
          "tracked_columns": ["product_id", "product_name", "category"],
          "columns": [
            {"name": "product_id", "data_type": "varchar", "max_length": 32, "nullable": false},
            {"name": "product_name", "data_type": "varchar", "max_length": 120, "nullable": false},
            {"name": "category", "data_type": "varchar", "max_length": 32, "nullable": false}
          ],
          "dq_rules": [
            {
              "id": "products_required",
              "handler": "required_columns",
              "version": "1",
              "severity": "error",
              "action": "fail",
              "parameters": {"columns": ["product_id", "product_name", "category"]}
            }
          ],
          "foreign_keys": [],
          "allow_empty": false,
          "layout": {"layout_strategy": "partitioned", "partition_columns": ["category"], "cluster_by_columns": []}
        },
        {
          "name": "movements",
          "grain": "One signed inventory movement event per movement_id",
          "keys": ["movement_id"],
          "tracked_columns": ["movement_id", "product_id", "movement_date", "quantity", "warehouse_code"],
          "columns": [
            {"name": "movement_id", "data_type": "bigint", "nullable": false},
            {"name": "product_id", "data_type": "varchar", "max_length": 32, "nullable": false},
            {"name": "movement_date", "data_type": "int", "format": "YYYYMMDD", "nullable": false},
            {"name": "quantity", "data_type": "int", "nullable": false},
            {"name": "warehouse_code", "data_type": "varchar", "max_length": 16, "nullable": false}
          ],
          "dq_rules": [
            {
              "id": "movements_required",
              "handler": "required_columns",
              "version": "1",
              "severity": "error",
              "action": "fail",
              "parameters": {"columns": ["movement_id", "product_id", "movement_date", "warehouse_code"]}
            }
          ],
          "foreign_keys": [{"columns": ["product_id"], "table": "products", "target_columns": ["product_id"]}],
          "allow_empty": true,
          "layout": {"layout_strategy": "partitioned", "partition_columns": ["warehouse_code"], "cluster_by_columns": []}
        }
      ]
    }
  ]
}
```
