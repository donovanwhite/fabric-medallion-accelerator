---
title: inventory_example movements generated contract
description: Read-only context derived from reviewed case inputs
ms.topic: reference
---

## Pinned draft context

Generated, unapproved and not native acceptance. Do not hand-edit.

```json
{
  "object_id": "inventory.silver.movements",
  "target": "lh_brz_slv_inventory_example.slv_inventory.movements",
  "grain": "One signed inventory movement event per movement_id",
  "business_keys": [
    "movement_id"
  ],
  "hash_policy": {
    "key_columns": [
      "movement_id"
    ],
    "value_columns": [
      "movement_id",
      "product_id",
      "movement_date",
      "quantity",
      "warehouse_code"
    ],
    "encoding": "spark_json_struct_sha256_v1"
  },
  "columns": [
    {
      "name": "movement_id",
      "data_type": "bigint",
      "nullable": false
    },
    {
      "name": "product_id",
      "data_type": "varchar",
      "max_length": 32,
      "nullable": false
    },
    {
      "name": "movement_date",
      "data_type": "int",
      "format": "YYYYMMDD",
      "nullable": false
    },
    {
      "name": "quantity",
      "data_type": "int",
      "nullable": false
    },
    {
      "name": "warehouse_code",
      "data_type": "varchar",
      "max_length": 16,
      "nullable": false
    }
  ],
  "rules": [
    {
      "id": "inventory.silver.movements.prepare_rows",
      "object_id": "inventory.silver.movements",
      "handler": "legacy.prepare_rows",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "scope": "object"
      }
    },
    {
      "id": "inventory.silver.movements.relationship_checks",
      "object_id": "inventory.silver.movements",
      "handler": "legacy.relationship_checks",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "scope": "object"
      }
    },
    {
      "id": "inventory.silver.movements.silver_checks",
      "object_id": "inventory.silver.movements",
      "handler": "legacy.silver_checks",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "scope": "object"
      }
    },
    {
      "id": "movements_required",
      "handler": "required_columns",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "columns": [
          "movement_id",
          "product_id",
          "movement_date",
          "warehouse_code"
        ]
      },
      "object_id": "inventory.silver.movements"
    }
  ],
  "layout": {
    "layout_strategy": "partitioned",
    "partition_columns": [
      "warehouse_code"
    ],
    "cluster_by_columns": []
  }
}
```
