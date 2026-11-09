---
title: inventory_example mlv_inventory_movement_monthly generated contract
description: Read-only context derived from reviewed case inputs
ms.topic: reference
---

## Pinned draft context

Generated, unapproved and not native acceptance. Do not hand-edit.

```json
{
  "object_id": "gold.mlv_inventory_movement_monthly",
  "kind": "mlv",
  "enabled": false,
  "contract": {
    "mlv_query": "CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS lh_gld_inventory_example.gld_inventory.mlv_inventory_movement_monthly\nAS SELECT CAST('SYNTHETIC_PROVISIONAL' AS STRING) AS data_scope, posting_month, product_sk, warehouse_code, SUM(quantity) AS quantity, COUNT(*) AS source_row_count\nFROM lh_gld_inventory_example.gld_inventory.fact_inventory_movement\nGROUP BY posting_month, product_sk, warehouse_code",
    "grain": [
      "posting_month",
      "product_sk",
      "warehouse_code"
    ],
    "synthetic_only": true,
    "refresh_policy": "Optimal"
  },
  "columns": [
    {
      "name": "data_scope",
      "data_type": "string",
      "nullable": false
    },
    {
      "name": "posting_month",
      "data_type": "int",
      "nullable": true
    },
    {
      "name": "product_sk",
      "data_type": "long",
      "nullable": false
    },
    {
      "name": "warehouse_code",
      "data_type": "string",
      "max_length": 16,
      "nullable": false
    },
    {
      "name": "quantity",
      "data_type": "decimal",
      "nullable": false,
      "precision": 38,
      "scale": 4
    },
    {
      "name": "source_row_count",
      "data_type": "long",
      "nullable": false
    }
  ],
  "layout": null
}
```
