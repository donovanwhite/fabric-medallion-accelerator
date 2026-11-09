---
title: inventory_example fact_inventory_movement generated contract
description: Read-only context derived from reviewed case inputs
ms.topic: reference
---

## Pinned draft context

Generated, unapproved and not native acceptance. Do not hand-edit.

```json
{
  "object_id": "gold.fact_inventory_movement",
  "kind": "atomic_fact",
  "enabled": false,
  "contract": {
    "name": "fact_inventory_movement",
    "source": "inventory",
    "table": "movements",
    "keys": [
      "movement_id"
    ],
    "date_field": "movement_date",
    "attributes": [
      "movement_id",
      "product_id",
      "warehouse_code"
    ],
    "period_field": null,
    "snapshot": false,
    "measures": [
      "quantity"
    ],
    "measure_types": {
      "quantity": "decimal(28,4)"
    },
    "grain": "One correctable event per movement_id, not a stock position",
    "date_roles": {},
    "units": {
      "quantity": "units"
    },
    "dimension_lookups": [
      {
        "dimension": "dim_product",
        "source_columns": [
          "product_id"
        ],
        "dimension_columns": [
          "product_id"
        ],
        "unknown_member": 0
      }
    ]
  },
  "columns": [
    {
      "name": "fact_sk",
      "data_type": "long",
      "nullable": false
    },
    {
      "name": "event_date",
      "data_type": "date",
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
      "name": "quantity",
      "data_type": "decimal",
      "nullable": false,
      "precision": 28,
      "scale": 4
    },
    {
      "name": "movement_id",
      "data_type": "long",
      "nullable": false
    },
    {
      "name": "product_id",
      "data_type": "string",
      "max_length": 32,
      "nullable": false
    },
    {
      "name": "warehouse_code",
      "data_type": "string",
      "max_length": 16,
      "nullable": false
    }
  ],
  "layout": {
    "layout_strategy": "partitioned",
    "partition_columns": [
      "posting_month"
    ],
    "cluster_by_columns": []
  }
}
```
