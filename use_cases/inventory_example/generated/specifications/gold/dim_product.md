---
title: inventory_example dim_product generated contract
description: Read-only context derived from reviewed case inputs
ms.topic: reference
---

## Pinned draft context

Generated, unapproved and not native acceptance. Do not hand-edit.

```json
{
  "object_id": "gold.dim_product",
  "kind": "scd2_dimension",
  "enabled": false,
  "contract": {
    "name": "dim_product",
    "source": "inventory",
    "table": "products",
    "keys": [
      "product_id"
    ],
    "attributes": [
      "product_name",
      "category"
    ],
    "grain": "One observed attribute interval per product_id",
    "source_instance": "db_inventory_example"
  },
  "columns": [
    {
      "name": "dimension_sk",
      "data_type": "long",
      "nullable": false
    },
    {
      "name": "durable_key",
      "data_type": "string",
      "nullable": false
    },
    {
      "name": "effective_from",
      "data_type": "timestamp",
      "nullable": true
    },
    {
      "name": "is_current",
      "data_type": "boolean",
      "nullable": false
    },
    {
      "name": "product_id",
      "data_type": "string",
      "max_length": 32,
      "nullable": false
    },
    {
      "name": "product_name",
      "data_type": "string",
      "max_length": 120,
      "nullable": false
    },
    {
      "name": "category",
      "data_type": "string",
      "max_length": 32,
      "nullable": false
    }
  ],
  "layout": {
    "layout_strategy": "partitioned",
    "partition_columns": [
      "is_current"
    ],
    "cluster_by_columns": []
  }
}
```
