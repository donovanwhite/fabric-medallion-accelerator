---
title: inventory_example products generated contract
description: Read-only context derived from reviewed case inputs
ms.topic: reference
---

## Pinned draft context

Generated, unapproved and not native acceptance. Do not hand-edit.

```json
{
  "object_id": "inventory.silver.products",
  "target": "lh_brz_slv_inventory_example.slv_inventory.products",
  "grain": "One product master record per product_id",
  "business_keys": [
    "product_id"
  ],
  "hash_policy": {
    "key_columns": [
      "product_id"
    ],
    "value_columns": [
      "product_id",
      "product_name",
      "category"
    ],
    "encoding": "spark_json_struct_sha256_v1"
  },
  "columns": [
    {
      "name": "product_id",
      "data_type": "varchar",
      "max_length": 32,
      "nullable": false
    },
    {
      "name": "product_name",
      "data_type": "varchar",
      "max_length": 120,
      "nullable": false
    },
    {
      "name": "category",
      "data_type": "varchar",
      "max_length": 32,
      "nullable": false
    }
  ],
  "rules": [
    {
      "id": "inventory.silver.products.prepare_rows",
      "object_id": "inventory.silver.products",
      "handler": "legacy.prepare_rows",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "scope": "object"
      }
    },
    {
      "id": "inventory.silver.products.relationship_checks",
      "object_id": "inventory.silver.products",
      "handler": "legacy.relationship_checks",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "scope": "object"
      }
    },
    {
      "id": "inventory.silver.products.silver_checks",
      "object_id": "inventory.silver.products",
      "handler": "legacy.silver_checks",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "scope": "object"
      }
    },
    {
      "id": "products_required",
      "handler": "required_columns",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "columns": [
          "product_id",
          "product_name",
          "category"
        ]
      },
      "object_id": "inventory.silver.products"
    },
    {
      "id": "inventory.source_business_checks",
      "object_id": "inventory.silver.products",
      "handler": "legacy.source_business_checks",
      "version": "1",
      "severity": "error",
      "action": "fail",
      "parameters": {
        "scope": "source",
        "objects": [
          "inventory.silver.products",
          "inventory.silver.movements"
        ],
        "source_code": "inventory"
      }
    }
  ],
  "layout": {
    "layout_strategy": "partitioned",
    "partition_columns": [
      "category"
    ],
    "cluster_by_columns": []
  }
}
```
