---
title: inventory_example dim_fiscal_period generated contract
description: Read-only context derived from reviewed case inputs
ms.topic: reference
---

## Pinned draft context

Generated, unapproved and not native acceptance. Do not hand-edit.

```json
{
  "object_id": "gold.dim_fiscal_period",
  "kind": "calendar",
  "enabled": false,
  "contract": {
    "pattern": "calendar_frames",
    "time_basis": "EXPLICIT_CASE_FISCAL_CALENDAR"
  },
  "columns": [
    {
      "name": "fiscal_period_sk",
      "data_type": "int",
      "nullable": false
    }
  ],
  "layout": {
    "layout_strategy": "partitioned",
    "partition_columns": [
      "fiscal_period_sk"
    ],
    "cluster_by_columns": []
  }
}
```
