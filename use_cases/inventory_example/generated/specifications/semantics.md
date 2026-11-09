---
title: inventory_example generated semantics
description: Deterministic draft context compiled from explicit case contracts
ms.topic: reference
---

## Generated draft

Use case: `inventory_example`. Baseline: `1.1.0`.

Declared measures are design contracts only. No semantic model, RLS or business publication is generated or approved.

Do not hand-edit generated files. Change the reviewed case input Markdown and recompile.

## Declared semantic contract

```json
{
  "measures": [
    {
      "name": "net_movement_quantity",
      "fact": "fact_inventory_movement",
      "column": "quantity",
      "aggregation": "sum",
      "unit": "units",
      "description": "Signed net movement during the selected event-date period; not current stock."
    }
  ]
}
```
