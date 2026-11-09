---
title: Inventory example Gold and semantics
description: Explicit observed product dimension, correctable movement fact and monthly movement aggregate
ms.date: 2026-11-09
ms.topic: reference
---

## Output meaning

Product dimension history uses observation time, not invented effective
history. Movement quantity is signed. Event corrections do not double-count.
The fiscal year starts in January for this fictional example; it is not
inherited from Treasury. No stock-balance, RLS or business-publication
capability is inferred.

```json
{
  "schema_version": 1,
  "document_type": "gold",
  "use_case_id": "inventory_example",
  "baseline_version": "1.1.0",
  "schema": "gld_inventory",
  "dimensions": [
    {
      "name": "dim_product",
      "source": "inventory",
      "table": "products",
      "grain": "One observed attribute interval per product_id",
      "keys": ["product_id"],
      "attributes": ["product_name", "category"],
      "time_basis": "observed",
      "unknown_member": 0,
      "layout": {"layout_strategy": "partitioned", "partition_columns": ["is_current"], "cluster_by_columns": []}
    }
  ],
  "facts": [
    {
      "name": "fact_inventory_movement",
      "source": "inventory",
      "table": "movements",
      "kind": "atomic_fact",
      "grain": "One correctable event per movement_id, not a stock position",
      "keys": ["movement_id"],
      "date_field": "movement_date",
      "measures": [{"column": "quantity", "unit": "units"}],
      "attributes": ["movement_id", "product_id", "warehouse_code"],
      "period_field": null,
      "dimension_lookups": [
        {"dimension": "dim_product", "source_columns": ["product_id"], "dimension_columns": ["product_id"], "unknown_member": 0}
      ],
      "layout": {"layout_strategy": "partitioned", "partition_columns": ["posting_month"], "cluster_by_columns": []}
    }
  ],
  "calendars": {
    "enabled": true,
    "fiscal_year_start_month": 1,
    "layout": {
      "dim_date": {"layout_strategy": "partitioned", "partition_columns": ["date_sk"], "cluster_by_columns": []},
      "dim_fiscal_period": {"layout_strategy": "partitioned", "partition_columns": ["fiscal_period_sk"], "cluster_by_columns": []}
    }
  },
  "aggregates": [
    {
      "name": "mlv_inventory_movement_monthly",
      "fact": "fact_inventory_movement",
      "group_by": ["posting_month", "product_sk", "warehouse_code"],
      "sum_columns": ["quantity"],
      "refresh_policy": "Optimal"
    }
  ],
  "semantics": {
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
  },
  "unavailable_measures": {
    "current_stock_balance": "Opening stock and a complete position contract are not supplied."
  }
}
```
