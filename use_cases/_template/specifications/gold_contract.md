---
title: New case Gold contract template
description: Declare outputs from business grain and semantics rather than source-name guesses
ms.date: 2026-11-09
ms.topic: reference
---

## Required decisions

Define dimensions/facts using the complete inventory example as a reference.
Declare keys, temporal meaning, correction policy, explicit joins, measures
and units. A current position is not a sum of event after-images.
Empty arrays below are incomplete, not permission to synthesize a model.

```json
{
  "schema_version": 1,
  "document_type": "gold",
  "use_case_id": "<case_id>",
  "baseline_version": "1.1.0",
  "schema": "<gold_schema>",
  "dimensions": [],
  "facts": [],
  "calendars": {
    "enabled": true,
    "fiscal_year_start_month": "<explicit_month_1_to_12>",
    "layout": {
      "dim_date": {"layout_strategy": "partitioned", "partition_columns": ["date_sk"], "cluster_by_columns": []},
      "dim_fiscal_period": {"layout_strategy": "partitioned", "partition_columns": ["fiscal_period_sk"], "cluster_by_columns": []}
    }
  },
  "aggregates": [],
  "semantics": {"measures": []},
  "unavailable_measures": {"<measure_name>": "<missing_authoritative_input_or_business_decision>"}
}
```
