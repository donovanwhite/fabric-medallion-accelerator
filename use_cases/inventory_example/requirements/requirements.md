---
title: Inventory example requirements
description: Explicit movement-flow outcomes mapped to all accelerator guardrails
ms.date: 2026-11-09
ms.topic: reference
---

## Business meaning

Quantity is a signed movement in units. Corrections replace an event's
analytical representation; retaining another Silver after-image does not
create a second movement. Monthly net movement is not current stock.

```json
{
  "schema_version": 1,
  "document_type": "requirements",
  "use_case_id": "inventory_example",
  "baseline_version": "1.1.0",
  "purpose": "Illustrate reusable processing of explicit product and signed inventory movement contracts.",
  "owners": ["example_data_engineering", "example_inventory_owner"],
  "baseline_requirements": [
    "BASE-001", "BASE-002", "BASE-003", "BASE-004", "BASE-005",
    "BASE-006", "BASE-007", "BASE-008", "BASE-009", "BASE-010",
    "BASE-011", "BASE-012", "BASE-013", "BASE-014", "BASE-015",
    "BASE-016", "BASE-017", "BASE-018", "BASE-019", "BASE-020",
    "BASE-021", "BASE-022", "BASE-023"
  ],
  "requirements": [
    {
      "id": "INV-REQ-001",
      "statement": "Preserve product attribute changes without inferring deletes from absent rows.",
      "baseline_ids": ["BASE-007", "BASE-008", "BASE-010", "BASE-011", "BASE-013"],
      "acceptance": ["An unchanged observation adds no version; A-to-B-to-A retains accepted transitions."]
    },
    {
      "id": "INV-REQ-002",
      "statement": "Retain one analytical event per movement identity, with signed quantity measured in units.",
      "baseline_ids": ["BASE-006", "BASE-012", "BASE-014", "BASE-015"],
      "acceptance": ["Correcting one movement changes its contribution rather than adding a second event."]
    },
    {
      "id": "INV-REQ-003",
      "statement": "Publish only reconciled monthly net movement; do not invent current stock without opening position.",
      "baseline_ids": ["BASE-016", "BASE-017", "BASE-021", "BASE-023"],
      "acceptance": ["Monthly aggregates reconcile to selected facts; current stock remains unavailable."]
    }
  ]
}
```
