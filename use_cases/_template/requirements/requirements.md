---
title: New case requirements template
description: Replace placeholders with reviewed business outcomes and acceptance
ms.date: 2026-11-09
ms.topic: reference
---

## Owner-reviewed requirements

Describe consumers, scope, source authority, privacy, unavailable measures
and acceptance. Inherit all BASE IDs unchanged. Case IDs are distinct from
the baseline; a case cannot redefine baseline authority.

```json
{
  "schema_version": 1,
  "document_type": "requirements",
  "use_case_id": "<case_id>",
  "baseline_version": "1.1.0",
  "purpose": "<business_outcome_and_scope>",
  "owners": ["<domain_owner>", "<engineering_owner>"],
  "baseline_requirements": [
    "BASE-001", "BASE-002", "BASE-003", "BASE-004", "BASE-005",
    "BASE-006", "BASE-007", "BASE-008", "BASE-009", "BASE-010",
    "BASE-011", "BASE-012", "BASE-013", "BASE-014", "BASE-015",
    "BASE-016", "BASE-017", "BASE-018", "BASE-019", "BASE-020",
    "BASE-021", "BASE-022", "BASE-023"
  ],
  "requirements": [
    {
      "id": "<CASE-REQ-001>",
      "statement": "<explicit_domain_requirement>",
      "baseline_ids": ["BASE-012", "BASE-014"],
      "acceptance": ["<measurable_expected_result_and_evidence>"]
    }
  ]
}
```
