---
title: Inventory example implementation plan
description: Illustrative local case compilation and acceptance without Treasury dependencies
ms.date: 2026-11-09
ms.topic: reference
---

## Baseline and inputs

Follow BASE-001 through BASE-023 and
`baselines/plans/specification_driven_accelerator/plan.md`.
This example has complete explicit source/Gold JSON blocks but no provisioned
source database, source records or native runtime evidence.

## Local steps

1. Validate requirements, ordered source schemas, keys, foreign keys and
   Gold grains/joins/date/units against baseline version 1.0.0.
2. Compile to this case's `generated` folder and review derived source,
   DQ, Gold, semantics and implementation context.
3. Build case-scoped runtime products from shared code and fixed safe
   adapters; prove Treasury outputs and shared source bytes remain unchanged.
4. Verify deterministic generation, missing-input rejection, baseline
   override rejection and stale-output detection.
5. Keep binding, capability/native acceptance, activation and business
   approval pending until separately authorized.
