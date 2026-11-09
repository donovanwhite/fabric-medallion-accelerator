---
title: Cutover and legacy decommissioning status
description: Local repository retirement is complete while native activation and workspace deletion remain unperformed
ms.date: 2026-11-09
ms.topic: reference
---

## Current status

No Fabric cutover or deployed-item deletion was performed.
The user selected local-only implementation and later review of cloud
legacy deletion. A3/A4 have not been satisfied by local tests.

The repository removes 15 source-specific medallion notebooks and the
standalone Gold build notebook. The default generated inventory is now
11 reusable items, not the original 26-item inventory. The four old
pipeline names are absent from normal publication.

Historical contracts, definitions and replay algorithms remain available
for review. Existing deployed notebooks/pipelines, data, schedules,
permissions and active metadata have not been changed.

## Required before native cutover

Review complete SQL/Copy/DAG/Gold/MLV/layout acceptance and native rollback
evidence. Resolve current target IDs and capacity, inspect old/new schedules,
jobs and downstream references, and prove no overlapping source/target
writers. Activate only the exact approved bound version after operator
signoff and successful readback.

## Required before deployed legacy deletion

Present the exact currently resolved legacy notebook/pipeline IDs and
dependency/job evidence for approval. Delete only individually approved
owned items after cutover and rollback retention decisions.

Never include source databases, either Lakehouse, retained Bronze,
canonical Silver, quarantine/audit, metadata versions or Gold releases
in the notebook cleanup. Do not treat successful activation as permission
to remove data or assign roles.
