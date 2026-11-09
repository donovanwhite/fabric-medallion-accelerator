---
title: Rollback boundaries and local preservation evidence
description: Preserved historical contracts and explicit configuration, worker, serving and layout rollback scope
ms.date: 2026-11-09
ms.topic: reference
---

## Status

Local preservation checks exist. No native rollback drill, configuration
activation, table rewind or data deletion was performed.

## Separate rollback surfaces

* Configuration rollback uses an explicit exported `cfg.usp_activate_version`
  operation for an already approved prior version, with the expected active
  version and attributable approval. It changes future runs, not old snapshots.
* Worker rollback must select a matching reviewed implementation hash.
  Rebinding an old manifest to changed code is rejected.
* Serving rollback selects an approved retained Gold release and its exact
  output table versions. It does not require unchanged rows to carry a
  new release ID and does not fabricate business publication.
* Physical layout recovery is a separate target-specific operation.
  The layout adapter rejects an unapproved rewrite and does not automatically
  overwrite, vacuum, restore or repartition historical tables.

## Preserved local evidence

Source registries/schemas, CSV manifests, the frozen
`deployment\tests\fixtures\medallion_baseline.json`, historical Bronze
resolvers, definition builders and `run_gold` compatibility wrapper remain.
The old extraction template remains as evidence outside the normal compiler
path. Normal generation does not emit source-bound wrappers.

Exact 44-table business contract/hash parity is checked locally. SQL lifecycle
exports retain compare-and-swap and parameter binding; fake service clients
check uncertain publication resumes without blind resubmission.
These are not a native SQL/Delta rollback rehearsal.

## Required native drill

Before cutover, independently verify active configuration, running jobs,
writer fences, worker hashes and pinned serving versions in the approved
target. Exercise the reviewed rollback under operator approval, then
read back original batch/history identities and consumer-selected outputs.
Record exact jobs, versions, pass/fail results and capacity impact.
No broad workspace cleanup or simulator reseed is an acceptable rollback.
