---
title: Multi-use-case accelerator architecture
description: Shared medallion mechanisms with isolated domain contracts and generated deployment products
baseline_version: 1.1.0
ms.date: 2026-11-09
ms.topic: concept
---

## Ownership structure

```text
baselines\
  requirements\             governance, BASE requirements, policy.json
  specifications\           architecture, contracts, generation workflow
  plans\                    shared architecture plans and historical evidence
use_cases\
  catalog.json              explicit registered cases and default selection
  _template\                authoring starter, not a runnable case
  <case>\
    use_case.json           case identity, resource namespace and adapters
    docling_specifications\ private raw/extracted evidence plus workflow notes
    requirements\           business outcomes and BASE crosswalk
    specifications\         structured source/Gold contracts and domain context
    plans\                  case implementation and acceptance plans
    contracts\              reviewed bootstrap/historical contract references
    extensions\             reviewed domain-only algorithms
    fixtures\               optional case-owned generators, snapshots and evidence
    generated\              compiler metadata, context MD, extensions and build products
artefacts\                  shared runtime sources, SQL and pipeline templates
deployment\                 reusable compiler, validation, packaging and publication
```

## Runtime and compilation boundaries

Human/AI authoring produces explicit reviewed case requirements and source/
Gold contracts. Deterministic compilation generates case-isolated draft
metadata and context documents. The normal builder reuses core code and
packages approved case extensions without embedding active source arrays.

Runtime reads approved pinned SQL metadata and immutable exports, not the
latest Markdown or active-version pointer inside a worker. Generated drafts
must pass binding, validation, approval and activation separately. Source
data enters Bronze through Copy, never through an invented notebook
replacement for ingestion.

## Case isolation

Treasury-specific rules, Gold inventories and mappings belong to Treasury
extensions/contracts. Other cases do not import them or require Treasury
CSV snapshots. Case resources and item prefixes prevent one case's build
from replacing another's workers or Lakehouses. The same reusable master/
worker roles may be packaged for independent case deployment namespaces;
there is no pipeline per source.

The current SQL control plane has explicit approved versions and an active
selection. Multiple-case operations must pin the intended approved version;
a global active pointer is not evidence that every case is selected at once.
Cross-case shared sources/targets require explicit authority/ownership
contracts, not coincidental equal business keys.

## Extension seam

Only domain contracts and reviewed specialist rules should vary by case.
Core changes are appropriate only for a new reusable capability, with
baseline review and regression proof. Do not copy entire Silver/Gold/DAG
implementations into each case. Self-contained generated notebook bundles
are deployment packaging, not hand-maintained forks.
