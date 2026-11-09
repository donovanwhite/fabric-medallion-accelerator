---
title: Accelerator use cases
description: Isolated domain specifications and generated profiles governed by the reusable baseline
ms.date: 2026-11-09
ms.topic: overview
---

## Separate the case from the framework

The accelerator is not a Treasury application with configurable names.
Treasury is its proving domain profile. Shared mechanisms remain in
`artefacts`; each case owns its evidence, contracts, rules and outputs.

Read [the baseline](../baselines/README.md), then start with
[_template](_template/README.md). [inventory_example](inventory_example/README.md)
is a small complete synthetic reference for the initial compiler;
[treasury](treasury/README.md) shows a richer reviewed bootstrap profile.

Register cases in [catalog.json](catalog.json). Each directory needs a
`use_case.json` descriptor with explicit resources/adapters and current
document paths. Shared code must discover the selected profile, never
guess a customer from source names or the first directory found.

## Authoring and outputs

Put plans, requirements and specifications under their matching case
subfolders. Put raw/Docling evidence in `docling_specifications`, reviewed
specialist algorithms in `extensions`, and all generated products in
`generated`. Do not duplicate shared medallion implementations.

Generated drafts are not active configurations. Every case inherits the
same BASE requirements and must retain unresolved business/native gates.
Different cases need explicit resource namespaces and pinned versions;
equal field names do not establish cross-case business identity.
