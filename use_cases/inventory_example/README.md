---
title: Inventory worked example
description: Small synthetic source-to-Gold contracts illustrating case onboarding without Treasury assumptions
ms.date: 2026-11-09
ms.topic: how-to
---

## Example boundary

This is a fictional inventory-movement case, not a deployed data product.
It demonstrates explicit contracts for two SQL tables, observed product
attributes, signed movement events and a monthly movement aggregate.
Opening stock and current balance are deliberately unavailable.

Read `requirements/requirements.md`, `specifications/source_contract.md`,
`specifications/gold_contract.md` and `plans/plan.md`. The JSON blocks are
complete compiler inputs; prose explains their meaning.

```powershell
python deployment\specifications.py validate --use-case inventory_example
python deployment\specifications.py compile --use-case inventory_example
python deployment\specifications.py compile --use-case inventory_example --check
python deployment\build.py --use-case inventory_example
```

Generated files live only in this case's `generated` directory. They are
drafts, not source data or runtime approval. Resource names are illustrative
prerequisites, not evidence that corresponding Fabric items exist.
