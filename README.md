---
title: Fabric medallion accelerator
description: Baseline-governed specification-driven development for independently configured use cases
ms.date: 2026-11-09
ms.topic: overview
---

## Reusable architecture, separate use cases

The accelerator supplies shared metadata-driven medallion mechanisms.
Treasury was the proving use case; its sources, business rules and financial
model are not the architecture or mandatory defaults for another case.

Provide reviewed source contracts and explicit Gold intent through case
Markdown. The compiler validates baseline inheritance and supported patterns,
then generates draft metadata, source/DQ/Gold/semantics context and a derived
implementation plan. The builder reuses core modules and packages only the
selected case's trusted extensions and deployment products.

Missing business keys, grain, joins, units, temporal meaning or approval
must remain blocking gaps. The compiler does not understand arbitrary prose,
invent financial meaning or convert a generated draft into native acceptance.

## Repository map

```text
baselines\
  requirements\             governing BASE requirements and machine policy
  specifications\           architecture, contracts and agent workflow
  plans\                    shared architecture plans and historical evidence
use_cases\
  catalog.json              registered case selection
  _template\                fill-in starter
  treasury\                 private local proof case (Git-ignored)
  inventory_example\        small complete non-Treasury example
  <case>\
    docling_specifications\ private source/extraction evidence
    requirements\           business outcomes and BASE crosswalk
    specifications\         source, Gold and semantic contracts
    plans\                  case delivery/evidence context
    contracts\              reviewed bootstrap or historical references
    extensions\             reviewed domain-only algorithms
    fixtures\               optional case-owned generators, snapshots and evidence
    generated\              compiler context/metadata and build products
artefacts\                  shared runtime sources, SQL and pipeline templates
deployment\                 reusable compiler, build, validation and publication
```

## Start here

Read [baseline governance](baselines/requirements/governance.md),
[the architecture](baselines/specifications/architecture.md) and
[the authoring workflow](baselines/specifications/specification_workflow.md).
Newcomers can follow [the starter](use_cases/_template/README.md) and
[inventory example](use_cases/inventory_example/README.md).

For implementation lookup, use the [task-to-code navigation](artefacts/README.md#task-to-code-navigation)
to select the relevant contract, shared entrypoint and focused tests.

```powershell
python deployment\specifications.py validate --use-case inventory_example
python deployment\specifications.py compile --use-case inventory_example
python deployment\specifications.py compile --use-case inventory_example --check
python deployment\build.py --use-case inventory_example
python deployment\build.py --use-case inventory_example --check
```

Use the existing deployment virtual environment for notebook dependencies.
Generated output remains in `use_cases/<case>/generated`; do not hand-edit
it or clone core Silver/Gold/DAG engines for a customer.

The entire `use_cases/treasury/` directory is private and Git-ignored,
including its documents, PDFs, contracts, code, fixtures and generated assets.
Keep it locally; do not force-add it. Fresh checkouts do not include this case.
Commands that select Treasury, including existing Treasury defaults, require
the private local folder. Use `--use-case inventory_example` for the shared
example. Ignore rules do not remove previously committed files from Git history.

## Guardrails and evidence

All cases inherit BASE-001 through BASE-023. Core mechanisms preserve pinned
metadata, immutable Bronze, canonical append-only Silver, explicit Gold grain
and temporal meaning, validated DAGs, fenced recovery, governed MLV/layout
operations and distinct publication gates.

Case specifications cannot override those requirements. The compiler rejects
supported structural/contract violations; human review is still required for
business correctness and any baseline version change.

No command shown above deploys, activates metadata, creates paid resources,
changes permissions or publishes business results. Local validation is not
native Fabric proof. Treasury's previous evidence remains a scoped historical
record, not certification of every case.

See [deployment](deployment/README.md) and
[the restructuring plan](baselines/plans/specification_driven_accelerator/plan.md)
for current implementation and compatibility boundaries. Treasury-specific
guidance is available in its private local case README.
