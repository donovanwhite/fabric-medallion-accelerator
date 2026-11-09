---
title: New use-case starter
description: Copy this directory and fill explicit source and Gold contracts without changing core architecture
ms.date: 2026-11-09
ms.topic: how-to
---

## Copy and name the case

Copy this directory to `use_cases/<case_id>`. Replace every angle-bracket
placeholder, use a lowercase identifier and register it in
`use_cases/catalog.json`. The template is intentionally not registered
or compilable before business decisions are supplied.

Read `baselines/README.md` and the complete synthetic
`use_cases/inventory_example` first. Do not copy Treasury-specific names,
codes, fiscal calendar, source keys or financial meanings.

## Fill these inputs

You can supply existing specifications, data dictionaries, schema exports
and business/reporting documentation. Use Docling for extraction, then an
agent to draft the following Markdown contracts with evidence references.
Review the derived decisions; you need not retype documented information,
but extraction does not resolve missing or contradictory business meaning.

* `use_case.json`: identity, resources, item namespace and adapter paths
* `requirements/requirements.md`: owner, outcomes, all inherited BASE IDs,
  case requirements and measurable acceptance
* `specifications/source_contract.md`: explicit ordered columns, source
  types, nulls, keys/hash inputs, DQ, relationships and supported layouts
* `specifications/gold_contract.md`: exact grains, joins, measures/units,
  time/correction rules, fiscal calendar, aggregates and unavailable outputs
* `plans/plan.md`: implementation dependencies and evidence gates

Keep supplied/raw extraction material in the private Docling area. Link
reviewed evidence without committing sensitive source documents or records.
Leave disputed meaning unresolved and stop generation; never fill a key
or approved-looking measure by guessing.

## Inherit processing and technical DQ

Reuse the standardized Bronze-to-Silver engine without case-specific forks.
Its schema/type, required/key, uniqueness, declared-constraint and
reconciliation checks form the inherited technical DQ floor. Provide the
schema and keys that parameterize them; add business checks through supported
`dq_rules` or reviewed specialist rules. Empty extra-rule lists do not turn
off standard DQ, and extra rules cannot weaken it.

## Generate and review

Validate and compile the selected case, then build from shared runtime
sources. Output belongs to its `generated` directory. Do not hand-edit
generated files or copy core worker code into a case.

The initial compiler supports explicit SQL/generic rule/dimensional patterns.
An unsupported algorithm needs a reviewed case extension or baseline
capability change. Neither route may weaken baseline guardrails.
