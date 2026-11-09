---
title: Shared Bronze and Silver engine
description: Reusable typed ingestion, rule evaluation, immutable observations and canonical append-only versions
ms.date: 2026-11-09
ms.topic: reference
---

## Shared responsibilities

Core ingestion verifies exact raw membership/headers, preserves declared
representations, applies typed/versioned DQ, retains rejected evidence,
compares explicit business-key/value hashes to committed state and appends
accepted business versions. Source-wide validation gates publication.
Unchanged observations/replay add no version; A-to-B-to-A retains accepted
transitions. No missing-row delete or automatic Bronze cleanup is inferred.

The generic rule engine owns supported handlers and mandatory baseline
checks. Reviewed case plugins own specialist predicates and source-wide
business meaning. Do not put source-name switches or customer dictionaries
back into the shared engine.

## Case contracts

The selected pinned manifest supplies source identity, schemas, ordered
columns, hash inputs, rules, layout and original observation/batch lineage.
Worker context must match the resolved metadata object; no universal
source trio or customer schema abbreviation is assumed.

Each case's specifications record its source meaning, specialist rules
and compatibility evidence. No case's snapshots are required by the engine.

Generated parents/workers live under the case's `generated/notebooks`;
source-specific notebook chains are not normal outputs. Shared initialization,
bounded DAG scheduling, per-task evidence and complete source fan-in remain
reusable orchestration mechanisms.
