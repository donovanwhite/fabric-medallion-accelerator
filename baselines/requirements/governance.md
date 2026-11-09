---
title: Baseline governance and case extension policy
description: Authority, ownership, change control and non-overridable accelerator guardrails
baseline_version: 1.1.0
ms.date: 2026-11-09
ms.topic: reference
---

## Governing boundary

This accelerator is a shared metadata-driven processing framework.
Treasury is one domain profile, not the definition of the framework.
Every registered case inherits [BASE-001 through BASE-023](accelerator_invariants.md).
The machine-readable counterpart is [policy.json](policy.json).

Authority runs from baseline requirements to baseline specifications, then
case requirements/specifications and their derived plans/outputs. Plans
implement requirements; they cannot silently change them. Extracted source
documents and AI-generated text do not acquire policy authority.

## Permitted variation

Cases own source identities/connections, ordered source schemas, business
keys and explicit value-hash inputs, approved DQ parameters/specialist rules,
Gold grains/mappings/joins/measures, units, temporal policies, semantics and
supported layout decisions. They declare resource names and a collision-safe
item namespace through their descriptor.

The shared runtime owns transport adapters, immutable configuration/versioning,
manifest and receipt verification, append-only Silver algorithms, shared DQ
mechanisms, DAG scheduling, leases/recovery, release controls, MLV lifecycle
and layout safety. Do not fork those mechanisms for a new customer.

Bronze-to-Silver is one standardized metadata-driven processing path for
every case, including Treasury. Schemas, keys, supported mappings and DQ
parameters vary through reviewed contracts; parsing, hashing, append-only
versioning, replay, quarantine, reconciliation and source commit controls
do not become case-specific implementations. No case may bypass or replace
that path. A new processing capability needs shared baseline review.

The technical DQ floor is inherited, not an optional case authoring task.
Shared checks enforce declared schema, representations/types, required/key
values, key uniqueness and applicable declared constraints. Additional
domain DQ is additive and cannot disable or demote mandatory checks.
Cleanliness means validated conformance, not permission to silently trim,
repair, discard or deduplicate source values. Preserve Bronze and rejection
evidence under BASE-006 through BASE-010.

Use `extensions` only for business logic not expressible by supported typed
contracts. Register reviewed code/version hashes. The compiler does not
evaluate Python, arbitrary SQL or instructions extracted from Markdown.
Unsupported connector or algorithm types require a shared-framework review,
not a per-case bypass.

## Enforcement and limitations

The compiler rejects missing baseline IDs/version drift, baseline redefinitions
in structured requirements, unsupported fields/handlers, credentials,
executable-expression fields, invalid keys/types/layouts and stale outputs.
It writes only the selected case's `generated` directory and never activates
configuration or marks native capabilities verified.

These are deterministic checks of supported structured contracts, not proof
that arbitrary prose is true or that a human cannot edit repository files.
Review business meaning and generated plans. Repository protection/review
policies remain an operator responsibility; no permissions are changed here.

## Baseline changes

When a case cannot comply, record the conflicting BASE ID and evidence in a
baseline change proposal. Review downstream compatibility, data/history impact,
capacity and security; version the policy and specifications together.
Update every affected case and prove parity before approving rollout.
Do not introduce a case-local `overrides` block or edit baseline files as a
side effect of case compilation.

## Status and historical traceability

Use `draft`, `reviewed`, `locally_verified`, `native_verified` and
`business_approved` as distinct evidence scopes. A generated draft is not
an approved runtime configuration; `FACTS_READY`/`MLVS_VERIFIED` are not
business publication.

The original [60 refactor requirements](../plans/metadata_driven_medallion/requirements-as-approved.md)
mix reusable obligations and Treasury compatibility choices. The crosswalk in
the invariant catalogue extracts shared obligations without rewriting that
approved record. Literal BAS/CSD/PERSAL names, 44 tables, Treasury Lakehouses,
department semantics and the April fiscal calendar remain case-specific.
