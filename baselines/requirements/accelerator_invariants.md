---
title: Non-overridable accelerator requirements
description: Stable shared requirements for every metadata-driven medallion use case
baseline_version: 1.1.0
ms.date: 2026-11-09
ms.topic: reference
---

## Shared invariants

Case requirements must reference every ID below. The legacy REQ mappings
refer to [the preserved approved refactor specification](../plans/metadata_driven_medallion/requirements-as-approved.md);
they are provenance, not permission to inherit Treasury business assumptions.

| ID       | Mandatory requirement                                                                                                                   | Legacy REQ mapping           |
|----------|-----------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| BASE-001 | One approved runtime metadata authority and one master orchestration role per selected deployment; no competing source arrays           | 001, 044, 053                |
| BASE-002 | Immutable approved versions, complete run pinning and future-selection configuration rollback                                           | 002, 006                     |
| BASE-003 | Validate contracts/dependencies before activation; keep projection order, priorities and dependency readiness separate                  | 003, 004, 005, 013           |
| BASE-004 | Resolve authorized compatible connections; store references and authentication metadata, never credentials                              | 007, 008                     |
| BASE-005 | Use ForEach/Copy landing and explicitly verified connector/authentication/arrival capabilities                                          | 009, 011                     |
| BASE-006 | Preserve exact source representations; validate identifiers and prohibit unrestricted executable metadata                               | 010, 012                     |
| BASE-007 | Immutable manifest-resolved batches, exact membership and lineage; replay never relabels or recopies extraction                         | 014, 015, 016, 017, 018, 019 |
| BASE-008 | One canonical append-only Silver store; deterministic business-key/value hashes; unchanged/replay adds no version and reversions remain | 020, 021, 022                |
| BASE-009 | Versioned DQ scope/severity/actions, retained rejection evidence and no silent loss or demoted mandatory blockers                       | 023, 024                     |
| BASE-010 | Source-wide commit only after complete required membership and blocking checks; partial writes remain unpublished                       | 025                          |
| BASE-011 | Missing source rows do not imply deletion; polling does not claim authoritative transaction CDC                                         | 026                          |
| BASE-012 | Gold declares grain, scoped identity, joins, measures, units, time and correction rules; names never establish business meaning         | 027, 028                     |
| BASE-013 | Deterministic non-overlapping SCD2 identities/intervals and explicitly declared time basis; no invented effective history               | 029                          |
| BASE-014 | Preserve transaction/detail or genuine position grain; corrections do not double-count events                                           | 030                          |
| BASE-015 | Consume committed explicit selections, wait for required dimensions and pin release membership by exact output versions                 | 031, 032                     |
| BASE-016 | Missing sources/definitions remain unavailable; technical readiness never grants business publication                                   | 033                          |
| BASE-017 | Controlled versioned MLV definition/refresh/constraint lifecycle with observed mode, reconciliation and visible fallback cost           | 034, 035, 036, 037, 038      |
| BASE-018 | One exclusive layout selector, compatible columns/materialization and runtime proof; no silent historical rewrite or liquid Z-Order     | 039, 040, 041, 042, 043      |
| BASE-019 | Validate structured DAGs; bound concurrency/timeouts; serialize conflicting writers; preserve Lakehouse bindings and propagate failures | 045, 046, 047, 048, 049      |
| BASE-020 | Record attempts and jobs before monitoring; reconcile uncertainty before retry; preserve batch identity and idempotent recovery         | 050                          |
| BASE-021 | Attributable safe audit/count reconciliation; distinguish UTC observations from business dates and unspecified-timezone values          | 055, 056                     |
| BASE-022 | Authorized/auditable activation and executable changes, independent bootstrap, least privilege and non-destructive rollback/cutover     | 051, 052, 054, 057, 059      |
| BASE-023 | Preserve privacy/consumption boundaries; measure capacity/cost and keep local, native and business evidence distinct                    | 058, 060                     |

## Required proof

Every case maps requirements and acceptance to these invariants and names
its input/output contracts. Compilation proves only supported contract
shape and deterministic generation. Shared tests preserve algorithmic
behaviour; case tests prove declared domain results. Native connector,
engine, recovery, layout/MLV and consumer proof remain separately scoped.
No case may report skipped or simulated work as native acceptance.
