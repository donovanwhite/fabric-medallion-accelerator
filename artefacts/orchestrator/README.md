---
title: Shared metadata control and orchestration
description: Reusable version pinning, immutable transport, DAG execution, recovery and release safety
ms.date: 2026-11-09
ms.topic: reference
---

## Architecture and authority

Follow [the baseline](../../baselines/README.md). SQL configuration versions,
layered objects, columns, rules, transformations, constraints and dependencies
are runtime authority. Workers consume complete verified pinned manifests,
not repository Markdown or mutable active selection.

Read metadata through the OLTP endpoint. Mirrored large-object values are
not configuration authority. Preserve compact run references, complete
transport hashes, paging and exact task/worker membership.

## Shared modules

* `schema.sql` and `procedures.sql`: configuration lifecycle, control ledger,
  attempt/source/target ownership, dispatch and reconciliation
* `silver_orchestration.py`: immutable manifests/receipts, context resolution,
  Silver DAG/fan-in, safe task results and callback transport
* `gold_orchestration.py`: Gold preparation/object fan-in and
  explicit MLV lifecycle integration requiring authoritative ownership
* `layout.py`: compatible prewrite inspection/creation, maintenance
  and separately gated migration preview
* `mlv.py`: definition/version checks, submission uncertainty,
  job recovery, actual-mode evidence and exact aggregate reconciliation

Domain code/resources are supplied by registered case profiles.
No module should infer a case from a source or output name.

## Case isolation and execution

Use the catalog and explicit `--use-case` selection. The builder packages
shared sources with that case's trusted specialist modules into its own
`generated/notebooks`, and derives its deployment manifest. Resource/item
names and prefixes belong to the case, not global business defaults.

Initialize shared objects before parallel workers. Validate DAGs before
execution, bound concurrency/timeouts, reserve attempts before submission
and reconcile existing service state before any retry. Failures withhold
required descendants and readiness, never become successful callbacks.

Gold completion pins exact outputs; unchanged rows need not carry the newest
release ID. MLV serving requires real ownership/mode observation and verified
inputs. Layout metadata cannot silently rewrite existing tables.
No cross-store transaction or business-publication approval is implied.

## Evidence boundary

Historical local proof is preserved in the baseline plans and case records.
Its source counts, resource names and scoped native blockers do not define
every new case.
Current case compilation/build checks do not certify live connector,
SQL/Delta/DAG, MLV/layout, rollback or consumer behavior.
