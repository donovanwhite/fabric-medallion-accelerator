---
title: Shared metadata-driven pipeline contracts
description: One operational master, reusable ForEach Copy landing and pinned notebook DAG execution
ms.date: 2026-11-09
ms.topic: reference
---

## Normal entry points

`pl_bronze_silver_gold` coordinates selected layers from one approved immutable
`db_orchestrator` version. `pl_copy_bronze_batch` is its reusable child for
bounded task pages and verified callback ingestion. It is not a second
source-specific operational entry point.

The proving-case sources are Fabric SQL databases. Shared Fabric SQL Database
and SQL Server adapters are selected by pinned connection metadata, not
case-specific pipeline code. The Copy loop uses connection
references, exact SQL geometry and lossless ordered projections from pinned
metadata, not deployment-generated `p_table_config` source arrays.
FTP/SFTP adapters remain distinct inactive framework components; they are
not part of the current demo execution scope.

## Reusable source connector contracts

Use the [connector contracts and template](connectors/README.md) for shared
Fabric adapter behavior and parameter bindings. Connector templates do not
own per-case source settings, table/column lists or credentials.

Approved pinned metadata supplies source configuration, schema/projections,
keys/rules and Fabric connection/authentication references. Fabric-managed
connections hold the actual authentication material. Case source Markdown
authors metadata drafts; it is not read as live runtime configuration.

## Execution contract

Prepare a pinned SQL run, export/verify the complete snapshot, then page the
selected source tasks through ForEach/Copy. Each task reserves an attributable
attempt before connector execution. No automatic Copy retry can overwrite
retained partial extraction files.

Silver waits for reconciled successful Bronze evidence. Its parent initializes
shared audit/namespaces and sealed parent populations before parallel object
workers, validates the DAG and publishes an idempotent source-wide commit only
after complete fan-in and blocking quality checks.

The selected Gold stage reserves and exports its dispatch, prepares version-
pinned committed source inputs, runs dimension/fact dependencies and verifies
complete outputs before technical `FACTS_READY`. Callback reconciliation and
a sealed success receipt preserve failures and withhold downstream readiness.
Silver-only selections skip the Gold stage.

MLV definition changes and refresh are controlled explicit lifecycle
operations, not unconditional recreation during fact processing.
No technical state grants case business approval.

## Immutable extraction and replay

New SQL landing follows
`Files/bronze/<source>_<description_slug>_sql/yyyyMMdd/<source_batch_id>/<table>.csv`.
The manifest pins exact membership, source observation time, contract and
source batch. Replay reuses these inputs without rerunning Copy.

Historical `Files/bronze/<source>/...` paths retain their original contract
and timestamps. Historical case extraction/rollback templates reside in
that case's contracts, not among the shared deployment templates.

There is no inferred delete, transaction-log CDC, automatic Bronze cleanup
or invented retention period. Partial Silver writes do not publish a source
commit. Configuration rollback does not undo data or imply table rewind.

## Validation boundary

Offline payload, fake-client, clean-namespace and local Spark checks are not
native Fabric acceptance. SQL control behavior, connector authorization,
generated-DAG execution/recovery, actual MLV/layout behavior and no-overlap
cutover still require approved runtime evidence. No deployed item or schedule
is deleted by changing the local inventory.
