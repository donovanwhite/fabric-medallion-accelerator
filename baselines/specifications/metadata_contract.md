---
title: Case specification and metadata contract
description: Typed authoring inputs, supported compiler scope and runtime authority
baseline_version: 1.1.0
ms.date: 2026-11-09
ms.topic: reference
---

## Authoring inputs

Every case descriptor declares four current documents: requirements,
source contract, Gold contract and implementation plan. The first three
contain one `json` fenced contract block each. Human prose supplies
explanations and evidence links, not hidden machine defaults.

All blocks identify `schema_version`, `document_type`, `use_case_id` and
`baseline_version`. Requirements inherit all BASE IDs and assign distinct
case requirement IDs with acceptance criteria. Baseline override or
credential fields are rejected, not merged into the baseline.

Existing source specifications, data dictionaries, business/process documents
and reporting designs may supply the evidence for these inputs. Docling
extracts their content; an agent can derive draft requirements, source
contracts/schemas, additional DQ, Gold/semantic intent and a case plan.
Keep document/page/section references and unresolved ambiguities explicit.
Reviewed structured contracts, not raw extraction or inferred approval,
remain the deterministic compiler input.

## Connector contract versus source-schema contract

Reuse shared [connector contracts](../../artefacts/pipeline/connectors/README.md)
to define Fabric connector/activity shape, supported authentication modes,
required metadata fields and parameter bindings. They do not contain a
case's actual source configuration, table/column inventory or credentials.

Case source-schema contracts remain required authoring inputs: they state
ordered columns, types, nulls, keys and extra DQ so agents/compiler can
produce reviewable metadata drafts. They are not another runtime authority.
At runtime, source configuration, columns/projections, keys/rules and
Fabric connection/authentication references come from the approved metadata
database version and its verified immutable run snapshot.

Credentials/tokens remain in Fabric-managed connections/authentication,
never plaintext metadata. The control-database connection and authorized
workspace/Lakehouse bindings remain deployment prerequisites; a template
cannot bootstrap metadata lookup without a trusted root binding.

## Standardized Bronze-to-Silver and DQ

Every case reuses the same Bronze-to-Silver engine. Metadata supplies
source-specific schemas, business/hash keys, supported mappings, relationships
and rule parameters; it does not select a forked ingestion implementation.
Preserve the shared manifest, hashing, append-only versioning, replay,
quarantine, reconciliation and commit semantics.

Technical DQ is inherited from the shared engine and the declared contract:

* Validate exact column membership/order and malformed input records.
* Validate supported types/representations, lengths, numeric bounds,
  precision/scale and date/time formats where declared.
* Enforce required values, non-null business keys and within-batch key
  uniqueness.
* Validate declared relationships and required source/table membership;
  reconcile accepted/rejected counts without silent loss.

The source schema and keys are still required to parameterize these checks.
An empty `dq_rules` or `specialist_rules` list means no additional domain
rules, not no technical DQ. Use those lists for supported extra validity,
cross-field or specialist business requirements. Declare their scope,
version, severity and action without weakening the technical floor.
Do not infer business relationships, ranges or cleaning transformations
from column names or incomplete extracted text.

## Initial compiler scope

Baseline 1.1.0 supports managed Fabric SQL Database `FabricSql`/`OAuth2`
and SQL Server `SqlServer`/`Basic` connection references,
ordered losslessly serializable source columns, explicit business/hash
keys, typed generic DQ rules, source-local foreign keys, observed-time
SCD2, atomic/position facts, explicit calendars, simple single-fact SUM/COUNT
MLV aggregates and declarative semantic measures.

Both SQL connectors use `source_type: sql`; `connector_type` distinguishes
their shared adapters. Existing contracts without `connector_type` retain
the Fabric SQL default. Fabric SQL requires resolved workspace/artifact
source geometry; SQL Server uses its managed connection and must not invent
Fabric source item IDs. New SQL Server execution requires explicit
`sql-server-copy-basic` native evidence, never a generated verification flag.
The BASE invariants and standardized Bronze-to-Silver semantics are unchanged.

Declare the source instance/schema, ownership, null/type/precision/date
rules, grain, key order, tracked value order and layout. Declare every Gold
source, grain/key/date, measure unit, dimension lookup, correction/snapshot
meaning, fiscal calendar and unavailable measure. Initial same-name
dimension lookups are supported; unapproved aliasing, arbitrary SQL/Python,
new transports and new algorithms are blocking extension requests.

Treasury is imported through its reviewed snapshot/profile adapter to
preserve proof-case identities, rather than pretending that domain prose
can recreate certified physical contracts.

## Generated products and authority

The compiler writes `generated/metadata/definition.json`, reviewable source/
Gold contracts, fixed safe extension adapters, a compilation receipt and
derived specification/plan Markdown. The receipt pins baseline, input and
output hashes. Hand-edited generated outputs block regeneration.

Shared runtime code is reused in case-scoped notebook bundles and a derived
deployment manifest. Compilation never fabricates connection IDs, capability
proof, validator approval, native results, model publication or data.
Source schemas and Gold intent alone cannot establish missing business
decisions; unresolved inputs must be clarified before generation.

## Change handling

Recompile only the selected case. Validate baseline inheritance and complete
dependencies, compare draft outputs and preserve data/history before
deployment. A configuration change does not itself become a business event.
An incompatible physical schema/layout or changed algorithm requires
separate migration and acceptance; generated Markdown cannot authorize it.
