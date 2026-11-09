---
title: Metadata-driven medallion refactoring requirements
description: Requirements for a Fabric SQL orchestration control plane, metadata-driven Copy landing, append-only Silver, governed Gold and notebook DAG execution
author: Data and AI Engineering
ms.date: 2026-11-06
ms.topic: reference
---

## Scope baseline

Status: requirements baseline for refactoring, not an implemented capability
or approval to change deployed resources.

Replace deployment-generated source arrays and embedded processing contracts
as the runtime configuration authority with a Microsoft Fabric SQL database
named `db_orchestrator`. Use one master pipeline, provisionally named
`pl_bronze_silver_gold`, to orchestrate all three layers.

Bronze and Silver use reusable, metadata-driven processing. Gold uses the same
orchestrator and supports reusable dimensional patterns, approved SQL and
versioned customer-specific notebooks. Gold business meaning is explicitly
authored, not inferred from source names or column similarity.

### Evidence and current boundary

Discovery used the existing source, ingestion, Gold and deployment documentation,
the reviewed runtime/compiler contracts, the supplied example artefacts and the
architecture decisions in the associated conversation.

The initial source boundary remains BAS, CSD and PERSAL: 17, 21 and 6 tables
respectively, giving 44 source tables. This is a refactor of their processing,
not authorisation to add other enterprise systems or manual uploads.

The existing two schema-enabled Lakehouses remain:

| Lakehouse             | Responsibility                                                |
| --------------------- | ------------------------------------------------------------- |
| `lh_brz_slv_treasury` | Immutable Bronze files, append-only Silver, quarantine, audit |
| `lh_gld_treasury`     | Dimensions, atomic/position facts, release controls and MLVs  |

`db_orchestrator` adds an operational control plane. It does not replace either
Lakehouse or become a store for replicated business records or credentials.

The requested scope includes connection references and authentication metadata,
sources and source types, descriptions, objects, columns, ordering, layer,
DQ rules, transformation rules, MLV queries, constraints and physical layout.
It also includes runtime dependency resolution, notebook parallelism, replay,
lineage and migration acceptance.

Out of scope for this requirements document are implementation code, deployment,
permission changes, a metadata administration application, new semantic models
or reports, production source certification and destructive data migration.

### Decisions and assumptions

* One master pipeline is the operational entry point. Reusable connector
  branches, child pipelines and worker notebooks are allowed; separate
  source-specific pipelines are not required for normal onboarding.
* ForEach and Copy activities remain mandatory for external-to-Bronze landing.
  Notebook DAGs handle transformations, not a JDBC replacement for landing.
* Connections remain Fabric-managed. Authentication metadata describes and
  validates a connection; it does not contain its credentials.
* There is one per-target layout flag selecting partitioning or liquid
  clustering, not two independent enable/disable flags.
* Silver remains one canonical append-only table per source table. SCD2 belongs
  in Gold, with transaction-grain facts and MLV aggregates.
* Metadata-only onboarding applies to already supported connector and
  transformation types. A new connector implementation or new business
  algorithm can require a reviewed framework extension.
* Existing production-readiness and business-publication gates remain in force.
  Technical completion is not approval to publish Treasury business results.

## User scenarios and testing

### Onboard and change a supported feed (P1)

A data engineer registers an authorised connection, source, objects and ordered
columns, then activates an approved metadata version.

Given a supported connector and valid contract, when the next run selects that
version, then its enabled objects are processed without editing pipeline arrays,
rebuilding source-specific notebooks or redeploying the master pipeline.

Given a configuration change during an active run, when another task starts,
then it still uses the version pinned at the start of that run.

### Land different source transports (P1)

A data engineer configures relational and file feeds through the same entry point.

Given an SQL feed, when landing runs, then ForEach/Copy uses its authorised
connection, ordered projection and run-scoped Bronze location.

Given a supported FTP/SFTP feed, when landing runs, then the connector-specific
Copy configuration uses its file location and format metadata. The test must
use a controlled endpoint; an SQL simulator is not evidence of FTP support.

### Retain business changes and reject invalid data (P1)

A data steward configures keys, types, tracked attributes and DQ rules.

Given initial value A, changed value B and a later return to A for one key, when
all three observations pass validation, then three committed Silver business
versions remain. Replaying an observation creates no additional version.

Given a blocking DQ failure, when another table has already staged accepted
versions, then no source-wide commit is published and Gold cannot consume
those uncommitted versions.

### Run independent transformations concurrently (P1)

An operator starts a run whose task dependencies are derived from metadata.

Given two independent targets and a concurrency limit of two, when their DAG
executes, then the two tasks may overlap and their dependent task waits for both.

Given a cycle, missing dependency or conflicting target writer, when the run
is prepared, then it fails validation before dependent data writes.

### Deliver governed Gold through the same framework (P1)

A domain modeller registers dimension, fact and aggregate definitions.

Given committed source batches and approved Gold rules, when the Gold DAG runs,
then dimensions precede dependent facts, fact grain is preserved and aggregates
reconcile to the selected fact outputs.

Given a specialist transformation not expressible by a supported generic
pattern, when a versioned notebook is registered, then it participates in the
same dependency, parameter, audit and failure contracts.

### Select a physical layout (P2)

A data engineer selects the layout flag and its associated columns.

Given the partitioned selection, when an eligible Delta target is created,
then only its partition columns determine that layout. Given the liquid
selection, only its clustering columns configure the selected strategy.

Given conflicting or unsupported layout metadata, when validation runs, then
the configuration is rejected rather than ignored or partially applied.

### Recover without losing lineage (P1)

An operator resumes a failed transformation using a retained extraction.

Given completed Bronze landing and a failed downstream task, when replay is
requested, then the original source batch, contract and observation timestamp
are reused without rerunning Copy or modifying its files.

Given a failed or uncertain submission, when recovery runs, then it inspects
the recorded job and task state before submitting another attempt.

### Edge cases

Acceptance must cover disabled required upstream objects, empty permitted files,
missing files, duplicate keys, malformed CSV, reordered columns, schema changes,
unavailable credentials, duplicate connection names and multiple transports
claiming the same logical dataset.

It must also cover interrupted metadata reads, oversized metadata selections,
same-source concurrent runs, partial Silver writes, fact corrections, late
observations, MLV refresh failure and metadata or layout rollback.

## Requirements

The requirement identifiers below are unique within this specification.
All are mandatory for the refactored capability unless an explicit scope gate
states otherwise. Logical field and entity names do not prescribe final DDL.

### Runtime control plane and configuration

* REQ-001: The framework must use `db_orchestrator`, a Fabric SQL database, as
  the authoritative source of active runtime configuration for Bronze, Silver
  and Gold. Changing approved metadata for a supported object must affect the
  next selected run without editing a deployed source array.
* REQ-002: Configuration must have stable identifiers, immutable approved
  versions, ownership and recorded activation history. A run must pin the
  complete applicable configuration and dependency set before processing;
  concurrent edits must not change its remaining tasks.
* REQ-003: Every processable object must declare its layer as `bronze`,
  `silver` or `gold`, its materialisation/load strategy and its upstream
  dependencies. The same logical dataset may have linked objects in different
  layers; a single mutable layer label must not replace those relationships.
* REQ-004: Metadata must retain separate column ordinals, execution priorities
  and dependency edges. Ordered projections must be deterministic, and
  execution priorities must not override dependency readiness.
* REQ-005: Contract and rule changes must be validated before activation.
  Unknown fields, invalid identifiers, missing keys, ambiguous mappings,
  unsupported capabilities and incompatible schema changes must produce
  actionable errors, not implicit defaults or automatic destructive evolution.
* REQ-006: The framework must support a read-only execution preview showing
  selected objects, connections, dependencies, paths, rule versions, layouts
  and expected targets. Reverting the active configuration must affect future
  runs without rewriting the configuration recorded for earlier runs.

### Connections and extraction

* REQ-007: Each feed must reference its Fabric connection, connector type,
  expected authentication type and applicable gateway. Connection resolution
  must reject missing, ambiguous, unauthorised or incompatible selections;
  no connection belonging to an unrelated feed may be substituted silently.
* REQ-008: Secrets, passwords, bearer tokens and refresh tokens must not be
  stored in orchestration metadata, notebook definitions, source control or
  logs. Authentication type must be checked against the selected connection,
  not interpreted as an instruction to generate or replace credentials.
* REQ-009: Landing must retain ForEach/Copy activities. Metadata selects
  supported connector-specific configurations and connection references;
  changing a type string must not be assumed to turn one connector into
  another or to support an authentication method that connector does not offer.
* REQ-010: SQL extraction metadata must describe source instance, schema,
  table/view, ordered columns and approved serialisation. Query values must
  use parameter binding where supported; identifiers must be validated and
  quoted. Untrusted metadata must not become arbitrary executable SQL.
* REQ-011: File extraction metadata must describe connector/protocol,
  directory or object location, file selection, format and applicable parsing
  settings. FTP and SFTP are distinct connector capabilities. File-completion
  checks and arrival/readiness policy must prevent partial arrivals from
  being treated as complete source batches.
* REQ-012: The framework must preserve exact source representations, including
  identifier zeros, bigint precision, decimal/money scale, Boolean encodings,
  column casing and declared date/time semantics. SQL `bit` transport must
  retain `0`/`1`; source Boolean text must retain its separately declared
  representation. A leading UTF-8 BOM must not change CSV column identity.
* REQ-013: Disabled objects must be excluded from a new extraction plan.
  A disabled object required by a selected downstream task must block that
  task unless an explicit valid alternative dependency is configured.
  Metadata retrieval must detect truncation and paginate or batch rather
  than silently omitting configured objects.
* REQ-014: Feeds delivering the same logical dataset must declare source
  identity, authority, overlap and conflict policy. Merely using different
  transports must not create duplicate business versions or merge unrelated
  source instances under identical business keys.

### Bronze naming, immutability and batch identity

* REQ-015: New landing paths must follow
  `Files/bronze/<source>_<description_slug>_<source_type>/<yyyyMMdd>/<run_id>/`.
  Name components must be validated lowercase stable tokens, with a
  human-readable description stored separately from `description_slug`.
* REQ-016: SQL landing must produce the configured per-object files; file
  connectors must retain original source files and their names where
  applicable. The batch manifest must resolve every landed path explicitly,
  allowing multiple files per logical object without relying on filename guesses.
* REQ-017: Each extraction must record a fixed UTC observation timestamp,
  source/feed identity, run ID, processing date, configuration/contract
  version, selected object membership and file/row evidence. A batch identity
  must not be reused to relabel changed content or a changed contract.
* REQ-018: Retained Bronze must be immutable and replayable. Failed batches
  must remain available, with no automatic deletion, overwrite or invented
  retention period. A failed Copy requiring re-extraction must use a new
  batch identity; transformation replay must not invoke Copy again.
* REQ-019: Existing `Files/bronze/<source>/...` paths must remain resolvable
  through their historical contracts/manifests. Introducing the new naming
  pattern must not require moving or rewriting historical files.

### Silver contracts, DQ and commits

* REQ-020: Silver schemas must follow `slv_<source_code>` by default and
  preserve source table names. The current explicit mappings `slv_bas`,
  `slv_csd` and `slv_psl` must remain compatible; a PERSAL schema rename
  requires a separate reviewed migration, not an implicit naming correction.
* REQ-021: Every source table must retain one canonical append-only Silver
  table containing business values, keys, SHA-256 key/value hashes,
  version/previous-hash metadata and batch/observation lineage. Separate
  business history tables, mutable current copies and per-record
  observation-index stores must not be introduced.
* REQ-022: Incoming values must be compared with the latest committed
  version for their business key. Unchanged values and replay must add no
  business version; A-to-B-to-A must preserve all accepted transitions.
  Metadata-only changes must not manufacture business-change events.
* REQ-023: DQ metadata must support required values, types, ranges, precision,
  column order, key uniqueness, referential consistency, reconciliation and
  source-specific business checks. Each rule must identify its scope,
  version, severity and explicit action: fail, quarantine or warn.
* REQ-024: Rejected records and rule evidence must remain in source-isolated
  quarantine, with safe operational summaries. No rule may silently discard
  a record. Existing blocking source-contract rules remain blocking unless
  an explicitly approved rule change defines different behaviour.
* REQ-025: A source-wide commit must be written only after all required
  objects, reconciliation and blocking checks pass. Partial writes and
  per-table success must not make an incomplete source batch consumable.
  Repeat execution must not duplicate a successful commit marker.
* REQ-026: Missing source rows must not imply deletion without an approved
  delete contract. Full polls must not be represented as transaction-log CDC;
  intermediate changes between observations require an authoritative CDC
  or event feed, not an orchestration metadata setting.

### Gold rules, dimensions and facts

* REQ-027: Gold must use the same orchestration and audit framework while
  allowing reusable dimension/fact patterns, approved SQL and versioned
  customer/domain notebook transformations. A new approved domain rule
  must not require a separate master pipeline.
* REQ-028: Every Gold definition must explicitly declare grain, business
  identity, sources, join keys/cardinality, time semantics, measures, units,
  null/unknown handling and correction policy. The framework must not infer
  business joins, department ownership or financial meaning from names.
* REQ-029: Dimensions must support configured SCD2 keys and tracked
  attributes, deterministic version identity and non-overlapping validity.
  Existing observed-time semantics must remain explicit; they must not be
  backdated as business-effective history without a certified policy.
* REQ-030: Financial facts must retain transaction/detail grain where the
  source supplies it. Genuine positions must retain their declared snapshot
  grain. A corrected event must not be counted as another payment or another
  unit of expenditure merely because Silver contains another after-image.
* REQ-031: Facts must wait for their required dimensions and committed source
  selections, using the configured temporal lookup and unknown-member policy.
  Cross-source cutoffs must be explicit; the existing common-extraction-date
  restriction remains until an alternative cutoff policy is approved.
* REQ-032: Gold completion must record selected source batches, metadata/rule
  versions, validation results and output Delta table versions. Release
  membership must remain pinned by output versions, not by requiring every
  unchanged row to carry the latest `gold_release_id`.
* REQ-033: Missing approved budgets, forecasts, complete liabilities, mappings,
  governance registers or publication evidence must remain unavailable.
  The framework must not generate placeholder measures or classify technical
  `FACTS_READY`/`MLVS_VERIFIED` states as business approval.

### Materialized lake views and constraints

* REQ-034: Aggregate definitions must support a versioned `mlv_query`,
  declared inputs, output columns/grain, applicable constraints and refresh
  policy. MLVs must aggregate curated facts; they must not replace required
  transaction detail or implement mutable SCD2/fact-correction storage.
* REQ-035: MLV authoring must be a controlled definition lifecycle, not
  unconditional recreation on every data run. Definitions require a
  schema-enabled Lakehouse, approved SQL and engine-capability validation;
  unsupported syntax must fail explicitly.
* REQ-036: Refresh must use Fabric's supported MLV lineage/refresh controls,
  whether scheduled or coordinated after Gold readiness by the master
  pipeline. The selected refresh must complete and reconcile before aggregate
  readiness is recorded. Conflicting scheduled and pipeline refreshes must
  not publish aggregates for an unintended Gold input state.
* REQ-037: Incremental refresh must not be promised solely because CDF is
  enabled or an MLV exists. Required source settings, query eligibility and
  observed refresh behaviour must be checked. Full-refresh fallback and its
  capacity implications must be visible to operators.
* REQ-038: Constraints must distinguish row validity, object-level uniqueness,
  relationship/reconciliation rules and engine-enforced constraints.
  Declaring a logical key must not imply the storage engine enforces it.
  MLV row-drop actions require explicit policy and rejection evidence; silent
  loss is not acceptable for financial reporting.

### One physical-layout flag

* REQ-039: Each eligible managed Delta target must use one layout selection
  flag, logically named `layout_strategy`, choosing `partitioned` or `liquid`.
  There must not be independent partitioning and liquid-clustering Boolean
  toggles that can both be enabled.
* REQ-040: The partitioned selection must use its validated ordered
  `partition_columns`; the liquid selection must use its validated
  `cluster_by_columns` (the requested `clusterBy` metadata). Conflicting
  inactive-layout settings, missing required columns and unknown columns
  must be rejected before writes.
* REQ-041: Layout settings must apply only to compatible materialisations.
  Raw Bronze files are not Delta layout targets. MLVs must accept only
  options supported by their documented/validated engine surface; a Delta
  table `CLUSTER BY` capability must not be assumed to apply to MLV DDL.
* REQ-042: Layout creation, alteration and maintenance must follow the
  selected strategy and supported runtime. The liquid selection must not
  invoke Z-Order. Maintenance policy must distinguish configuring clustering
  columns from executing the physical optimisation work.
* REQ-043: Existing unpartitioned or differently organised tables must remain
  unchanged until an explicit compatible layout decision and migration are
  approved. Flipping metadata must not silently repartition, replace or
  rewrite historical tables. Such migrations require data equivalence and
  downstream-consumer compatibility evidence.

### Notebook DAG and run lifecycle

* REQ-044: One master pipeline must coordinate the selected layers. Source,
  object, rule and dependency selection must occur at runtime from the
  pinned control-plane version, not from separately maintained source arrays.
* REQ-045: Notebook DAGs must be generated as structured data and validated
  with `notebookutils.notebook.validateDAG` before execution through
  `notebookutils.notebook.runMultiple`. Activity identities must be unique;
  missing dependencies and cycles must fail before dependent execution.
* REQ-046: DAG concurrency, cell/task timeout and total timeout must be
  explicit and bounded. Independent targets may run concurrently, but
  same-target writers and overlapping same-source batches must be serialized.
  Shared audit namespaces/tables must be initialised before parallel writers.
* REQ-047: DAG execution must respect parent/child Lakehouse bindings.
  Separate Silver and Gold parent notebook stages are allowed for the
  current two-Lakehouse design. Parallel reference runs must not be treated
  as independent unlimited Spark clusters.
* REQ-048: Workers must receive stable run/task/configuration references and
  retrieve the pinned contract without embedding whole mutable metadata
  payloads in pipeline parameters. Every selected worker implementation
  must be version-identifiable and execute without hidden interactive state.
* REQ-049: Every task must report success, failure and structured counts or
  output references. Exceptions must propagate to the orchestrator after
  audit persistence; logging a failed task and returning success is prohibited.
  Required descendants and publication must remain blocked after failure.
* REQ-050: Submission and recovery must record job/attempt identity before
  monitoring, distinguish uncertain submission from known failure, and
  reconcile existing activity state before retrying. Transformation replay
  must retain original batch identity; retries must be idempotent and must
  not mask incomplete source commits or partial Gold releases.

### Deployment, migration and operational controls

* REQ-051: Deployment must provision `db_orchestrator`, its versioned schema,
  required configuration seeds and reusable pipeline/notebook assets after
  approval. Bootstrap must resolve its own metadata-database access without
  depending on an ingestion run that itself needs that metadata.
* REQ-052: Connection handling must retain the original accelerator's
  resolve, validate and bind approach. Interactive deployment must identify
  missing or expired prerequisites and support a guided recheck/resume
  after user-controlled connection setup/consent. Noninteractive and dry-run
  modes must fail with actionable prerequisite details, never wait on input.
  Automated delegated OAuth consent or browser automation is not required.
* REQ-053: Existing registries and schemas must seed a reconciled initial
  metadata version rather than remain a competing runtime authority.
  `deployment/build.py`, local validation and the inventory manifest must
  be adapted for reusable assets and metadata contracts. The refactor must
  not assume the previous 26-item inventory remains the target inventory.
* REQ-054: Activation must retain historical configuration and support
  read-only comparison with existing behaviour. New naming, keys, schemas,
  hash semantics and layout must not silently alter accepted historical
  data. Old pipelines must not be scheduled concurrently with replacement
  writers; decommissioning needs separate approval after parity evidence.
* REQ-055: Run audit must connect source/feed, object/layer, configuration,
  transformation/rule version, source batch, task attempt, job, timestamps,
  counts, DQ outcomes and target versions. UTC observation/run timestamps
  must remain distinct from business dates and timezone-unspecified fields.
* REQ-056: The framework must reconcile source, accepted and quarantined
  counts and retain rule-level results. Operational failures must identify
  the failing object/task and remediation context without exposing source
  records, credentials or sensitive SQL parameter values in general logs.
* REQ-057: Metadata activation and executable rule changes must require
  authorised, auditable control. Runtime readers, metadata administrators
  and deployment operators must have distinct least-privilege needs;
  choosing a connection must not grant access to it.
* REQ-058: Capacity planning must account for the additional SQL database,
  Copy concurrency, shared Spark sessions, DQ work, Gold processing, MLV
  fallback and layout maintenance. On the existing F8, concurrency must be
  measured and bounded rather than derived from a platform maximum.
* REQ-059: Rollback must distinguish active configuration, worker version,
  data/release selection and physical layout. It must not delete historical
  batches, truncate Silver or automatically rewind a production target.
  No cross-store atomic transaction may be claimed for SQL metadata,
  OneLake files and Delta outputs.
* REQ-060: The refactor must preserve source classification/privacy
  boundaries, department attribution and existing consumption gates.
  CSD remains a shared supplier master, not department-owned data.
  No new RLS assignments, consumer permissions, production certification
  or business-publication mechanism is authorised by this specification.

### Key entities and required metadata

The following is a logical allocation of metadata, not final table DDL.
Configuration entities require stable IDs and version relationships.
Multivalued columns, rules and dependencies must remain structured, not
comma-separated strings whose meaning depends on display order.

| Logical entity         | Required content                                                                                                                                                |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Configuration version  | Version identity, status, owner, definition/contract hashes, validation result and activation history                                                           |
| Connection             | Logical name, Fabric connection reference, connector type, authentication type, gateway reference and expected access context; no credentials                   |
| Source/feed            | Logical source and instance, source type/transport, stable description slug, editable description, connection reference, authority/overlap policy, enabled flag |
| Data object            | Feed or upstream references, layer, source table/view or file locator, target, format, load/materialisation type, enabled flag and execution priority           |
| Object column          | Object/version reference, source/target column, ordinal, type/precision/scale, nullability, business-key role, hash-tracking role and serialisation             |
| DQ rule                | Rule identity/version, object/column scope, rule type or approved expression, severity, thresholds and fail/quarantine/warn action                              |
| Constraint             | Constraint identity/type, participating objects/columns, expression or key definition, enforcement mechanism and violation policy                               |
| Transformation         | Versioned pattern, approved SQL or notebook/implementation reference, parameters, output contract and customer/domain ownership                                 |
| MLV definition         | Output object, approved `mlv_query`, input dependencies, output grain, constraints, refresh policy and definition/version identity                              |
| Storage layout         | Target, single `layout_strategy` flag, `partition_columns` or `cluster_by_columns`, supported runtime/capability and maintenance policy                         |
| Object/task dependency | Upstream and downstream identities, required completion/commit condition and dependency type                                                                    |
| Run/task attempt       | Run, task, attempt and job identities, pinned version, selected batches, paths, timestamps, state, counts, errors and output-version references                 |

Connection and execution status are observations with timestamps, not proof
that a future extraction will retain access. A connection health check alone
is not proof of table permissions, file accessibility or lossless transport.

### Naming and execution contract

Examples of new Bronze locations:

```text
Files/bronze/bas_finance_sql/<yyyyMMdd>/<run_id>/atbs03bs.csv
Files/bronze/bas_finance_ftp/<yyyyMMdd>/<run_id>/<original_filename>
Files/bronze/csd_finance_sql/<yyyyMMdd>/<run_id>/supplier_identification.csv
```

Logical Silver remains independent of transport:

```text
slv_bas.atbs03bs
slv_csd.supplier_identification
slv_psl.t_employee
```

The default schema convention is `slv_<source_code>`. The existing PERSAL
mapping is an explicit compatibility binding, not permission to change
source identity or rename a populated schema.

The intended execution sequence is:

```text
Resolve connections and capabilities
  -> pin configuration, dependency closure and run identity
  -> retrieve Bronze extraction tasks
  -> ForEach with connector-specific Copy
  -> verify and seal immutable source batches
  -> execute dependency-aware Silver notebook DAG
  -> reconcile and commit each required source batch
  -> execute Gold DAG: dimensions before dependent facts
  -> validate and record version-pinned Gold release
  -> refresh required aggregate MLV lineage
  -> reconcile aggregate outputs and record technical readiness
```

This sequence is an orchestration contract, not a requirement for one
monolithic notebook. Gold business rules may be customer-specific while
submission, lineage, dependency checks and recovery remain reusable.
MLV definition changes follow a controlled lifecycle separately from
routine refresh. No Treasury business publication is implied.

## Success criteria

### Measurable outcomes

| ID     | Outcome                                                                                                                                                  | Requirement coverage        |
| ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- |
| SC-001 | One approved metadata-only object addition and one column-order change take effect on a new run without changing or redeploying the master pipeline.     | REQ-001 to REQ-006          |
| SC-002 | One multi-connection SQL run and one controlled file-connector run land exactly their configured objects through ForEach/Copy; invalid auth/type fails.  | REQ-007 to REQ-014          |
| SC-003 | New feed paths match the naming contract, and replay of one historical old-path batch remains successful without moving its files.                       | REQ-015 to REQ-019          |
| SC-004 | All 44 baseline source contracts reconcile to their selected source populations; accepted plus quarantined rows equal extracted rows per object.         | REQ-012, REQ-020 to REQ-025 |
| SC-005 | Replay adds zero business versions; A-to-B-to-A retains three transitions; an abandoned partial batch is excluded from all committed-state reads.        | REQ-021, REQ-022, REQ-025   |
| SC-006 | Invalid type, duplicate key, missing parent and malformed-file fixtures produce the configured action and attributable rule evidence.                    | REQ-023, REQ-024, REQ-038   |
| SC-007 | Gold parity preserves business grain, temporal relationships and monetary totals; correcting one event does not double its value.                        | REQ-027 to REQ-033          |
| SC-008 | Every selected MLV reconciles to its declared Gold input state, and a failed refresh prevents aggregate readiness.                                       | REQ-034 to REQ-038          |
| SC-009 | Separate fixtures demonstrate both layout-flag selections; conflicting/unsupported settings fail before writes, with zero row changes from layout alone. | REQ-039 to REQ-043          |
| SC-010 | Two independent DAG tasks overlap within the configured limit; dependent tasks wait; a cycle or required failure prevents downstream readiness.          | REQ-044 to REQ-050          |
| SC-011 | A metadata edit during execution affects zero remaining tasks in that run, and a retry is traceable without duplicate accepted output or commits.        | REQ-002, REQ-048 to REQ-050 |
| SC-012 | Approved bootstrap, repeat deployment and non-destructive rollback preserve existing data; missing credentials are reported without exposing secrets.    | REQ-051 to REQ-060          |

Gold parity comparisons must distinguish business values from expected changes
to technical run IDs and load timestamps. Raw physical Silver row counts can
include abandoned versions; committed-state equivalence is the required
comparison, with abandoned records still traceable.

No unmeasured duration, concurrency improvement or cost reduction is promised.
Before production activation, record end-to-end duration, peak concurrency,
capacity pressure, refresh mode and cost estimates against the existing
baseline using the same datasets and workload conditions.

### Refactoring exit conditions

The refactor is ready for controlled cutover only when the outcomes above
have executable evidence, unsupported capabilities fail explicitly, historical
replay remains possible and the operator can identify the exact metadata,
code, input and output versions behind a completed run.

Cutover approval does not certify unresolved production keys, department
mappings, financial definitions or business publication. Those remain the
separate gates in the source, Gold and semantic-model specifications.

## References and design provenance

### Repository evidence

* [Implementation plan, task breakdown and traceability](plan.md)
* [Source specification and coverage](../../../use_cases/treasury/specifications/source_coverage.md)
* [Current source and Lakehouse inventory](../../../use_cases/treasury/specifications/source_contract.md#review-context)
* [Immutable Bronze and append-only Silver contract](../../../use_cases/treasury/specifications/silver_implementation.md)
* [Current ForEach/Copy and replay contract](../../../artefacts/pipeline/README.md)
* [Gold grain, SCD2 and source boundaries](../../../use_cases/treasury/specifications/gold_data_model.md)
* [Gold release and MLV execution](../../../use_cases/treasury/specifications/gold_implementation.md)
* [Semantic model and publication/access boundaries](../../../use_cases/treasury/specifications/semantic_model_design.md)
* [Current deployment prerequisites and generation](../../../deployment/README.md)

### Supplied reference artefacts

The reviewed `pl_ingest_merge_delta_brz_slv.json` demonstrates metadata Lookup,
DAG construction and outer-notebook invocation.
`nb_outer_corp_func_fin_delta_py.ipynb` demonstrates parallel notebook dispatch.
`nb_create_corp_func_fin_delta_py.ipynb` demonstrates a parameterised worker,
but its mutable hash-matching merge and catch-and-print failure behaviour must
not replace the current Silver/recovery contracts.

`nb_create_delta.zip` contains identical copies of those two reviewed notebooks.
`nb_sarb_returns_scraper_py.ipynb` illustrates a specialist metadata-driven
transformation and the need to avoid oversized parameter payloads. Its manual
upload use case is not added to this accelerator's scope.

These are design references, not repository/runtime dependencies. This
specification does not copy their code, client-specific configuration,
credentials or local filesystem paths.

### Platform references

Platform capabilities must be revalidated during implementation, particularly
connector parameterisation, MLV options and cross-workload Delta compatibility.

* [Lookup activity and output limits](https://learn.microsoft.com/en-us/fabric/data-factory/lookup-activity)
* [ForEach activity](https://learn.microsoft.com/en-us/fabric/data-factory/foreach-activity)
* [Switch activity](https://learn.microsoft.com/en-us/fabric/data-factory/switch-activity)
* [Fabric connection parameterisation differences](https://learn.microsoft.com/en-us/fabric/data-factory/upgrade-planning-azure-data-factory)
* [NotebookUtils DAG execution and Lakehouse constraints](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-notebook-run)
* [Liquid clustering](https://learn.microsoft.com/en-us/fabric/data-engineering/liquid-clustering)
* [Materialized lake view SQL syntax and constraints](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view)
* [Materialized lake view refresh APIs](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/materialized-lake-views-public-api)
