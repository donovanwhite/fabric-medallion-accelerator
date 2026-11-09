---
title: Metadata-driven medallion implementation plan
description: Dependency-ordered refactoring tasks and complete requirement traceability for the Fabric orchestration control plane and medallion runtime
author: Data and AI Engineering
ms.date: 2026-11-07
ms.topic: reference
---

## Summary

Current local disposition (2026-11-09): normal generation/publication now uses
six reusable notebooks and an 11-item inventory. The 15 source-specific
notebooks and standalone Gold build notebook are removed locally; shared
algorithms, historical definition builders and parity fixtures remain.
Source connection/geometry binding, lifecycle operation exports, local
MLV/layout adapters and Gold runtime-quality checks have executable local
evidence. The MLV preview does not install a native serving-ownership or
refresh-mode observer, and physical migration still requires a separate
target-specific contract.

The user selected local-only work and Fabric SQL pseudo sources; current
FTP/SFTP execution is deferred. This local inventory transition is not
native acceptance, A3 cutover or A4 decommissioning. No deployed legacy
items, data, schedules, roles or active SQL configuration were changed.
See [local acceptance and outstanding gates](validation/acceptance.md),
[rollback boundaries](validation/rollback.md) and
[cutover/decommissioning status](validation/cutover.md).

The following delivery summary records the earlier foundation/B05 baseline.
Later local components do not mark upstream-blocked native tasks complete.

Status: implementation in progress. T001-T015 are verified as the additive
control-plane foundation, and the independent T049 control-project compiler is
verified locally. T016-T021 remain in progress. T022-T030 have local
components and evidence, but acceptance is blocked by their unfinished upstream
Copy/control and connector gates. T031-T048 and T050-T064 remain pending.
The new components cover manifest-backed source contexts, versioned Silver
rules, object workers and generic Silver DAG/notebooks.
The default installer and business pipelines have not been replaced.

The [batch report](batch-report.yaml),
[implementation checkpoint](checkpoints/tasks-to-impl.yaml) and
[capability evidence](validation/capabilities.json) record the current scope.
Full end-to-end acceptance and cutover have not passed.
Batch B05 distinguishes pure/kernel tests, real local Spark checks and
unverified Fabric execution. Local component implementation does not check
off a task whose required upstream integration or acceptance is incomplete.
The initial SQL Copy/Silver control path is now connected locally. Native
TDS/pipeline acceptance, complete transformation recovery, native file
observation and the later Gold/layout/installer phases remain open. No authorization was provided
for the requested isolated staging checks, so all work remains local.

The existing work was preserved in commit `fff5618` before B05 edits.
Failed/partial Bronze dispatch now reaches reconciliation, records complete
reserved/undispatched membership and retains uncertain activity observations
without retry. SQL file descriptors and immutable marker/stability manifest
transport are connected locally to the generic parents. B05 passed 69 affected
checks, 179 offline tests (25 opt-in skipped), two real local Spark checks and
the 55-batch T-SQL parser gate. A fresh Delta replay remains blocked by missing
approved Windows Hadoop native tools. No native acceptance task is checked off
on the strength of these local results.

Local control-only bootstrap/resume and guided connection rechecks already
cover portions of T049-T052. Full generic inventory binding, activation and
installer acceptance are still pending; do not restart the accepted foundation.

On 2026-11-09 the user chose replacement before deletion of the redundant
source notebooks. Subsequent local work adds the generic Gold parent/worker,
explicit source storage bindings, complete object-result fan-in and Gold
dispatch/callback integration. These are local components of T035 and related
installer/generation work, not completion of their upstream or native gates.
The explicit `--bootstrap-orchestrator --metadata-assets` publisher derives
a 10-item reusable inventory and journals resolved publication bindings
without source reseeding. It leaves the SQL draft unbound/inactive and
does not replace the default legacy deployment or full T050-T054 acceptance.
The legacy artefacts remain until replacement acceptance and retirement
conditions are satisfied. The batch report/checkpoint distinguishes this
replacement-first work from the earlier B05 evidence.

Specification:
[metadatadriven-medallion-requirements.md](requirements-as-approved.md).
Scope: all 60 requirements and all 12 success criteria.

Refactor the existing accelerator in place into one operational entry point,
`pl_bronze_silver_gold`, backed by `db_orchestrator`. Keep ForEach/Copy for
landing, canonical append-only Silver, observed-time SCD2, transaction-grain
facts, position snapshots and version-pinned Gold releases.

Implement generic runtime metadata, connector-specific Copy templates,
Lakehouse-bound notebook DAGs and governed Gold extensions. Do not create a
generic expression language that attempts to infer accounting rules.

Deliver in ten phases with 22 plan items and 64 executable tasks. Local
implementation can proceed through the offline gates; cloud-dependent proof
and cutover remain separately approved activities.

## Technical context

### Existing anchors and preservation requirements

| Existing surface                                                   | Controlling behaviour and planned treatment                                                                                                           |
| ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `deployment\src\fabric_treasury_deployer\contracts.py`             | Imports three registries/schemas into `Source`; retain as the bootstrap and historical-contract importer, not the new runtime authority               |
| `deployment\src\fabric_treasury_deployer\definitions.py`           | Builds explicit SQL projections, embedded notebook contracts and source pipeline arrays; reuse serialisation and replace normal runtime bindings      |
| `artefacts\pipeline\full_snapshot_pipeline.json`                   | Sequential ForEach/Copy and immutable run folders; retain as legacy evidence while introducing reusable metadata-driven definitions                   |
| `artefacts\medallion\treasury_ingestion.py`                        | Typed parsing, hashes, version links, quarantine, sealed batches and source commits; adapt context and task scope without changing business semantics |
| `deployment\src\fabric_treasury_deployer\gold.py`                  | Seeds explicit dimensions, facts, mappings and 11 MLV definitions; export these as the initial approved metadata contract                             |
| `artefacts\gold\treasury_gold.py`                                  | Pins inputs, builds Gold sequentially and records output versions; separate task execution from release coordination                                  |
| `deployment\src\fabric_treasury_deployer\deployer.py` and `cli.py` | Resolve/test connections and publish items; add safe bootstrap/resume and guided prerequisite rechecks                                                |
| `deployment\src\fabric_treasury_deployer\fabric.py`                | Fabric REST, pagination, regional operation normalisation and no automatic mutation retries; reuse these safeguards                                   |
| `deployment\build.py` and `deployment\manifest.json`               | Currently generate 17 notebooks and a 26-item inventory; derive the replacement inventory from reusable assets rather than retain these counts        |
| `deployment\tests`                                                 | Existing `unittest` contract and opt-in Spark/Delta suites; retain their business assertions and add focused metadata/DAG coverage                    |

These are source anchors, not a claim that the new capability exists. Current
live source and Gold validation must be re-established before migration; an
earlier partial deployment is not acceptance evidence for this refactor.

### Implementation choices

* Keep Python 3.11-3.13 for the deployer, the existing standard-library Fabric
  client and Jupyter-based notebook generation. Do not introduce a web service,
  ORM, metadata UI or mandatory new runtime package by default.
* Use Fabric SQL for configuration, activation and run/task control. Store
  secrets only in Fabric-managed connections, with a separate explicitly
  supplied orchestrator SQL connection such as `conn_orchestrator_sql`.
* Use SQL transactions for configuration activation, run preparation and
  lease ownership. Do not claim a transaction spans SQL, OneLake and Delta.
* Read immutable approved metadata through the orchestrator connection.
  Export the pinned run contract into a run-scoped OneLake manifest using a
  metadata Copy step. Notebook workers consume that immutable snapshot by
  run/task reference; they do not fetch OAuth connection credentials or
  depend on the mutable active-version pointer.
* Carry compact task descriptors through Lookup and page/batch them. If
  pagination requires a child ForEach, use one reusable
  `pl_copy_bronze_batch` child rather than nested unsupported loop shapes.
  This still exposes one master pipeline, not one pipeline per feed.
* Persist per-task result envelopes to distinct run/attempt locations.
  Reconcile them into the SQL operational ledger through the orchestrator
  connection. Existing Delta source-commit and Gold-release records remain
  the data-readiness authorities; SQL ledger lag must be recoverable.
* Bind Silver DAG parents/workers to `lh_brz_slv_treasury` and Gold DAG
  parents/workers to `lh_gld_treasury`. Child tasks use explicit persisted
  input/output references, not variables or cached DataFrames belonging to
  another notebook's interpreter.
* Reuse the current business-key and value-hash algorithms. Metadata version,
  activation time and deployment identity are not additional business hash
  inputs. Preserve old contract hashes for historical manifests and commits.
* Use the single `layout_strategy` flag, `partitioned` or `liquid`, with only
  its corresponding column collection. Preserve existing layout until an
  explicit compatible decision/migration is approved; absence of a historical
  layout decision is not a third strategy or permission to create a new target.
* Run customer-specific Gold through approved typed patterns, SQL definitions
  or versioned notebook references registered in metadata. Do not add a
  separate customer master pipeline.

The metadata transport, connector expression properties and runtime
permissions above require the early capability gate. If the supported Fabric
surface cannot implement them, stop that capability and revise the plan;
do not silently revert to embedded arrays or per-source JDBC landing.

### Local baseline recorded during planning

| Check or capability                                                     | Observed result                                                                  |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Canonical offline deployment suite                                      | 51 tests: 34 passed, 17 opt-in skipped, zero failures, exit code 0               |
| `deployment\build.py --check`                                           | Passed for the current 17 generated notebooks and 26-item manifest, exit code 0  |
| Deployment virtual environment                                          | Python 3.11.15                                                                   |
| Local opt-in Spark dependencies declared in `deployment\pyproject.toml` | PySpark 3.5.7 and Delta 3.3.2; not equivalent to cloud Runtime 2.0 feature proof |
| Docker                                                                  | Command exists; daemon did not respond successfully to `docker info`             |
| Node.js                                                                 | Available, v26.13.0; no browser application is in scope                          |
| Workspace                                                               | Existing dirty worktree; preserve all unrelated work and historical artifacts    |

No dependencies were installed, cloud calls made or application sources
modified to prepare this plan.

## Constitution check

No standalone constitution, project topology or knowledge-graph artifact was
found. Apply the repository's active engineering instructions and the
requirements baseline instead; do not fabricate additional migration rules.
There are no topology G-group labels to attach to tasks.

| Principle                    | Plan treatment                                                                                          |
| ---------------------------- | ------------------------------------------------------------------------------------------------------- |
| Data fidelity and history    | Preserve typed source values, immutable batches, hash identity, source commits and Gold output versions |
| Evidence-first delivery      | Freeze parity fixtures first; require narrow checks in each implementation slice                        |
| Least privilege and privacy  | Connection references only; no secrets or business records in metadata/audit summaries                  |
| Safe mutation                | Offline implementation by default; separate approval gates for cloud proof, bootstrap and cutover       |
| Naming and ownership         | Lowercase names, source table names and explicit `persal` to `slv_psl` compatibility                    |
| Failure visibility           | Persist evidence, propagate failures and withhold downstream readiness                                  |
| Small coherent changes       | Add focused shared modules; retain existing tested algorithms rather than rewrite the medallion stack   |
| Supported platform behaviour | Prove connectors, DAG execution, layout and MLV compatibility; fail unsupported settings explicitly     |

## Applied guidelines

The installed migration guideline collection contains Spring Boot scaffolding
and Struts-to-Spring patterns, neither of which applies. No framework migration
guideline is imposed.

Apply the existing Python, Fabric, notebook, SQL/data, testing and Markdown
instructions. Use the platform references already recorded in the
requirements document for connector, DAG, clustering and MLV decisions.
The supplied example notebooks are design provenance only; do not copy their
code, client configuration, mutable hash-matching merge or catch-and-print
failure behaviour.

## Approval gates and execution rules

| Gate | Required approval/evidence                                                                                        | Blocks                                      |
| ---- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| A1   | Explicit approval for bounded capability probes in an identified test workspace, with authorised connections      | Claiming dynamic connector/runtime support  |
| A2   | Explicit approval for staging bootstrap, the new SQL database, controlled file endpoints and end-to-end execution | Cloud deployment and SC-001 to SC-012 proof |
| A3   | Parity evidence, rollback drill, approved target/layout decisions, no active legacy writers and operator signoff  | Cutover and scheduling/activation changes   |
| A4   | Separate reviewed permission, retention, layout-rewrite or decommissioning approval where applicable              | Destructive changes and access assignments  |

A1 was subsequently approved for bounded synthetic probes. A2 was subsequently
approved only for `db_orchestrator` creation, draft metadata and metadata-only
SQL validation. Broader source staging/end-to-end execution, A3 cutover and A4
actions remain unapproved. Legacy retirement is not an automatic consequence
of successful deployment.

The user subsequently selected local-only implementation. Later SQL, pipeline
and connector changes have not been deployed or run in Fabric. The dedicated
`conn_orchestrator_sql` now exists and was inspected read-only; its creation
does not expand staging or cutover approval.

Each task must run its narrowest affected checks and record pass/fail/skip
counts and exit status. A failed check means the task is incomplete.
Mocked/local validation never substitutes for the named live capability gate.

`[Depends:...]` below lists immediate task prerequisites. Source annotations
identify behaviour to preserve, not permission to overwrite user changes.
User-story labels map to the scenarios in the requirements document:
US1 onboarding, US2 transports, US3 Silver/DQ, US4 DAGs, US5 Gold,
US6 layout and US7 recovery.

## Implementation steps

### Phase 1: Setup, fidelity baseline and platform proof

#### Plan item 1.1: Freeze the brownfield contract

Requirements: REQ-012, REQ-019, REQ-054, REQ-060.

Design inputs: source specification, current source registry, ingestion
documentation, Gold model and existing executable tests.

Capture a deterministic fixture of the 44 source contracts, schema/key/type
definitions, existing hash/commit semantics, historical path shape and Gold
object contracts. Compare committed logical state, not raw physical version
counts. Do not include source records, credentials or tenant-specific IDs.

* T001 and T002 are prerequisites for changes to runtime semantics.
* Record current Gold/reference dictionaries as synthetic/provisional, not
  certified production classifications or joins.

Tasks:

- [x] T001 [Plan:1.1] Create `deployment\tests\fixtures\medallion_baseline.json` from current contract/manifests, preserving keys, types, legacy paths and Gold object/grain definitions. [Source: deployment\src\fabric_treasury_deployer\contracts.py#load_sources] [Source: deployment\src\fabric_treasury_deployer\gold.py#gold_config]
- [x] T002 [Plan:1.1] Extend baseline parity assertions in `deployment\tests\test_contracts.py` and `deployment\tests\test_gold_contracts.py`; retain current history, source-boundary and unsupported-measure assertions. [Depends:T001]

#### Plan item 1.2: Prove the platform boundaries before reliance

Requirements: REQ-005, REQ-007, REQ-035, REQ-041, REQ-047, REQ-058.

Design inputs: requirements platform references and current Copy definition.

Prepare opt-in, bounded probes for dynamic connection ID selection between two
authorised Fabric SQL connections, parameterised metadata SQL access,
SQL-to-OneLake metadata snapshot export and exact parameter injection into
worker notebooks. Identify the supported property shapes and authentication
for API-based MLV refresh. Verify supported connector/auth pairs and
parent/child Lakehouse behaviour; no credential extraction.

A passing local serialization test is not a passing connection-expression
test. FTP/SFTP and layout-specific cloud proof have later dedicated tasks.

Tasks:

- [x] T003 [Plan:1.2] Add `deployment\tests\test_orchestration_capabilities.py` with offline payload checks and opt-in live probes, explicit target/connection arguments, deadlines and safe evidence output. [Source: deployment\src\fabric_treasury_deployer\definitions.py#pipeline_definition] [Depends:T002]
- [x] T004 [Plan:1.2] After A1, execute the probes in `deployment\tests\test_orchestration_capabilities.py`; record supported shapes, denied/unsupported cases and unresolved capability blockers in `docs\plans\metadata-driven-medallion\validation\capabilities.json`. [Depends:T003]

Exit gate: the local fixture is stable; unsupported features are not marked
supported. Local foundational work can proceed before A1, but no connector
activation or deployment acceptance may bypass T004.

### Phase 2: Foundational metadata and bootstrap import

#### Plan item 2.1: Add typed metadata and the SQL control schema

Requirements: REQ-001, REQ-002, REQ-003, REQ-004, REQ-005, REQ-008,
REQ-014, REQ-023, REQ-038, REQ-051, REQ-055, REQ-057.

Implement the logical entities already defined by the requirements: version,
connection reference, source/feed, layered object, ordered columns, rules,
constraints, transformations, MLV definitions, layouts and dependency edges.
Include run/task/attempt, result-inbox and lease/fencing control records.

Use explicit keys and foreign-key relationships in Fabric SQL where
supported. Represent multivalued columns/dependencies structurally. Treat
key/relationship declarations for Delta as validation contracts, not assumed
engine-enforced constraints.

Protect approved configurations from in-place edits. Activation is an
authorised SQL operation with recorded actor/time; read/preview operations do
not change active configuration. Define least-privilege roles/procedure
boundaries without automatically granting memberships.

Tasks:

- [x] T005 [Plan:2.1] Add pure typed metadata models and validation in `deployment\src\fabric_treasury_deployer\metadata.py`, including stable identities, structured rules/dependencies, layer/materialisation and the single layout flag. [Source: deployment\src\fabric_treasury_deployer\contracts.py#identifier,_validate_tables] [Depends:T002]
- [x] T006 [Plan:2.1] Add additive versioned DDL in `artefacts\orchestrator\schema.sql` for configuration, operational records and explicit access boundaries; keep business data and credentials out. [Depends:T005]
- [x] T007 [Plan:2.1] Add configuration validation/activation and read-only version/preview procedures in `artefacts\orchestrator\procedures.sql`, with immutable approved versions and auditable activation history. [Depends:T006]

#### Plan item 2.2: Seed existing contracts without creating two authorities

Requirements: REQ-020, REQ-026, REQ-034, REQ-053.

Use the existing registries, schemas, Gold specs, demo mappings and MLV SQL to
compile the first metadata release. Preserve `slv_psl`, synthetic-only flags,
unavailable measures, hash exclusions and existing contract-hash lookup.
Seed all current source-specific business checks as versioned rule handlers;
do not leave undisclosed hardcoded rules alongside configurable ones.

Package the initial seed separately from runtime metadata access. Rebuilding
notebooks must not reset active configuration. Preserve a historical adapter
for old contracts without reading the repository as a new-run fallback.

Tasks:

- [x] T008 [Plan:2.2] Add the bootstrap importer/compiler in `deployment\src\fabric_treasury_deployer\orchestrator.py` for all 44 source tables, Gold specs, mappings, rules and MLV definitions; preserve historical contract identities. [Source: deployment\src\fabric_treasury_deployer\contracts.py#Source.runtime_config,load_sources] [Source: deployment\src\fabric_treasury_deployer\gold.py#gold_config,mlv_definitions] [Depends:T005,T007]
- [x] T009 [Plan:2.1,2.2] Add `deployment\tests\test_metadata.py` and `deployment\tests\test_orchestrator_sql.py` for model/seed parity, immutable activation, identifier safety and SQL definition contracts; reserve live SQL transaction proof for A2. [Depends:T007,T008]

Exit gate: every original contract/rule has an explicit seeded representation;
metadata activation fails on invalid or unsupported definitions.

### Phase 3: Run planning, immutable manifests and execution control

#### Plan item 3.1: Compile and pin complete execution plans

Requirements: REQ-001, REQ-002, REQ-003, REQ-006, REQ-013, REQ-014,
REQ-017, REQ-048.

Compile the selected version's dependency closure, deterministic task IDs,
source/feed authority, target ownership and worker versions. Reject disabled
required dependencies, conflicting authorities and colliding target/path
identities. Keep priority distinct from dependency order.

Export approved metadata at a fixed version through the orchestrator SQL
connection into a canonical immutable run manifest. Validate its object count,
content hash and worker hashes before accepting it. Metadata export is not
business-source landing and does not change the ForEach/Copy requirement.

Read-only preview must expose the same selection/validation logic, using SQL
read procedures and offline fixtures as appropriate. Do not introduce a new
SQL client dependency merely to duplicate this preview.

Tasks:

- [x] T010 [US1] [Plan:3.1] Implement execution-plan compilation and preview in `deployment\src\fabric_treasury_deployer\metadata.py`, including dependency closure, authority, disabled objects, paging and stable IDs. [Depends:T008,T009]
- [x] T011 [US1] [Plan:3.1] Add `artefacts\orchestrator\treasury_orchestration.py` for canonical snapshot read/verification, run-scoped references and manifest resolution; workers must not read mutable active metadata. [Depends:T010]
- [x] T012 [US1] [Plan:3.1] Add `deployment\tests\test_run_plans.py` for concurrent activation, complete metadata paging, path/target collisions, legacy identities and rejection of missing dependencies. [Depends:T010,T011]

#### Plan item 3.2: Make retries, leases and audit reconciliation explicit

Requirements: REQ-018, REQ-025, REQ-046, REQ-049, REQ-050, REQ-055,
REQ-056, REQ-059.

Separate master run ID, source batch ID, task ID, attempt ID and service job
ID. Preserve the existing source batch identity in Silver's legacy
`pipeline_run_id` lineage binding rather than substituting an attempt ID.

Acquire source/target leases with fencing, renew only for the owning run and
check active service jobs before reclaiming stale ownership. Do not expire a
lease and permit another writer while the old job may still be running.

Use unique result locations per task/attempt and idempotent result ingestion
into the operational ledger. Reconcile SQL state with authoritative Delta
commits/releases after interruptions. Do not require per-record indexes or
claim distributed atomicity.

Tasks:

- [x] T013 [US7] [Plan:3.2] Extend `artefacts\orchestrator\procedures.sql` with idempotent run preparation, bounded task pages, source/target leases, fenced attempts and result-inbox reconciliation. [Depends:T007,T012]
- [x] T014 [US7] [Plan:3.2] Implement result envelopes, failure propagation and recovery reconciliation in `artefacts\orchestrator\treasury_orchestration.py`; extend `deployment\src\fabric_treasury_deployer\fabric.py` only for required safe job operations. [Source: deployment\src\fabric_treasury_deployer\fabric.py#request,paged] [Depends:T011,T013]
- [x] T015 [US7] [Plan:3.2] Add `deployment\tests\test_run_lifecycle.py` for duplicate result/commit attempts, uncertain submissions, stale fences, same-target writers and redacted errors. [Depends:T013,T014]

Exit gate: task completion and recovery are deterministic under partial
success; no failure is converted into successful downstream readiness.

### Phase 4: Metadata-driven Bronze through ForEach/Copy

#### Plan item 4.1: Replace source arrays with runtime SQL task selection

Requirements: REQ-009, REQ-010, REQ-012, REQ-013, REQ-044.

Build the master pipeline's prepare, snapshot-export, Lookup and bounded
batch-dispatch stages. Its reusable Copy loop consumes pinned task rows and
switches only among supported connector templates. Do not put source tables
back into deployment-generated `p_table_config` defaults.

Keep identifier-quoted SQL projections and exact type serialization,
including the live-proven BOM and `bit` fixes. Parameterise metadata query
values through supported Script/Lookup/procedure parameters. Never concatenate
an untrusted run ID, predicate or table name into an executable query.

Tasks:

- [ ] T016 [US2] [Plan:4.1] Extend `deployment\src\fabric_treasury_deployer\definitions.py` with metadata-driven master/Copy generators, preserving `copy_projection` semantics and using runtime connection/task references. [Source: deployment\src\fabric_treasury_deployer\definitions.py#copy_projection,pipeline_definition] [Depends:T010,T015]
- [ ] T017 [US2] [Plan:4.1] Add `artefacts\pipeline\bronze_silver_gold_pipeline.json` and reusable `artefacts\pipeline\copy_bronze_batch_pipeline.json` with prepare/export, paged task selection and ForEach/Switch/Copy; use the supported shapes established by T004 before live enablement. [Source: artefacts\pipeline\full_snapshot_pipeline.json] [Depends:T011,T013,T016]
- [ ] T018 [US2] [Plan:4.1] Add `deployment\tests\test_metadata_pipeline.py` proving runtime table/connection references, bounded paging, secure input/output, no automatic Copy retry and no embedded normal-run source arrays. [Depends:T016,T017]

#### Plan item 4.2: Support file-source shape and arrival readiness

Requirements: REQ-009, REQ-011, REQ-016.

Add distinct FTP and SFTP adapters with supported auth/gateway settings, file
selection and transport options. Prefer complete raw file copy for file feeds;
format interpretation and column contracts remain explicit downstream.

Require a configured completion signal or reviewed stable-arrival policy;
file existence alone is not completeness. Capture selected file membership
before processing, retain filenames and reject changing/partial selections.
Disabled or unproved connector capabilities cannot be activated.

Tasks:

- [ ] T019 [US2] [Plan:4.2] Add `artefacts\pipeline\connectors\ftp_copy.json` and `artefacts\pipeline\connectors\sftp_copy.json`, and bind them from the generator without per-source templates. [Depends:T017]
- [ ] T020 [US2] [Plan:4.2] Add transport-aware arrival and multi-file membership validation to `artefacts\orchestrator\treasury_orchestration.py`; use explicit metadata policy rather than implicit filename conventions. [Depends:T011,T019]
- [ ] T021 [US2] [Plan:4.2] Add `deployment\tests\test_file_ingestion.py` and extend capability probes for controlled FTP/SFTP endpoints; cover changing files, incomplete arrivals, empty permitted files and retained raw membership. [Depends:T019,T020]

#### Plan item 4.3: Resolve new and historical Bronze from manifests

Requirements: REQ-012, REQ-015, REQ-016, REQ-017, REQ-018, REQ-019.

Make source/feed/description/type and exact file paths contract inputs.
Preserve the new naming convention without renaming source columns, the
`persal` logical source or historical folders. Keep strict headers, safe path
scope and logical multiset sealing across the configured file membership.

Add a legacy-contract adapter for old source/date/run/table paths. Historical
replay must use the original contract, observation timestamp and sealed hash;
the new active metadata version must not relabel old batches.

Tasks:

- [ ] T022 [US2] [Plan:4.3] Adapt `artefacts\medallion\treasury_ingestion.py` context/path/raw-read/inventory entry points to validated manifest inputs and multi-file objects; retain explicit legacy path resolution. [Source: artefacts\medallion\treasury_ingestion.py#processing_context,source_path,read_raw,inventory] [Depends:T011,T018]
- [ ] T023 [US2] [Plan:4.3] Extend `deployment\tests\test_runtime_controls.py` and `deployment\tests\test_spark_runtime.py` for new/old paths, tampering, original timestamps, BOMs, strict headers and exact multi-file parsing. [Depends:T021,T022]

Exit gate: metadata-selected SQL and file tasks compile without source-array
fallback; historical replay remains compatible. Real connector proof still
requires the designated cloud gates.

### Phase 5: Generic Silver and dependency-aware notebook execution

#### Plan item 5.1: Drive existing Silver algorithms through metadata

Requirements: REQ-020, REQ-021, REQ-022, REQ-023, REQ-024, REQ-026,
REQ-056.

Keep the existing typed parse, deterministic SHA-256, latest-committed
comparison and insert-only merge. Refactor rule selection, source-specific
checks and object scope, not the business-history algorithm.

Use named, versioned handlers for complex existing source rules and validated
typed rule definitions for generic checks. Preserve required-rule severity,
source-isolated quarantine and raw rejected values. Do not evaluate arbitrary
Python from metadata or silently expand schema.

A new metadata version is not a new business event. Keep existing Silver
schema and lineage types compatible; attach additional configuration lineage
at batch/task level unless a separate additive schema migration is approved.

Tasks:

- [ ] T024 [US3] [Plan:5.1] Extract existing specialist checks into `artefacts\medallion\treasury_rules.py` and add metadata rule dispatch to `treasury_ingestion.py`; preserve all existing BAS/CSD/PERSAL checks and failure actions. [Source: artefacts\medallion\treasury_ingestion.py#prepare_rows,add_foreign_key_rules,source_business_checks] [Depends:T008,T023]
- [ ] T025 [US3] [Plan:5.1] Add object-scoped processing to `artefacts\medallion\treasury_ingestion.py` while retaining hash/version/commit-filter semantics and explicit `slv_psl` binding. [Source: artefacts\medallion\treasury_ingestion.py#process_table,append_silver,eligible_silver,latest_silver] [Depends:T022,T024]
- [ ] T026 [US3] [Plan:5.1] Add `deployment\tests\test_metadata_rules.py` and extend `test_append_only_runtime.py` for rule parity, unchanged polls, A-to-B-to-A, failed equal-hash batches, quarantine and schema-change rejection. [Depends:T024,T025]

#### Plan item 5.2: Generate and execute the Silver DAG safely

Requirements: REQ-004, REQ-025, REQ-031, REQ-045, REQ-046, REQ-047,
REQ-049, REQ-058.

Generate inventory/load/validation/retention tasks from dependency metadata.
Initialise audit and target namespaces before parallel writers. Foreign-key
checks must read the configured sealed parent population, not accidentally
depend on whichever table finishes first.

Serialize same-target work; permit independent objects within a conservative
configured concurrency. Publish one idempotent source commit only after
source-wide business validation. Keep no-deletion retention.

Tasks:

- [ ] T027 [US4] [Plan:5.2] Implement Silver DAG construction, unique activity IDs, dependency validation and concurrency/timeout controls in `artefacts\orchestrator\treasury_orchestration.py`. [Depends:T014,T026]
- [ ] T028 [US4] [Plan:5.2] Generate `artefacts\orchestrator\notebooks\nb_prepare_medallion_run.ipynb`, `nb_orchestrate_silver.ipynb` and `nb_silver_worker.ipynb` through `orchestrator.py` and `deployment\build.py`, embedding shared code but no active source contracts. [Source: deployment\src\fabric_treasury_deployer\definitions.py#notebook_document,_code_cell] [Depends:T027]
- [ ] T029 [US3] [Plan:5.2] Adapt `artefacts\medallion\treasury_ingestion.py` source validation/retention for task-result fan-in and idempotent commits; preserve failure exclusion and original snapshot evidence. [Source: artefacts\medallion\treasury_ingestion.py#validate_source,cleanup_snapshots] [Depends:T025,T027]
- [ ] T030 [US4] [Plan:5.2] Add `deployment\tests\test_notebook_dag.py` covering missing/cyclic dependencies, two overlapping independent tasks, same-target serialization, shared initialisation, failed descendants and binding compatibility. [Depends:T028,T029]

Exit gate: parallelism changes scheduling only; committed Silver business
state, hashes, quality outcomes and retained history match the baseline.

### Phase 6: Governed Gold rules and Gold DAG

#### Plan item 6.1: Load explicit Gold contracts from the pinned version

Requirements: REQ-027, REQ-028, REQ-029, REQ-030, REQ-033, REQ-060.

Move current dimensions, facts, mappings, correction/snapshot policies and
blocked measures into the initial metadata version. Bootstrap may use the
existing synthetic assets; normal Gold runs may not re-read local seed CSVs.

Preserve observed-time SCD2, exact fact grains, unknown members, legitimate
unattributed data, shared CSD supplier semantics and the synthetic-only gate.
Do not invent department ownership or complete budgets/forecasts.

Tasks:

- [ ] T031 [US5] [Plan:6.1] Separate bootstrap Gold metadata export from normal runtime configuration in `deployment\src\fabric_treasury_deployer\gold.py`; seed existing explicit grains, joins, mappings and unavailable measures. [Source: deployment\src\fabric_treasury_deployer\gold.py#gold_config,dimension_specs,mlv_definitions] [Depends:T008,T030]
- [ ] T032 [US5] [Plan:6.1] Adapt `artefacts\gold\treasury_gold.py` to pinned source/object manifests and rule versions without changing SCD2/fact semantics or historical path interpretation. [Source: artefacts\gold\treasury_gold.py#gold_request,_gold_inputs,build_scd2,build_fact] [Depends:T022,T031]
- [ ] T033 [US5] [Plan:6.1] Extend `deployment\tests\test_gold_contracts.py` and `test_gold_runtime.py` to compare seeded metadata with existing Gold grains, mappings, temporal keys and monetary outputs. [Depends:T031,T032]

#### Plan item 6.2: Split Gold work without losing release consistency

Requirements: REQ-029, REQ-030, REQ-031, REQ-032, REQ-036, REQ-045,
REQ-047, REQ-049.

Separate release preparation, dimension tasks, dependent fact tasks and
release finalisation. Persist references to pinned Silver versions,
sealed full-extract membership and dimension task outputs.

Do not share in-memory frame dictionaries between child notebooks. Load
required dimension outputs by recorded table version; preserve correction
upserts and snapshot membership semantics.

Hold compatible target/release ownership through final validation and required
MLV refresh. A version change during the aggregate-serving window invalidates
readiness. Do not claim all Gold tables commit atomically.

Tasks:

- [ ] T034 [US5] [Plan:6.2] Refactor `artefacts\gold\treasury_gold.py` into preparation, object-build and finalisation functions while retaining a compatibility wrapper and output-version manifest semantics. [Source: artefacts\gold\treasury_gold.py#run_gold,_gold_inputs,_merge_gold,_release_event] [Depends:T014,T033]
- [ ] T035 [US4] [Plan:6.2] Add Gold DAG support and generated `artefacts\orchestrator\notebooks\nb_orchestrate_gold.ipynb` and `nb_gold_worker.ipynb`, with the Gold Lakehouse binding and explicit dimension-to-fact dependencies. [Source: deployment\src\fabric_treasury_deployer\gold.py#gold_notebook_definition] [Depends:T028,T034]
- [ ] T036 [US5] [Plan:6.2] Add `deployment\tests\test_gold_dag.py` for input/dimension version pinning, unknown members, failed/partial releases, corrected events and snapshot reconciliation after retry. [Depends:T034,T035]

#### Plan item 6.3: Register specialist transformations without a new pipeline

Requirements: REQ-027, REQ-028, REQ-048, REQ-057.

Allow approved typed patterns, Spark SQL query definitions and registered
notebook references with declared inputs/outputs. Validate approval identity,
code/query version and target permissions before dispatch.

Do not use string `eval`/`exec` to execute metadata expressions. Definition
approval and engine-side validation are required for executable SQL; regular
expressions are not a complete SQL safety or semantic validator.

Tasks:

- [ ] T037 [US5] [Plan:6.3] Add transformation registration/dispatch to `deployment\src\fabric_treasury_deployer\metadata.py` and `artefacts\orchestrator\treasury_orchestration.py`, with approved implementation hashes and explicit contracts. [Depends:T010,T035]
- [ ] T038 [US5] [Plan:6.3] Add `deployment\tests\test_transformation_registry.py` for generic and specialist transformations, changed code/query versions, unsupported algorithms, undeclared outputs and withheld readiness on failure. [Depends:T036,T037]

Exit gate: reusable and specialist Gold tasks produce equivalent
source-backed business outputs and retain all production/publication gates.

### Phase 7: MLV lifecycle, refresh and constraint evidence

#### Plan item 7.1: Separate definition changes from routine refresh

Requirements: REQ-032, REQ-034, REQ-035, REQ-036, REQ-037, REQ-050,
REQ-058.

Version approved MLV SQL and only apply definition changes when the approved
definition differs. Do not run `CREATE OR REPLACE` for every data run.

Use supported public lineage/refresh controls with recorded job identity,
deadlines and readiness readback. Retain the serving target lock/version
check through reconciliation. An overlapping schedule or another Gold writer
must block or invalidate the conflicting operation, not publish stale totals.

Check source CDF and actual refresh mode. A supported full-refresh fallback
is valid if it reconciles; record the mode/capacity cost rather than promise
incremental refresh. Scheduled refresh must not rely on notebook-session-only
Spark settings.

Tasks:

- [ ] T039 [US5] [Plan:7.1] Refactor MLV definition registration in `gold.py` and `treasury_gold.py`, and adapt `artefacts\gold\nb_treasury_gold_mlv_definitions.ipynb` generation to approved definition hashes and explicit input contracts. [Source: deployment\src\fabric_treasury_deployer\gold.py#mlv_definitions] [Source: artefacts\gold\treasury_gold.py#register_gold_mlvs] [Depends:T031,T038]
- [ ] T040 [US7] [Plan:7.1] Add the supported MLV refresh/job controls to `deployment\src\fabric_treasury_deployer\fabric.py` and the shared orchestrator runtime, using the authenticated surface proved at A1 and retaining no-blind-retry semantics. [Depends:T014,T039]
- [ ] T041 [US5] [Plan:7.1] Add `deployment\tests\test_mlv_lifecycle.py` for unchanged definitions, version changes during refresh, schedule conflicts, job recovery, full-refresh visibility and exact aggregate reconciliation. [Depends:T039,T040]

#### Plan item 7.2: Enforce constraints without losing rejection evidence

Requirements: REQ-024, REQ-038, REQ-056.

Keep row checks, uniqueness, referential checks and aggregate reconciliation
separate. Record the enforcement engine and violation policy.

Do not enable `ON MISMATCH DROP` for financial serving data unless the
approved policy and retained rejection evidence make its exclusions
reconcilable. The default technical serving gate must fail unsupported
constraints or unexplained differences.

Tasks:

- [ ] T042 [US5] [Plan:7.2] Add declared Gold/MLV constraint evaluation and safe reconciliation evidence to `artefacts\gold\treasury_gold.py`, reusing the metadata rule model rather than a second rule language. [Source: artefacts\gold\treasury_gold.py#_assert_unique,register_gold_mlvs] [Depends:T024,T039]
- [ ] T043 [US5] [Plan:7.2] Extend `deployment\tests\test_gold_runtime.py` and `test_mlv_lifecycle.py` for logical versus engine constraints, approved exclusions, failed reconciliation and no false business approval. [Depends:T041,T042]

Exit gate: aggregates have verified input membership/output totals and
constraint evidence; no definition/refresh failure yields aggregate readiness.

### Phase 8: Physical layout through one flag

#### Plan item 8.1: Apply selected layout to supported materialisations

Requirements: REQ-039, REQ-040, REQ-041, REQ-042.

Use `layout_strategy` as the sole selector. `partitioned` requires ordered
partition columns; `liquid` requires clustering columns. Reject inactive
settings, incompatible target types and unsupported runtimes.

Raw files have no Delta layout. Do not assume Delta table clustering syntax
is MLV syntax. Historical layouts can be inspected and preserved without
inventing a new selector value; creating or migrating a target requires a
complete explicit decision.

Tasks:

- [ ] T044 [US6] [Plan:8.1] Add `artefacts\orchestrator\treasury_layout.py` for layout validation, supported table creation and maintenance planning, with one selector and no liquid/Z-Order combination. [Depends:T005,T043]
- [ ] T045 [US6] [Plan:8.1] Integrate layout planning with Silver/Gold target creation in `treasury_ingestion.py` and `treasury_gold.py`; inspect existing layouts and fail on unapproved changes rather than overwrite tables. [Source: artefacts\medallion\treasury_ingestion.py#append_silver] [Source: artefacts\gold\treasury_gold.py#_merge_gold] [Depends:T025,T034,T044]
- [ ] T046 [US6] [Plan:8.1] Add `deployment\tests\test_layout_contracts.py` and opt-in `test_layout_runtime.py` for both selections, invalid columns/settings, preserved legacy layouts and identical logical data. [Depends:T044,T045]

#### Plan item 8.2: Make layout migration and maintenance separate operations

Requirements: REQ-042, REQ-043, REQ-059.

Provide a read-only migration/maintenance preview with source/target versions,
expected rewrites, cost/capacity estimate and downstream compatibility checks.
Do not infer rewrite approval from a metadata flag change.

Apply any approved physical migration under writer ownership, and compare
full logical contents before accepting the result. Preserve replay/time-travel
requirements; no automatic vacuum, truncation or irreversible rollback.

Tasks:

- [ ] T047 [US6] [Plan:8.2] Add preview and explicitly gated maintenance/migration operations to `artefacts\orchestrator\treasury_layout.py`, with version/lease checks and separate configuration versus physical optimisation actions. [Depends:T014,T045,T046]
- [ ] T048 [US6] [Plan:8.2] Extend `deployment\tests\test_layout_runtime.py` for rejected unapproved rewrites, interrupted migration, content equivalence and preserved historical references; reserve Fabric consumer proof for A2/A4. [Depends:T046,T047]

Exit gate: the flag controls the selected strategy, while historical data and
layout are unchanged unless a separate approved operation proves equivalence.

### Phase 9: Installer, generation and operational documentation

#### Plan item 9.1: Bootstrap the control plane and guide connection setup

Requirements: REQ-007, REQ-008, REQ-050, REQ-051, REQ-052, REQ-057.

Build the orchestration SQL project independently of the source simulators.
Resolve/test/bind the orchestrator and feed connections; do not programmatically
consent OAuth or use browser automation.

Interactive setup lists every missing/expired connection and supports recheck
after user action. Noninteractive and dry-run modes never prompt. Record
accepted cloud operations before monitoring and reconcile partial state on
resume.

Existing source database seeds must not be reapplied merely to install the
orchestrator. Only publish supported compatible definitions, and activate
configuration after schema, bindings and worker versions are ready.

Tasks:

- [x] T049 [Plan:9.1] Add the orchestrator SQL project/seed definition compiler to `deployment\src\fabric_treasury_deployer\orchestrator.py` and `definitions.py`, separating additive control-plane deployment from source simulator reseeding. [Source: deployment\src\fabric_treasury_deployer\definitions.py#sql_database_definition] [Depends:T007,T008,T013]
- [ ] T050 [Plan:9.1] Refactor `deployment\src\fabric_treasury_deployer\deployer.py` for staged bootstrap/resume, safe metadata activation, connection capability validation and persistent operation reconciliation. [Source: deployment\src\fabric_treasury_deployer\deployer.py#AcceleratorDeployer.deploy] [Depends:T015,T018,T021,T030,T038,T043,T048,T049]
- [ ] T051 [Plan:9.1] Update `deployment\src\fabric_treasury_deployer\cli.py` and `deployment\deploy.ps1` with orchestrator binding, explicit version/activation and resume controls, guided TTY rechecks and noninteractive failure behaviour. [Source: deployment\src\fabric_treasury_deployer\cli.py#create_parser,main] [Depends:T050]
- [ ] T052 [Plan:9.1] Extend `deployment\tests\test_deployer.py` and add `test_cli.py` for fresh/partial bootstrap, declined consent, expired connections, safe overwrite, activation/rollback permissions and noninteractive/dry-run no-mutation behaviour. [Depends:T050,T051]

#### Plan item 9.2: Generate reusable assets and retire runtime duplication

Requirements: REQ-001, REQ-044, REQ-048, REQ-053.

Generate shared runtime/validator cells and generic worker notebooks. Keep
active table/column/rule values out of the notebook definitions. Build only
the initial metadata seed and controlled reusable assets from repository
contracts.

Derive inventory from declared reusable assets and optional connector helpers,
not the old source count. Keep legacy definitions only for reviewed rollback
and historical replay; they must not be a competing normal-run entry point.

Tasks:

- [ ] T053 [Plan:9.2] Refactor `deployment\build.py`, the notebook generators and `deployment\manifest.json` for reusable runtime-only notebooks, worker code hashes and derived inventory counts. [Source: deployment\build.py#main] [Source: deployment\src\fabric_treasury_deployer\definitions.py#notebook_document,notebook_definition] [Depends:T028,T035,T039,T045,T052]
- [ ] T054 [Plan:9.2] Update `deployment\tests\test_contracts.py` and `test_definitions.py` to reject stale generic assets, embedded active contracts and unexplained item counts while retaining legacy compatibility assertions. [Depends:T053]
- [ ] T055 [Plan:9.2] Update `README.md`, `deployment\README.md`, `artefacts\pipeline\README.md`, `artefacts\medallion\README.md` and `artefacts\gold\README.md` with metadata activation, preview, replay, layout and connection setup procedures. [Depends:T054]

Exit gate: local generation/validation is reproducible and installation does
not silently reset configuration, sources, historical data or permissions.

### Phase 10: End-to-end evidence, rollback and controlled cutover

#### Plan item 10.1: Execute acceptance in an approved isolated target

Requirements: REQ-022, REQ-037, REQ-054, REQ-058.

After A2, resolve the exact tenant/workspace/capacity and isolate the test
targets from existing writers. Stage the new database and reusable assets
without deleting old ones. Re-establish the complete current baseline before
using it as a migration comparator.

Exercise all SC-001 to SC-012 journeys, including actual metadata changes
without redeployment, real connector execution, rejected authority/auth,
failed batch recovery, DAG concurrency, Gold correction and MLV reconciliation.
Record actual refresh mode, durations, capacity pressure and maintenance cost.

Do not report an SQL simulator as file-connector proof or local Delta as
Fabric SQL/MLV/Direct Lake interoperability proof.

Tasks:

- [ ] T056 [Plan:10.1] Add `deployment\tests\test_orchestration_live.py` as the opt-in end-to-end driver with explicit target/fixture/evidence arguments, job journaling, readback and safe failure cleanup policy. [Depends:T004,T052,T054]
- [ ] T057 [US1] [Plan:10.1] After A2, use `deployment\tests\test_orchestration_live.py` to stage bootstrap and prove metadata-only object/column changes, snapshot isolation, complete paging and denied connection/activation cases. [Depends:T056]
- [ ] T058 [US3] [Plan:10.1] Use `deployment\tests\test_orchestration_live.py` to prove all-source committed Silver, A-to-B-to-A/replay/partial failure, DAG ordering and Gold correction/release/MLV failure behaviour. [Depends:T030,T036,T043,T057]
- [ ] T059 [US2] [Plan:10.1] Use `deployment\tests\test_orchestration_capabilities.py` and `test_file_ingestion.py` against approved FTP/SFTP fixtures; verify raw membership, partial arrivals and connector-specific authentication. [Depends:T004,T021,T057]
- [ ] T060 [US6] [Plan:10.1] Run `deployment\tests\test_layout_runtime.py` against approved scratch targets on the supported Fabric runtime and verify downstream SQL readability plus actual MLV refresh-mode/constraint behaviour. [Depends:T048,T057]
- [ ] T061 [Plan:10.1] Produce `docs\plans\metadata-driven-medallion\validation\acceptance.md` from executable evidence: 44-contract parity, all declared Gold outputs, every SC outcome, exit codes, gaps and measured capacity/cost; do not mark skipped live cases passed. [Depends:T058,T059,T060]

#### Plan item 10.2: Rehearse rollback and cut over only after approval

Requirements: REQ-006, REQ-033, REQ-043, REQ-054, REQ-059, REQ-060.

Distinguish metadata activation rollback, worker rollback, serving release
selection and physical layout recovery. Configuration rollback cannot undo
data writes or imply a physical table rewind.

Prove rollback in the isolated target before cutover. At A3, inspect legacy
jobs/schedules, withhold activation while any old writer can overlap, then
activate the approved framework version. Do not delete old objects or grant
new permissions as part of normal activation.

Tasks:

- [ ] T062 [US7] [Plan:10.2] Rehearse version/worker/serving-selection rollback using the controls in `deployment\src\fabric_treasury_deployer\cli.py`; document preserved data/history and physical-layout exclusions in `docs\plans\metadata-driven-medallion\validation\rollback.md`. [Depends:T052,T055,T061]
- [ ] T063 [Plan:10.2] After A3, execute the reviewed cutover through `deployment\src\fabric_treasury_deployer\cli.py`, proving no legacy or same-target writer overlaps and recording target/version/schedule decisions in `docs\plans\metadata-driven-medallion\validation\cutover.md`. [Depends:T061,T062]
- [ ] T064 [Plan:10.2] Finalise the operator procedures in `deployment\README.md` and the cutover evidence, retaining synthetic/publication limits and listing any decommissioning, permission or physical-rewrite action as a separate A4 approval. [Depends:T063]

Exit gate: every required acceptance outcome has evidence or an explicit
blocking status; rollback is rehearsed; no destructive or permission action
has been inferred from cutover.

## Phase dependencies and stopping conditions

| Phase | Prerequisites and stopping condition                                                                                           |
| ----- | ------------------------------------------------------------------------------------------------------------------------------ |
| 1     | Read existing contracts; no runtime changes before the parity fixture. T004 requires A1.                                       |
| 2     | T002; may be implemented locally while A1 is pending. No schema deployment is implied.                                         |
| 3     | Typed metadata and seed validation; stop on ambiguous authority, dependency closure or run identity.                           |
| 4     | Run-plan/control contracts; live connector enablement additionally requires T004 and relevant endpoint approval.               |
| 5     | Manifest-backed landing and rule parity; stop if append-only, commit or hash behaviour diverges.                               |
| 6     | Committed Silver/task contracts; stop on changed grain, unsupported publication assumptions or unpinned dimensions.            |
| 7     | Gold release and transformation contracts; stop if refresh cannot be authenticated, locked, reconciled or monitored safely.    |
| 8     | Known target contracts; any incompatible physical rewrite remains blocked pending A4.                                          |
| 9     | Local integration and validation across earlier phases; no mutation without the applicable A2 approval.                        |
| 10    | T004 and complete local gates; A2 for execution, rollback evidence before A3, A4 for separately reviewed irreversible actions. |

## Project structure

Paths below are proposed implementation outputs unless already present.
The planning deliverables are only this file and its two traceability checkpoints.

```text
artefacts\
  orchestrator\
    schema.sql
    procedures.sql
    treasury_orchestration.py
    treasury_layout.py
    notebooks\
      nb_prepare_medallion_run.ipynb
      nb_orchestrate_silver.ipynb
      nb_silver_worker.ipynb
      nb_orchestrate_gold.ipynb
      nb_gold_worker.ipynb
  pipeline\
    bronze_silver_gold_pipeline.json
    copy_bronze_batch_pipeline.json
    connectors\
      ftp_copy.json
      sftp_copy.json
  medallion\
    treasury_ingestion.py
    treasury_rules.py
  gold\
    treasury_gold.py
    nb_treasury_gold_mlv_definitions.ipynb
deployment\
  build.py
  manifest.json
  src\fabric_treasury_deployer\
    metadata.py
    orchestrator.py
    contracts.py
    definitions.py
    deployer.py
    cli.py
    fabric.py
    gold.py
  tests\
    fixtures\medallion_baseline.json
    test_metadata.py
    test_orchestrator_sql.py
    test_run_plans.py
    test_run_lifecycle.py
    test_metadata_pipeline.py
    test_file_ingestion.py
    test_metadata_rules.py
    test_notebook_dag.py
    test_gold_dag.py
    test_transformation_registry.py
    test_mlv_lifecycle.py
    test_layout_contracts.py
    test_layout_runtime.py
    test_cli.py
    test_orchestration_capabilities.py
    test_orchestration_live.py
    existing regression suites
docs\plans\metadata-driven-medallion\
  plan.md
  checkpoints\
    spec-to-plan.yaml
    plan-to-tasks.yaml
  validation\
    capabilities.json
    acceptance.md
    rollback.md
    cutover.md
```

Do not create all proposed files as empty scaffolding. Add each only when its
implementing task supplies behaviour and focused verification. Generated
notebooks remain generated outputs, not hand-maintained copies.

## Testing strategy

### Workload and critical journeys

App type: batch data-platform/CLI workload, not an HTML application.
No browser E2E tier is required.

Critical journeys are runtime metadata onboarding/version pinning, multi-
connection and file landing, append-only change/DQ/commit recovery,
dependency-aware parallelism, Gold temporal/correction/release fidelity,
MLV readiness, both layout selections and safe bootstrap/rollback.

### Primary validation stack and fallbacks

| Capability                  | Primary evidence                                                                   | Fallback and explicit coverage loss                                                                                 |
| --------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Pure metadata/compiler      | Existing `unittest` discovery, deterministic fixtures and fake Fabric clients      | No downgrade needed; no credentials or network required                                                             |
| SQL control-plane semantics | Actual Fabric SQL schema/procedures, activation, locking and access cases after A2 | Local SQL Server may supplement if Docker becomes available; mocks do not prove Fabric SQL transactions/permissions |
| Spark/Delta business rules  | Existing opt-in local Spark/Delta tests plus added object/DAG/replay fixtures      | Pure tests when JVM/native prerequisites fail; history/data execution remains unverified                            |
| Notebook orchestration      | Actual `runMultiple` in approved Fabric sessions, including binding and overlap    | Fake runner proves DAG structure only, not service parameter injection or resource behaviour                        |
| ForEach/Copy and auth       | Approved live SQL and controlled FTP/SFTP endpoints                                | Payload/mock tests only; connector support remains blocked, not passed                                              |
| MLV/layout interoperability | Actual supported Fabric runtime, SQL readback, MLV lineage/jobs and reconciliation | Local/mocked DDL checks only; Fabric feature and consumer compatibility remain unverified                           |
| Browser                     | Not applicable: no rendered UI or OAuth automation is in scope                     | Node availability does not change the data-platform validation plan                                                 |

The unavailable Docker daemon does not justify replacing Fabric proof with
SQLite, dropping connector tests or introducing browser automation. No
containers, services or new dependencies are started during planning.

### Test infrastructure and execution rule

Reuse `unittest`, existing temporary-data fixtures, fake clients and opt-in
Spark setup. New tests must be reachable through the canonical discovery
command. Cloud suites must skip by default with a clear missing-approval/
environment reason and require explicit target/connection/evidence arguments.

Local test dependencies remain in `deployment\pyproject.toml`; add a package
only after proving an unmet requirement and reviewing the manifest change.
Do not introduce a second test runner or new authentication mechanism merely
for convenience.

Implementers must run a narrow selector immediately after each slice and
rerun it after repairs. Run affected suites, then the offline regression and
asset checks at integration gates. Capture actual return code and counts;
skipped opt-in tests never count as live proof.

Canonical offline commands from the repository root:

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'deployment\src'
.\deployment\.venv\Scripts\python.exe -B -m unittest discover `
  -s .\deployment\tests -p "test_*.py" -q
.\deployment\.venv\Scripts\python.exe .\deployment\build.py --check
```

Use existing documented JVM/native setup for opt-in local Spark suites;
do not assume that installing Python packages proves the native runtime.
Real SQL/MLV/layout evidence requires the supported Fabric runtime and
the correct workload API, not an approximate local engine.

### Legacy test assets

Source: discovered in `deployment\tests`.
Migration decision: reuse with focused edits, not replacement.

| Existing suite                       | Preserve semantic intent                                                                   |
| ------------------------------------ | ------------------------------------------------------------------------------------------ |
| `test_contracts.py`                  | Source inventory, keys, reserved names and schema fidelity                                 |
| `test_definitions.py`                | Lossless SQL projections, secure Copy configuration, bindings and self-contained notebooks |
| `test_fabric.py`, `test_deployer.py` | Safe retry/LRO behaviour, no-mutation preflight and item-conflict handling                 |
| `test_runtime_controls.py`           | Scope, date, run identity and Lakehouse/path guards                                        |
| `test_spark_runtime.py`              | Exact parsing, malformed data, BOM, monetary precision and source rules                    |
| `test_append_only_runtime.py`        | Replay, reversion, failed-batch exclusion, version links and retention                     |
| `test_gold_contracts.py`             | Atomic/snapshot grains, blocked measures and aggregate contracts                           |
| `test_gold_runtime.py`               | SCD2, correction upserts, explicit source selections and release/snapshot consistency      |

The supplied notebook examples are not automated regression tests and must
not substitute for the suites above.

### Acceptance and evidence

Execute all 12 SC outcomes from the requirements document. T061 must reference
the exact command/job, target, metadata/worker version, source selection,
output versions, pass/fail/skip counts, exit code and remaining gaps.

Isolate cloud fixtures by approved target/run identifiers. Keep historical
and failed evidence; clean only explicitly identified disposable fixtures
when authorised. Never broad-delete the workspace or use source simulator
reseeding to hide a processing defect.

Measure F8 concurrency rather than assert a speedup. Begin cloud notebook and
Copy concurrency conservatively, increase only with evidence, and retain
no-overlap enforcement for the same source/target.

## Requirement mapping

Implementation evidence below names planned files/checks. It does not claim
these files or behaviours have already been implemented.

| REQ ID  | Description                         | Plan items          | Planned implementation evidence                                                         |
| ------- | ----------------------------------- | ------------------- | --------------------------------------------------------------------------------------- |
| REQ-001 | Runtime metadata authority          | 2.1, 3.1, 9.2       | `metadata.py`, SQL schema, run manifests and generic build contracts                    |
| REQ-002 | Immutable versions and run pinning  | 2.1, 3.1            | Activation procedures and `test_run_plans.py`                                           |
| REQ-003 | Layered objects and relationships   | 2.1, 3.1            | Typed object model and dependency-closure validation                                    |
| REQ-004 | Separate ordering semantics         | 2.1, 5.2            | Column ordinals, priorities and validated DAG edges                                     |
| REQ-005 | Activation validation               | 1.2, 2.1            | Capability evidence, metadata and SQL activation checks                                 |
| REQ-006 | Preview and future-run rollback     | 3.1, 10.2           | Read-only preview and rollback drill                                                    |
| REQ-007 | Connection/auth/gateway validation  | 1.2, 9.1            | Capability probes, `fabric.py`, guided installer checks                                 |
| REQ-008 | Credential exclusion                | 2.1, 9.1            | Reference-only schema and redacted installer/audit tests                                |
| REQ-009 | Connector-specific ForEach/Copy     | 4.1, 4.2            | Master/batch templates and FTP/SFTP adapters                                            |
| REQ-010 | Safe SQL extraction projections     | 4.1                 | `copy_projection` reuse and parameterised metadata selection                            |
| REQ-011 | File shape and completion           | 4.2                 | File adapter/arrival tests and real endpoint evidence                                   |
| REQ-012 | Exact source representations        | 1.1, 4.1, 4.3       | Baseline, projection and Spark CSV/type regression                                      |
| REQ-013 | Disabled dependencies and paging    | 3.1, 4.1            | Closure/page tests and bounded batch dispatch                                           |
| REQ-014 | Source authority and overlap        | 2.1, 3.1            | Source-instance/feed model and authority collision checks                               |
| REQ-015 | New Bronze naming                   | 4.3                 | Manifest path resolver and namespace guards                                             |
| REQ-016 | Exact file membership               | 4.2, 4.3            | File membership manifests and multi-file parsing                                        |
| REQ-017 | Immutable batch lineage             | 3.1, 4.3            | Run snapshots, contract hashes and timestamp/path tests                                 |
| REQ-018 | Retention and replay separation     | 3.2, 4.3            | Attempt lifecycle and retained-batch replay                                             |
| REQ-019 | Historical path compatibility       | 1.1, 4.3            | Legacy fixture and historical manifest adapter                                          |
| REQ-020 | Silver namespace compatibility      | 2.2, 5.1            | Seeded `slv_bas`/`slv_csd`/`slv_psl` contracts                                          |
| REQ-021 | One append-only Silver store        | 5.1                 | Existing insert-only merge and append-only regression                                   |
| REQ-022 | Hash/version change semantics       | 5.1, 10.1           | A-to-B-to-A/replay tests and live committed-state parity                                |
| REQ-023 | Configured DQ coverage              | 2.1, 5.1            | Rule model, seeded handlers and rule regression tests                                   |
| REQ-024 | Quarantine and no silent loss       | 5.1, 7.2            | Rule actions, raw rejection retention and constraint exclusion evidence                 |
| REQ-025 | Source-wide commit gating           | 3.2, 5.2            | Idempotent source validation and failed-attempt exclusion                               |
| REQ-026 | No inferred deletes or false CDC    | 2.2, 5.1            | Seed policy and polling/deletion regressions                                            |
| REQ-027 | Reusable and specialist Gold        | 6.1, 6.3            | Gold rule import and registered transformation tests                                    |
| REQ-028 | Explicit Gold business contracts    | 6.1, 6.3            | Grain/join/measure contracts and declared transform inputs/outputs                      |
| REQ-029 | Configured observed-time SCD2       | 6.1, 6.2            | Existing SCD2 logic and temporal DAG regressions                                        |
| REQ-030 | Atomic facts and corrections        | 6.1, 6.2            | Fact/snapshot contracts and no-double-count correction tests                            |
| REQ-031 | Dimension and source readiness      | 5.2, 6.2            | Source commit fan-in, dimension dependency/version tests                                |
| REQ-032 | Version-pinned Gold releases        | 6.2, 7.1            | Release manifests and serving-window version checks                                     |
| REQ-033 | Unsupported measures remain gated   | 6.1, 10.2           | Blocked-measure fixtures and unchanged publication boundary                             |
| REQ-034 | Versioned MLV query contracts       | 2.2, 7.1            | Seeded SQL definitions and change-aware MLV registration                                |
| REQ-035 | Controlled MLV definition lifecycle | 1.2, 7.1            | Schema/capability proof and definition hash checks                                      |
| REQ-036 | Managed refresh and reconciliation  | 6.2, 7.1            | Gold ownership, refresh jobs and aggregate readiness checks                             |
| REQ-037 | Honest incremental/full behaviour   | 7.1, 10.1           | CDF/refresh-mode evidence and capacity measurements                                     |
| REQ-038 | Explicit constraint enforcement     | 2.1, 7.2            | Constraint model, engine attribution and rejection/reconciliation tests                 |
| REQ-039 | One layout selector                 | 8.1                 | `treasury_layout.py` and single-flag model tests                                        |
| REQ-040 | Selected layout columns             | 8.1                 | Validated partition/cluster collections and conflict checks                             |
| REQ-041 | Materialisation capability checks   | 1.2, 8.1            | Capability evidence and unsupported MLV/raw-file layout rejection                       |
| REQ-042 | Layout-specific maintenance         | 8.1, 8.2            | Selected creation/maintenance operations and no liquid Z-Order                          |
| REQ-043 | Explicit physical migration         | 8.2, 10.2           | Migration preview, equivalence checks and A4 exclusions                                 |
| REQ-044 | One runtime-driven entry point      | 4.1, 9.2            | Master pipeline, bounded reusable helper and derived deployment inventory               |
| REQ-045 | Validated structured notebook DAG   | 5.2, 6.2            | `validateDAG`/`runMultiple` compilation and topology tests                              |
| REQ-046 | Bounded and serialised writers      | 3.2, 5.2            | Leases/fences, initialisation and concurrency tests                                     |
| REQ-047 | Lakehouse/session compatibility     | 1.2, 5.2, 6.2       | Binding probes and separate Silver/Gold parents                                         |
| REQ-048 | Pinned worker contracts/versions    | 3.1, 6.3, 9.2       | Immutable snapshots, implementation hashes and runtime-only generated cells             |
| REQ-049 | Observable failure propagation      | 3.2, 5.2, 6.2       | Result envelopes, failed-descendant and withheld-release tests                          |
| REQ-050 | Safe submission and retry recovery  | 3.2, 7.1, 9.1       | Journaled operations, MLV job identity and bootstrap resume                             |
| REQ-051 | Independent control-plane bootstrap | 2.1, 9.1            | SQL project, schema/procedures and staged installer                                     |
| REQ-052 | Guided OAuth prerequisite recheck   | 9.1                 | TTY recheck and noninteractive/dry-run CLI tests                                        |
| REQ-053 | Reconciled seed and new inventory   | 2.2, 9.2            | Bootstrap importer, reusable generator and manifest checks                              |
| REQ-054 | Non-destructive transition/parity   | 1.1, 10.1, 10.2     | Baseline fixtures, acceptance report and no-overlap cutover                             |
| REQ-055 | End-to-end operational lineage      | 2.1, 3.2            | Version/run/task/attempt model and ledger reconciliation                                |
| REQ-056 | Counts and actionable safe errors   | 3.2, 5.1, 7.2       | DQ/result summaries, accepted/quarantine totals and redaction                           |
| REQ-057 | Authorised metadata/rule changes    | 2.1, 6.3, 9.1       | Activation roles, transform approval and installer permission checks                    |
| REQ-058 | Measured capacity/cost controls     | 1.2, 5.2, 7.1, 10.1 | Capability bounds, concurrency, refresh mode and measured F8 evidence                   |
| REQ-059 | Distinct rollback surfaces          | 3.2, 8.2, 10.2      | SQL/Delta reconciliation, layout previews and rehearsed version/serving rollback        |
| REQ-060 | Privacy and consumption boundaries  | 1.1, 6.1, 10.2      | Source-boundary fixtures, preserved Gold mappings and unchanged consumer approval gates |

## Known risks and required proof

| Risk                                     | Required treatment                                                                                         |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Dynamic connection field support differs | T004 must prove the actual Fabric connector expression surface; no hidden per-feed deployment fallback     |
| OAuth metadata access from workers       | Use connection-backed export and pinned manifests; do not try to retrieve OAuth credentials in notebooks   |
| SQL/OneLake/Delta partial completion     | Record independent state and reconcile against commit/version evidence; no cross-store atomicity claim     |
| Changed hashes or contracts on import    | Freeze old identities and compare logical history; incompatible changes require explicit migration         |
| Concurrent target/audit writes           | Initialise shared objects first and enforce fenced ownership; test on the actual Fabric engine             |
| Monolithic Gold split changes semantics  | Preserve input populations, version-pinned dimensions, snapshot membership and correction parity           |
| MLVs observe mutable current tables      | Hold serving ownership, verify versions before/after refresh and invalidate conflicting results            |
| Layout/runtime interoperation            | Probe both layout modes and SQL/MLV consumers on supported Fabric; never auto-upgrade historical targets   |
| Incomplete live baseline                 | Re-establish all source/Gold evidence under A2; do not promote earlier partial runs to refactor acceptance |
| Additional capacity consumption          | Estimate the SQL database, Copy, Spark, MLV and maintenance cost before A2; retain measured evidence       |

There are no unresolved product-scope clarification markers. These are
execution proof gates, not permission to mark an unsupported capability
complete. No `research.md` is produced because this pass introduces no new
unresolved specification decision.

## Traceability artifacts

* [Specification to plan](checkpoints/spec-to-plan.yaml)
* [Plan to tasks and immediate dependencies](checkpoints/plan-to-tasks.yaml)
* [Redundancy and deletion assessment](redundancy-assessment.md)
* [Local acceptance and remaining native gates](validation/acceptance.md)
* [Rollback boundaries](validation/rollback.md)
* [Cutover and decommissioning status](validation/cutover.md)

The checkpoints validate coverage and dependency structure only. They are
not implementation, deployment, runtime acceptance or business-publication
signoff.
