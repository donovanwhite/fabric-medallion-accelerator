---
title: Local replacement acceptance and remaining native gates
description: Scope-specific evidence for metadata binding, MLV/layout components and repository retirement
author: Data and AI Engineering
ms.date: 2026-11-09
ms.topic: reference
---

## Verdict and scope

Local replacement components and repository retirement have executable
evidence. Full medallion/native acceptance is not signed off.
The user explicitly selected local-only implementation and Fabric SQL
databases as pseudo sources. FTP/SFTP execution is deferred, not passed.
Deployed legacy deletion will be reviewed separately.

Read-only discovery identified the existing Treasury workspace on active
F16 capacity. No SQL/Fabric writes, pipeline runs, schedule/permission
changes, paid resource creation or deployed-item deletions followed.
Older F8 planning assumptions are historical and are not current capacity
or cost measurements.

## Implemented local surfaces

* Immutable source binding fills managed connection IDs and source SQL
  geometry without modifying historical business hash policy. Normal
  publication persists a bound inactive draft and never reseeds sources.
* `configure.py` binds/previews local configurations and exports separate
  parameterized register/validate/approve/activate/rollback operations.
  Activation uses SQL compare-and-swap, not a local success-shaped result.
* The normal build generates six reusable notebooks and an 11-item
  inventory. No default build or installer regenerates the 16 retired
  notebooks or publishes four source/standalone Gold pipeline entries.
* Generic Silver/Gold paths validate existing layouts before writing.
  New business targets require an explicit layout; liquid requires
  runtime-specific evidence. `timestamp_ntz` remains timezone-unspecified.
* The MLV component validates the eleven shipped single-fact SUM/COUNT
  definitions, approved version/hash, schema capability, pinned source
  versions, schedule conflicts and exact aggregate/constraint evidence.
  Refresh intent/job identity persists before monitoring; uncertain
  submissions do not trigger duplicate POSTs.
* The MLV notebook is a pinned metadata preview, not an unrestricted
  authoring/refresh trigger. `mlv_for_release` requires a real external
  serving-ownership checker; refresh also needs retained actual-mode
  evidence. These native adapters have not been fabricated or accepted.
* Gold rules distinguish blocking failures from retained warnings.
  Runtime uniqueness, relationships and full multiset reconciliation
  cannot silently discard financial rows or imply engine enforcement.

## Executed evidence

The final offline suite result is recorded in
[the batch report](../batch-report.yaml). Opt-in skips are not native proof.
The stable final run executed 364 tests: 326 passed, zero failed and
38 opt-in cases skipped, exit code 0, in 339.243 seconds.
The nine real local Spark checks below were executed separately.

Local runs completed:

| Check                                | Result                         | Coverage boundary                                                                        |
|--------------------------------------|--------------------------------|------------------------------------------------------------------------------------------|
| Replacement component selector       | 141 passed, 7 skipped          | Pure/fake-client metadata, MLV, layout and Gold integration                              |
| Generated assets                     | 6 notebooks, 11 manifest items | Reproducible local packaging, no legacy regeneration                                     |
| Default local deployment validation  | Exit 0                         | All 44 source contracts, 1,619 columns and snapshot/provenance checks                    |
| Real Spark Silver quality regression | 1 passed                       | Retained warnings and blocking source checks                                             |
| Real Spark Gold quality/constraints  | 3 passed                       | Warnings, failure, uniqueness, pinned relationships, duplicate-preserving reconciliation |
| Real Spark MLV reconciliation        | 5 passed                       | Distributed aggregate equality and supported runtime constraints                         |
| Changed-source Ruff checks           | Exit 0                         | Runtime, compiler, installer and test files                                              |
| PowerShell parser                    | No syntax errors               | Wrapper syntax only, no cloud invocation                                                 |

Real Spark runs used the installed Spark-compatible JDK 8 and existing
Python dependencies with process-local settings. No dependencies or native
Hadoop binaries were installed. Windows reported missing Hadoop native
tools; local Delta filesystem tests therefore remain a separate opt-in
prerequisite. WSL had no preconfigured PySpark runtime and Docker was not
running. These limitations were not worked around with a fake engine.

## Native acceptance matrix

| Plan surface                      | Native status                                      | Required evidence                                                                                    |
|-----------------------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------|
| SQL binding/validation/activation | Not run                                            | Authorized role execution and exact readback of registered/approved/active versions                  |
| All-source Copy and generated DAG | Not run                                            | Actual 44-table extraction, parameter injection, source commits and fenced outputs                   |
| Recovery/rollback                 | Not run                                            | Existing job reconciliation, retained batch replay and approved rollback drill                       |
| MLV authoring and refresh         | Not run                                            | Schema/authorization proof, serving ownership, selected-lineage job and retained actual refresh mode |
| Partitioned/liquid execution      | Not run                                            | Supported Fabric runtime, actual physical detail and SQL consumer compatibility                      |
| Physical migration                | Not authorized or implemented as a generic rewrite | Target-specific isolated rewrite/cutover contract, retained history and full content/consumer proof  |
| FTP/SFTP                          | Deferred for this demo                             | Future separately approved endpoint/arrival scope if reintroduced                                    |
| Schedules/cutover                 | Not performed                                      | No-overlap proof and operator approval                                                               |
| Deployed legacy deletion          | Not performed                                      | Exact owned IDs, dependency/job review and explicit decommissioning approval                         |

Full SC-001 through SC-012 acceptance, production certification and business
publication remain unverified. Local retirement is not a claim that those
gates passed.
