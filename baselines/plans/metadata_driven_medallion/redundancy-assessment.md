---
title: Metadata-driven medallion redundancy and deletion assessment
description: Plan-based retirement inventory, coupled cleanup changes, retained contracts and deletion readiness
author: Data and AI Engineering
ms.date: 2026-11-09
ms.topic: reference
---

## Assessment outcome

Current disposition on 2026-11-09: the 16 notebooks listed in D01/D02 have
been deleted from the repository. Normal generation/publication now uses
six reusable notebooks and an 11-item inventory. The four legacy pipeline
names no longer belong to that inventory.

Shared runtime code, source contracts, frozen parity fixtures, historical
definition builders, the sequential Gold compatibility wrapper and the old
extraction template remain. The retained MLV notebook is metadata-backed
and preview-safe; explicit lifecycle execution requires real serving
ownership and refresh-mode evidence.

The user selected local-only implementation, Fabric SQL pseudo sources,
no current FTP/SFTP execution and later review of deployed legacy deletion.
Local source retirement is not an A3 cutover or A4 decommissioning pass.
No deployed items, source data, permissions, schedules or active SQL
configuration were changed.

The assessment below records the original reviewed baseline and deletion
scope. Its historical "currently present" observations are not the current
repository inventory.

The target architecture supersedes all 15 source-specific medallion notebooks.
The standalone Gold build notebook is also a retirement candidate because
the plan replaces separate source/Gold execution with one metadata-driven
master and generic Silver/Gold parents and workers.

The assessed file-deletion inventory contains 16 notebooks, currently
5,229,751 bytes in total. Four existing pipeline item names must leave the
normal deployment inventory. They are generated workspace definitions, not
four separate pipeline JSON files to delete.

Current build and installer references are cleanup dependencies, not evidence
that these assets belong in the target design. Remove those references with
the superseded outputs rather than preserving source-specific execution as
the permanent default.

The original assessment documented deletion scope only. Subsequent local
implementation performed D01/D02 retirement; it did not approve cutover
or authorize remote decommissioning.
Local source retirement and deletion of deployed Fabric items are separate
operations.

## Authority and classification

Reviewed against [the implementation plan](plan.md), at repository commit
`73a9bac`. The worktree was clean before this report was added.
No standalone `constitution.md` was found; the plan records the same boundary.

| Classification   | Meaning                                                                                       |
|------------------|-----------------------------------------------------------------------------------------------|
| Delete           | Superseded deployable file; remove with its coupled references and reviewed rollback handling |
| Retire           | Stop generating, publishing or exposing an obsolete normal-run entry point                    |
| Refactor         | Remove obsolete branches or assumptions, but retain the containing file                       |
| Retain           | Required shared behaviour, bootstrap provenance, historical compatibility or target asset     |
| Archive          | Historical evidence, excluded from normal runtime generation and publication                  |

The controlling plan decisions are:

* Plan items 4.1 and 5.2 replace deployment-generated source arrays and
  five-notebook source chains with runtime task selection and generic workers.
* Plan items 6.1 and 6.2 move Gold configuration to pinned manifests and split
  execution into preparation, object tasks and finalization.
* Plan item 7.1, specifically T039, retains and adapts the MLV-authoring
  notebook. It is not a blanket Gold-notebook deletion.
* [Plan item 9.2](plan.md#plan-item-92-generate-reusable-assets-and-retire-runtime-duplication)
  retires runtime duplication, embedded active contracts and the old fixed
  inventory. Legacy definitions remain only for reviewed rollback and replay.
* Plan item 10.2 and gates A3/A4 separate cutover from decommissioning.
  Successful deployment does not authorize deleting old workspace objects.

## File deletion inventory

### D01 Delete the 15 source-specific notebooks

These are source-bound generated wrappers, not 15 independent business
implementations. Each embeds an active `SOURCE_CONFIG`, the shared rules
and ingestion runtime, then calls one step of the old source chain.

Plan mapping: T022-T030, T050-T055; REQ-001, REQ-044, REQ-048, REQ-053.
Replacement: pinned source contexts, `nb_prepare_medallion_run`,
`nb_orchestrate_silver` and `nb_silver_worker`.

The exact file inventory is:

```text
artefacts\medallion\bas\nb_bas_setup_and_namespace_validation.ipynb
artefacts\medallion\bas\nb_bas_bronze_csv_inventory.ipynb
artefacts\medallion\bas\nb_bas_bronze_csv_to_silver_delta.ipynb
artefacts\medallion\bas\nb_bas_bronze_silver_validation.ipynb
artefacts\medallion\bas\nb_bas_bronze_snapshot_cleanup.ipynb
artefacts\medallion\csd\nb_csd_setup_and_namespace_validation.ipynb
artefacts\medallion\csd\nb_csd_bronze_csv_inventory.ipynb
artefacts\medallion\csd\nb_csd_bronze_csv_to_silver_delta.ipynb
artefacts\medallion\csd\nb_csd_bronze_silver_validation.ipynb
artefacts\medallion\csd\nb_csd_bronze_snapshot_cleanup.ipynb
artefacts\medallion\persal\nb_persal_setup_and_namespace_validation.ipynb
artefacts\medallion\persal\nb_persal_bronze_csv_inventory.ipynb
artefacts\medallion\persal\nb_persal_bronze_csv_to_silver_delta.ipynb
artefacts\medallion\persal\nb_persal_bronze_silver_validation.ipynb
artefacts\medallion\persal\nb_persal_bronze_snapshot_cleanup.ipynb
```

Evidence: `deployment\src\fabric_treasury_deployer\definitions.py:219-278`
constructs the embedded source contract and step call;
`deployment\build.py:41-59` regenerates every source notebook;
`deployment\src\fabric_treasury_deployer\deployer.py:263-289` publishes
the source chain.

Delete the listed notebooks with the R01-R04 changes below. Remove their
three source directories only if no retained files remain. Do not delete
the parent `artefacts\medallion` directory or its shared Python modules.

### D02 Delete the superseded standalone Gold build notebook

```text
artefacts\gold\nb_treasury_silver_to_gold.ipynb
```

Plan mapping: T031-T036, T050-T055; REQ-027, REQ-044, REQ-045, REQ-048.
Replacement: `nb_orchestrate_gold`, `nb_gold_worker` and the Gold stage
of `pl_bronze_silver_gold`.

This is a target-design retirement inference from the one-master decision
and the proposed source structure, not an explicit filename-deletion task.
The plan's proposed tree retains the MLV notebook but omits this build notebook.

Evidence: `deployment\src\fabric_treasury_deployer\gold.py:622-698`
embeds `GOLD_CONFIG` and calls the sequential `run_gold` wrapper;
`gold.py:744-781` constructs its separate pipeline;
`deployer.py:292-315` publishes that separate execution path.

The replacement Gold notebook files are currently absent. Shared Gold
preparation, object execution, DAG and finalization functions already exist;
their presence does not establish a deployable generic Gold notebook chain.
Retain the shared runtime and its compatibility wrapper as required by T034.

### A01 Archive the old extraction template rather than blanket-delete it

```text
artefacts\pipeline\full_snapshot_pipeline.json
```

Plan mapping: existing preservation anchors, T016-T018, T053 and T062-T064.
Replacement: `bronze_silver_gold_pipeline.json` and
`copy_bronze_batch_pipeline.json`.

The template is obsolete for normal execution, but the plan explicitly says
to retain it as legacy evidence. Remove it from the active compiler path
and retain it in a reviewed rollback/evidence location. Physical deletion
requires an explicit retention decision; this report does not override that
preservation requirement.

Evidence: `plan.md:72` retains the template as legacy evidence;
`deployment\src\fabric_treasury_deployer\definitions.py:326-361`
still reads it and inserts deployment-generated `p_table_config`.

## Coupled code and inventory retirement

### R01 Replace source-specific generation and validation

Plan mapping: T028, T035, T039, T053-T054.

| Surface                                               | Obsolete normal-run scope                                                                        |
|-------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `deployment\build.py:41-59`                           | Loop generating five notebooks per source                                                        |
| `deployment\build.py:60-77`                           | Standalone Gold build output; retain the adapted MLV-authoring output                            |
| `deployment\build.py:78-97`                           | Generic notebooks treated as an optional add-on rather than the target generation path           |
| `deployment\build.py:111`                             | Hardcoded notebook and 26-item inventory totals                                                  |
| Deployer `definitions.py:118-128`                     | `copy_table_config()` as the normal extraction task authority                                    |
| Deployer `definitions.py:219-278`, `292-324`          | `notebook_document()` and `notebook_definition()` for source-bound wrappers                      |
| Deployer `definitions.py:326-392`                     | `pipeline_definition()` for separate source chains and deployment-generated arrays               |
| Deployer `deployer.py:120-189`                        | Local validation requiring retired source notebooks and the standalone Gold build/pipeline       |
| Deployer `gold.py:622-742`, `744-781`                 | Build-only notebook generation/binding branch and `gold_pipeline_definition()`                   |

Paths prefixed with "Deployer" are relative to
`deployment\src\fabric_treasury_deployer`.

Refactor these branches together. Do not delete `build.py`, `definitions.py`,
`deployer.py` or `gold.py` wholesale. Keep shared cell/part builders,
definition validation, SQL serialization, SQL project generation, metadata
pipeline generators and MLV-authoring generation.

Remove or redefine the transitional `--metadata` switch when generic generation
becomes the default. Do not remove the generic generation branch itself.
Retained rollback outputs must have an explicit compatibility path, not
silently reappear in the normal build.

### R02 Retire four independent pipeline entries

Plan mapping: T016-T018, T050-T055 and T062-T064.

```text
pl_full_medallion_bas_finance
pl_full_medallion_csd_finance
pl_full_medallion_persal_finance
pl_silver_to_gold_treasury
```

Remove these names from the normal generated manifest and installer ownership
inventory. Replace their normal operational role with the metadata master
and reusable Copy helper. Historical names may remain in evidence, original
contracts and an explicit rollback inventory.

Evidence: `deployment\manifest.json:4-108`;
`deployment\src\fabric_treasury_deployer\contracts.py:362-376`;
`deployer.py:250-315`; `gold.py:17-21`.

Retire `GOLD_PIPELINE` and the source-specific publication loops from normal
deployment after wiring their replacements. Keep the required connections and
source simulator contracts. Do not reseed source databases merely to install
or update the orchestrator.

No separate per-source pipeline JSON files exist for these four generated
items. Removing local entries neither stops jobs/schedules nor deletes
already deployed items. Those actions require later approved cloud work.

### R03 Regenerate the manifest and separate historical identity

Plan mapping: T001-T002, T008, T022-T023 and T053-T054.

Refactor `STEPS`, `Source.notebooks`, source notebook-name uniqueness checks
and `owned_inventory()` so they no longer define the normal reusable inventory.
Keep historical name mappings only where reviewed replay or rollback needs them.
Regenerate `deployment\manifest.json`; do not delete the manifest.

Evidence: `deployment\src\fabric_treasury_deployer\contracts.py:16-22`,
`109-115`, `346-376`; `deployer.py:104-112`;
`artefacts\sources\source_registry.json:8-52`.

The registry's old `pipeline` fields are obsolete deployment metadata, but
deleting them naively changes historical contract identity:
`contracts.py:321-324` hashes the complete registry entry. Preserve the frozen
import/hash representation while separating new runtime/deployment inventory.
Do not silently recalculate old contract hashes to make cleanup pass.

Retain `Source.runtime_config()`, `Source.bronze_root`, `load_sources()` and
the source registry as bootstrap/historical adapters. They are not authorized
as fallback configuration authorities for new metadata runs.

### R04 Replace obsolete test expectations without deleting semantic tests

Plan mapping: T002, T018, T026, T030, T033, T036, T052 and T054.

| Existing assertion or test surface                          | Required change                                                                                  |
|-------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `deployment\tests\test_contracts.py:117`, `142-145`         | Replace normal inventory count 26 and five-notebook expectations with declared reusable assets   |
| `deployment\tests\test_deployer.py:139-157`                 | Replace 26 items, 17 notebooks, four pipelines and three-database normal deployment assumptions  |
| `deployment\tests\test_definitions.py:84-172`               | Retarget source-chain/embedded-notebook tests to generic bindings and runtime metadata selection |
| `deployment\tests\test_gold_contracts.py:152-231`           | Retarget standalone build/pipeline checks; preserve Gold binding, stale-code and DQ assertions   |
| Existing source serialization and safe-deployment tests     | Preserve exact types, identifier quoting, security settings and no-automatic-job execution       |
| Historical path, append-only, Gold and baseline tests       | Retain semantic coverage and explicit legacy compatibility fixtures                              |

Do not remove the regression suites just because they reference legacy
helpers. Move the relevant assertions to generic surfaces; keep compatibility
assertions explicitly scoped to compatibility, not the normal inventory.
The 44-table source inventory is a business boundary, not the obsolete
26-item deployment count.

### R05 Update operational documentation and architecture

Plan mapping: T055 and T064.

Update rather than delete these documentation surfaces when the corresponding
runtime transition is implemented:

* `README.md:24-28`: separate source pipeline names presented as the inventory
* `deployment\README.md:10-21`, `126-134`: old generated counts and publication flow
* `artefacts\medallion\README.md:8-43`: five-notebook-per-source inventory and transitional generation
* `artefacts\pipeline\README.md:8-32`, `77-80`: source chains and separate Gold pipeline
* `artefacts\gold\README.md:19-24`: standalone Gold build execution surface
* `deployment\fabric-architecture.svg:17-23`, `39-40`: source arrays, separate chains and fixed counts

Retain previous delivery plans, checkpoints and batch reports as dated
historical evidence. They are not executable duplication. Label their role
and link the current plan; do not rewrite old results to imply new acceptance.

## Assets and behaviour that must remain

| Asset or behaviour                                                                  | Retention reason and plan mapping                                                                         |
|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `artefacts\medallion\treasury_ingestion.py`                                         | Typed parsing, append-only versions, hashes, quarantine and source commits; T022-T029                     |
| `artefacts\medallion\treasury_rules.py`                                             | Named/versioned specialist and generic DQ handlers; T024-T026                                             |
| `artefacts\orchestrator\treasury_orchestration.py`                                  | Pinned manifests, receipts, task results, leases and Silver DAG/fan-in; T011-T015, T020, T027-T030        |
| `artefacts\orchestrator\schema.sql`, `procedures.sql`                               | Control-plane metadata, activation and execution ledger; T006-T007, T013                                  |
| Deployer `metadata.py`, `orchestrator.py`                                           | Typed configuration/run planning, bootstrap import and generic notebook packaging; T005, T008, T010, T028 |
| Three generic preparation/Silver notebooks under `artefacts\orchestrator\notebooks` | Target reusable runtime outputs, not source-specific duplication; T028                                    |
| `artefacts\gold\treasury_gold.py`                                                   | SCD2, fact correction, preparation/object/finalization and release semantics; T032-T036                   |
| `artefacts\gold\nb_treasury_gold_mlv_definitions.ipynb`                             | Explicitly retained and adapted for controlled MLV lifecycle; T039                                        |
| Metadata master/Copy templates and distinct FTP/SFTP adapters                       | Required connector-specific landing behind one operational entry point; T016-T021                         |
| Source registry, schemas, source manifests, CSV seeds and synthetic builders        | Reconciled bootstrap, demo simulator setup and hash-pinned provenance; T001-T002, T008, T031              |
| `deployment\tests\fixtures\medallion_baseline.json`                                 | Frozen historical identity and semantic comparator; T001-T002, T054, T061                                 |
| Shared Fabric client, authentication and deployer entry points                      | Reused safe deployment and job controls; T014, T040, T050-T052                                            |
| Requirements, source specification, Gold/semantic/reporting/planning designs        | Business requirements and scope evidence; not alternative executable implementations                      |

Some compatibility names look obsolete but preserve required behaviour:

* `cleanup_snapshots()` performs retention verification, not deletion.
  The new controller still calls it in
  `artefacts\orchestrator\treasury_orchestration.py:2722-2728`.
  T029 requires retaining no-deletion retention.
* `processing_context()` and historical path/hash handling remain adapters
  for original batches. T022 explicitly preserves legacy resolution.
* `run_gold()` is a compatibility wrapper retained by T034. It is not the
  normal metadata entry point and must not be deleted with D02.
* `gold_config()` and `bootstrap_gold_config()` still export seed contracts.
  T031 separates bootstrap from runtime; it does not remove bootstrap.
* Rule handlers named `legacy.*` preserve existing blocking checks. T008
  and T024 require their versioned representation, not their deletion.
* Embedded shared code in generic notebooks is intentional packaging.
  T028 and T053 allow shared runtime/validator cells. Embedded active source,
  column and rule configuration is the duplication to eliminate.

Do not include retained Bronze, Silver, quarantine, audit tables, metadata
versions or Gold releases in an artefact cleanup. Their preservation,
replay and publication semantics remain requirements.

## Deletion readiness and execution order

The design classification is settled; integrated deletion readiness is not.
Local code can be refactored without deploying or deleting cloud resources,
but deleting the listed files alone breaks the current compiler/installer.

Observed replacement gaps at the reviewed commit:

* `nb_orchestrate_gold.ipynb` and `nb_gold_worker.ipynb` are absent even though
  the metadata master requires a Gold notebook binding
  (`deployment\src\fabric_treasury_deployer\definitions.py:720-734`).
* `artefacts\orchestrator\treasury_layout.py` and the end-to-end live driver
  `deployment\tests\test_orchestration_live.py` are absent.
* Generic inventory binding and full installer acceptance remain incomplete.
  The default installer still publishes D01/D02 and the four old pipelines.
* `validation\acceptance.md`, `validation\rollback.md` and
  `validation\cutover.md` do not yet exist.
* [Capability evidence](validation/capabilities.json) records the metadata
  pipeline runtime as unverified, source staging/cutover as unapproved and
  current execution scope as local only.

The absence of later planned components is unfinished implementation, not
additional dead code. Gold kernels already exist beyond the plan summary's
pending labels; do not reimplement or discard them merely because integrated
task acceptance remains pending.

Execute cleanup in this order:

1. Complete the reusable Silver/Gold, MLV, layout and installer dependencies
   named by T053. Preserve the tested processing algorithms.
2. Under T053-T055, replace normal generation, validation, deployment inventory
   and documentation coherently. Remove D01/D02 from active source outputs;
   preserve reviewed rollback definitions outside the normal entry path.
3. Keep the old extraction template as evidence and retain original contract
   identities. Retarget tests and regenerate the reusable manifest.
4. Verify local generic packaging and frozen business parity. Confirm that
   the normal build cannot regenerate D01/D02 or embedded active contracts.
5. After the required A2 approval, complete T056-T061 acceptance and T062
   rollback proof. Local checks do not replace these Fabric gates.
6. Under A3/T063, inspect actual schedules/jobs and prevent overlapping old
   and new writers before activation. Stop exposing legacy normal-run paths.
7. Under separately approved A4 decommissioning, remove only identified
   deployed legacy items. Do not infer cloud deletion from local file removal.

This report is not a command list for immediate deletion and does not change
the plan's approval boundaries.

### Replacement-first implementation update

After this assessment, the user explicitly chose replacement before deletion,
then confirmed local-only implementation. The replacement components were
implemented and the listed repository notebooks were retired.

The local replacement now includes `nb_orchestrate_gold.ipynb`,
`nb_gold_worker.ipynb` and `treasury_gold_orchestration.py`. The master has
Gold attempt reservation, dispatch export, object execution, failed/successful
reconciliation and a sealed result gate. These address the missing Gold
notebook/component paths identified at the reviewed commit; they do not
establish native execution, MLV/layout completion or approved cutover.

An explicit `--bootstrap-orchestrator --metadata-assets` publication path
also derives and binds the 10 reusable control/Lakehouse/notebook/pipeline
items without source reseeding or deletion. Its mode-specific journal
reconciles publication operations and rechecks prerequisite identities.
Full publication now persists bound source connection/geometry references
in a distinct inactive draft. Offline lifecycle exports support separate
validation, approval and compare-and-swap activation/rollback; none of those
SQL operations was executed against Fabric.

The earlier inventory and design classifications remain valid. Original
observations above refer to commit `73a9bac`, not a claim that the new local
Gold files are still absent. The normal deployment path is now reusable.
Historical definition builders remain explicitly separated from normal
outputs, while native replacement acceptance remains unverified.

## Evidence from this review

The focused dependency checks passed locally: 66 tests run, 66 passed,
zero failed and zero skipped, exit code 0. They exercise current contracts,
old and generic definition generation, installer behaviour and Gold/DAG
components. Fake-client tests do not mutate a Fabric workspace.

```powershell
$env:PYTHONPATH = (Join-Path (Get-Location) 'deployment\src') + ';' +
    (Join-Path (Get-Location) 'deployment\tests')
.\deployment\.venv\Scripts\python.exe -B -m unittest `
    test_contracts test_definitions test_deployer test_gold_contracts `
    test_gold_dag test_metadata_notebooks test_metadata_pipeline -q
.\deployment\.venv\Scripts\python.exe -B .\deployment\build.py --metadata --check
```

The generation check passed for 20 current notebooks and the legacy 26-item
manifest, exit code 0. This proves generated outputs match today's compiler.
It does not prove that the inventory satisfies T053, that deletion is ready
or that the metadata framework has passed live acceptance.

Exact-path inventory inspection confirmed the 16 deletion candidates, their
embedded contracts, four generated legacy pipeline names and the replacement
gaps listed above. No cloud discovery, pipeline execution, resource mutation
or file deletion was performed.

## Completion criteria for the deletion change

* D01/D02 are absent from normal source outputs and cannot be regenerated
  or republished by the default build/deployer.
* Normal inventory derives from declared reusable assets, not source count
  or a fixed 26-item expectation.
* New runs consume approved pinned metadata without repository-array or
  embedded-contract fallback.
* Reviewed rollback/replay assets remain explicitly separated and original
  contract hashes, paths and observation times still resolve.
* Business-rule, Silver history, Gold grain/correction and version-pinned
  release assertions survive the test migration.
* MLV authoring, no-deletion retention and shared runtime modules remain.
* Documentation describes actual implemented entry points; native acceptance
  and any deployed-item deletion have their own recorded evidence/approvals.
