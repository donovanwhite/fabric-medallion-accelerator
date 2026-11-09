---
title: Shared accelerator artefacts
description: Reusable medallion mechanisms and explicit case ownership boundaries
ms.date: 2026-11-09
ms.topic: reference
---

## Shared mechanisms

`medallion`, `gold`, `orchestrator` and `pipeline` contain reusable engine
sources and deployment templates. They must not acquire another customer's
source arrays, hardcoded business dictionaries, Gold inventories or policy
exceptions. Shared Python modules have domain-neutral names;
case selection and domain meaning come from explicit profiles.

Generated case notebooks, manifests, contracts and context belong under
`use_cases/<id>/generated`, not beside shared source modules.
Case-specific specialist algorithms belong to the case's `extensions`.

Shared [source connector contracts](pipeline/connectors/README.md) describe
Fabric adapter shapes and metadata bindings. They do not duplicate
case schemas, runtime source configuration or credentials.

## Task-to-code navigation

After the required agent/baseline context, open only the relevant row's module
README, contract, controlling symbol and focused tests. Follow dependencies
only to resolve a concrete uncertainty; inspect shared source, not embedded
copies in generated notebooks. Use the
[specification workflow](../baselines/specifications/specification_workflow.md)
for metadata-first decisions before proposing a framework change.

These routes are source-verified navigation, not execution or acceptance
evidence. Case document paths come from the selected descriptor, not a global
business profile. Module ownership follows the linked source directories.
For contract definitions, reuse the
[metadata contract](../baselines/specifications/metadata_contract.md) and the
linked module READMEs instead of duplicating their contents here.

| Architectural domain | Responsibility | Source file | Entry point or symbol | Metadata contract | Consuming component | Related tests |
|----------------------|----------------|-------------|-----------------------|-------------------|---------------------|---------------|
| Design time: cases | Resolve a case and compile reviewed source/Gold intent | [use_cases.py](../deployment/src/fabric_medallion_deployer/use_cases.py), [spec_compiler.py](../deployment/src/fabric_medallion_deployer/spec_compiler.py) | `load_use_case`, `validate_case_documents`, `compile_use_case` | [Catalog](../use_cases/catalog.json), descriptor `documents`, structured requirements/source/Gold JSON | [specifications.py](../deployment/specifications.py) -> case-generated metadata/contracts | [test_spec_compiler.py](../deployment/tests/test_spec_compiler.py) |
| Shared metadata/control | Validate definitions, select tasks, bind drafts and export lifecycle operations | [metadata.py](../deployment/src/fabric_medallion_deployer/metadata.py), [configuration.py](../deployment/src/fabric_medallion_deployer/configuration.py) | `load_configuration`, `compile_run_plan`, `bind_configuration`, `lifecycle_pipeline_definition` | Typed configuration, [SQL schema](orchestrator/schema.sql), immutable versions | [configure.py](../deployment/configure.py), run preparation and worker manifest loading | [test_metadata.py](../deployment/tests/test_metadata.py), [test_configuration_binding.py](../deployment/tests/test_configuration_binding.py) |
| Design time: packaging | Package shared code and compose bound pipeline definitions | [orchestrator.py](../deployment/src/fabric_medallion_deployer/orchestrator.py), [definitions.py](../deployment/src/fabric_medallion_deployer/definitions.py) | `metadata_runtime_sources`, `metadata_notebook_document`, `metadata_master_pipeline_definition`, `metadata_copy_pipeline_definition` | Compilation receipt, worker-code hashes, resolved Fabric bindings | [build.py](../deployment/build.py) -> notebooks/manifest; [deployer.py](../deployment/src/fabric_medallion_deployer/deployer.py) -> publication definitions | [test_metadata_notebooks.py](../deployment/tests/test_metadata_notebooks.py), [test_metadata_pipeline.py](../deployment/tests/test_metadata_pipeline.py) |
| Runtime: Bronze | Land immutable CSVs through metadata-selected Copy adapters | [Master template](pipeline/bronze_silver_gold_pipeline.json), [Copy template](pipeline/copy_bronze_batch_pipeline.json), [procedures.sql](orchestrator/procedures.sql) | `ctl.usp_copy_task_page` | [Pipeline contract](pipeline/README.md), [connector contracts](pipeline/connectors/README.md), pinned tasks and ordered projections | Copy helper -> Bronze files -> Silver source contexts | [test_metadata_pipeline.py](../deployment/tests/test_metadata_pipeline.py), [test_sql_connectors.py](../deployment/tests/test_sql_connectors.py) |
| Runtime: orchestration | Verify pinned metadata and dispatch bounded notebook DAGs | [silver_orchestration.py](orchestrator/silver_orchestration.py) | `read_run_manifest`, `build_silver_dag`, `run_notebook_dag`, `execute_silver_task` | [Orchestrator contract](orchestrator/README.md), verified manifests, layer dispatch and attempts | Generated parent/worker entry cells via `notebookutils.notebook`; Silver/Gold engines | [test_run_manifests.py](../deployment/tests/test_run_manifests.py), [test_notebook_dag.py](../deployment/tests/test_notebook_dag.py) |
| Runtime: Silver/DQ | Append accepted versions and gate source-wide commit | [ingestion.py](medallion/ingestion.py), [rules.py](medallion/rules.py) | `load_silver`, `validate_source`, `apply_rules` | [Silver contract](medallion/README.md), source contexts, ordered key/hash/column rules, selected case extensions | `execute_silver_task` -> source commit -> Gold preparation | [test_append_only_runtime.py](../deployment/tests/test_append_only_runtime.py), [test_metadata_rules.py](../deployment/tests/test_metadata_rules.py) |
| Runtime: Gold | Build declared dimensions/facts and finalize releases | [gold_orchestration.py](orchestrator/gold_orchestration.py), [gold_runtime.py](gold/gold_runtime.py) | `run_gold_parent`, `execute_gold_task`, `prepare_gold`, `build_gold_object`, `finalize_gold` | [Gold contract](gold/README.md), compiled Gold intent, committed input/output versions | Gold parents/workers -> version-pinned release and MLV inputs | [test_gold_dag.py](../deployment/tests/test_gold_dag.py), [test_gold_runtime.py](../deployment/tests/test_gold_runtime.py) |
| Deployment/validation | Validate packages locally; publish only with separate authorization | [cli.py](../deployment/src/fabric_medallion_deployer/cli.py), [deployer.py](../deployment/src/fabric_medallion_deployer/deployer.py) | `main`, `validate_local`, `AcceleratorDeployer.deploy` | [Deployment guide](../deployment/README.md), generated manifest, authorized bindings/journal | [deploy.py](../deployment/deploy.py) and installed CLI | [test_cli.py](../deployment/tests/test_cli.py), [test_metadata_deployment.py](../deployment/tests/test_metadata_deployment.py) |

The linked shared Python modules and pipeline templates are authoritative
authoring sources. Generated or embedded notebook implementations are derived
packaging: inspect them only for a packaging/binding discrepancy or execution
evidence, never as the first implementation lookup or an edit target.

Design-time contracts produce unapproved drafts; runtime consumes approved,
pinned SQL metadata and verified manifests. Explicit source-contract authoring
does not establish an autonomous discovery service, and semantic declarations
do not establish a deployed semantic model. For MLV/layout-specific work,
continue from the orchestrator and Gold module READMEs rather than loading it
for every task.

Test links identify coverage locations, not passing results. Check private-case,
Spark and native prerequisites before selecting tests; compilation, local
checks, native acceptance, activation and business publication remain distinct.
Use [deployment commands](../deployment/README.md) with an explicitly selected
case and the existing deployment environment. Preserve local case selection
and configuration; do not copy private defaults into shared navigation.

When contracts or symbols move, revalidate the affected rows and their tests
instead of creating another map. Resolve version-sensitive references against
the current reviewed [policy](../baselines/requirements/policy.json), not copied
version labels in older plans. Governing documents and descriptor-declared
case documents are receipt inputs; keep them and compiler receipts unchanged
when maintaining this unpinned navigation index.

## Case-owned assets

Source snapshots, synthetic generators, profiles, validation reports and
historical case pipelines belong under `use_cases/<id>`, not in this tree.
Shared module names, handler namespaces and deployment utilities must be
domain-neutral. New cases do not require another case's fixture population.

Shared runtime code is packaged, not manually forked. Preserve approved
history/hash contracts and the distinction between local component tests,
native acceptance and business publication.
