---
title: Multi-case compilation and deployment tools
description: Explicit case selection, isolated products, immutable metadata binding and safe publication
ms.date: 2026-11-09
ms.topic: how-to
---

## Case selection and output

All accelerator operations must explicitly identify their intended use case.
No customer or demonstration use case is selected by default.

`use_cases/catalog.json` declares registered cases with `default_use_case: null`.
Every operational entrypoint requires explicit `--use-case <id>` selection;
missing/unsupported selections fail before generation, binding, validation or
cloud access. Python callers must also supply the case explicitly, or pass
an already case-bound configuration to APIs that accept it. Resource names
and item namespaces come from the selected descriptor.

CLI case selection is distinct from runtime metadata-version selection.
A blank `p_metadata_version` lets `ctl.usp_prepare_pipeline_run` pin the
approved active version; generated preparation/worker code rejects a
different case identity before dispatch. Supply the intended metadata version
for an explicitly pinned run. This does not select a default CLI use case.

Treasury uses reviewed snapshot/domain adapters. `inventory_example` uses
compiled Markdown contracts and does not require Treasury source CSVs.
Shared mechanisms remain one codebase. Each case's generated notebooks,
metadata/context and manifest live under `use_cases/<id>/generated`.

The normal runtime roles are five generic notebooks, controlled MLV preview,
one master/Copy helper, two Lakehouses and the metadata database.
Counts are derived from reusable roles, not source table count.
Existing source databases are prerequisites; runtime publication never
recreates or reseeds them.

## Author, validate and compile

Read `baselines/README.md`, then the selected case's requirements,
source/Gold contract and plan.

```powershell
.\deployment\.venv\Scripts\python.exe deployment\specifications.py validate --use-case inventory_example
.\deployment\.venv\Scripts\python.exe deployment\specifications.py compile --use-case inventory_example
.\deployment\.venv\Scripts\python.exe deployment\specifications.py compile --use-case inventory_example --check
.\deployment\.venv\Scripts\python.exe deployment\build.py --use-case inventory_example
.\deployment\.venv\Scripts\python.exe deployment\build.py --use-case inventory_example --check
```

Compiler input is explicit JSON in Markdown. Unsupported or incomplete
business semantics block generation. Prose/extraction never becomes
executable Python, arbitrary SQL or automatic business inference.

Normal build validates selected metadata and stale case output.
Generated drafts stay unbound/unapproved and native capability flags remain
false until actual evidence is reviewed through the separate lifecycle.
Hand-edited compiler products block regeneration; change inputs instead.

## Validate runtime packaging locally

```powershell
.\deployment\.venv\Scripts\python.exe deployment\deploy.py --use-case inventory_example --validate-local
$env:PYTHONPATH = "$(Join-Path (Get-Location) 'deployment\src');$(Join-Path (Get-Location) 'deployment\tests')"
.\deployment\.venv\Scripts\python.exe -B -m unittest test_spec_compiler test_multi_case test_cli.EntrypointCaseSelectionTests -q
```

These public-only smoke checks do not authenticate, publish or require private
case assets. Tests compile/generate only disposable fixtures or in-memory bundles.
Compiled cases use their explicit source contracts, not invented seed data.

Do not treat full test discovery as a fresh-checkout smoke command. Several
indexed suites require authorized local case assets; Spark/Delta and native
tiers are separately opt-in. Snapshot adapter validation also reads the
explicitly selected case's source/provenance assets. Inspect test prerequisites
and permissions before expanding beyond the public smoke set.

## Binding and lifecycle

`configure.py` accepts the selected case and explicit source/control binding
files. Binding creates a new content-addressed inactive draft with resolved
source connection/SQL geometry references; it never certifies capabilities.
Lifecycle operation exports remain separate register, validate, approve,
activate/rollback and preview steps with parameter binding and activation
compare-and-swap. They do not automatically submit SQL.

Private binding/journal files contain managed references, never credentials.
Do not commit them, raw evidence or actual source records.

Fabric SQL Database and SQL Server use distinct shared adapters selected
from pinned connection metadata. Fabric SQL binds `FabricSql`/`OAuth2` and
requires an existing Fabric database item ID. SQL Server binds
`SqlServer`/`Basic`; its existing managed connection supplies server/database
and gateway routing, so it has no `source_database_ids` entry.
Every configured feed still requires a resolved `connection_ids` entry.
Mixed cases supply database IDs only for Fabric SQL feeds.

SQL Server preflight resolves Fabric's managed `SQL` connection type,
checks Basic mode, permitted connectivity and gateway references, and tests
connection availability before publication. This does not certify Copy or
set the required `sql-server-copy-basic` capability verified. Baseline 1.1.0
keeps other SQL Server authentication modes explicitly unsupported until
implemented and evidenced. See the [connector contracts](../artefacts/pipeline/connectors/README.md).

## Remote publication is separate

Remote preflight/publication requires an explicitly approved target, online
authorized source/control connections and a mode-specific persistent journal.
Recheck identities on resume and reconcile uncertain submissions rather
than blindly retry. Compatible overwrites require explicit approval.

No example above activates a configuration, starts a source job, rewrites
layouts, assigns permissions, creates paid resources or deletes deployed
items. Case generation, native acceptance, business publication and
decommissioning remain distinct gates.

## Compatibility

The shared Python package is `fabric_medallion_deployer`; installed CLI
packaging exposes `fabric-medallion-deploy`. Repository entry scripts retain
their documented paths. Older imports using the former case-named package
must use the neutral namespace.

Optional fixture tools are `deployment/generate_fixtures.py` and
`deployment/validate_fixtures.py`. They select declared case adapters;
case implementations/data live in `use_cases/<id>/fixtures`, never shared
`artefacts`. They do not generate or mutate source data during local
validation.

Historical module/package names and reviewed snapshot aliases preserve
existing proof contracts. They are not baseline business defaults.
Treasury's reviewed provenance rebaseline records file moves/builder changes
separately from unchanged CSV bytes, schemas and business identity.
Do not regenerate data to hide a relocation defect.
