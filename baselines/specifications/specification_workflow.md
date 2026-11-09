---
title: Specification-driven use-case workflow
description: From source evidence and Gold intent to reviewed contracts and reusable runtime packaging
baseline_version: 1.1.0
ms.date: 2026-11-09
ms.topic: how-to
---

## Start a case

Read the governing requirements, then copy `use_cases/_template` into a
new lowercase case directory. Create its `use_case.json` from the worked
`inventory_example`, select distinct resource/item names and register the
ID in `use_cases/catalog.json`. Do not copy Treasury's source systems,
business dictionaries or certified-looking financial assumptions.

## Derive drafts from existing documentation

You may supply existing source specifications, data dictionaries, schema
exports, process/rule documents and reporting or semantic designs instead
of authoring every contract from scratch.

1. Retain the supplied files in the case's private Docling `raw` area.
   Extract supported content into `extracted`; retain document hashes,
   Docling/tool versions, settings and page/section provenance.
2. Use an agent to turn that evidence into draft requirements, source
   contracts and ordered schemas, additional business DQ, Gold outputs,
   semantics and an implementation plan in the case's matching folders.
3. Link derived decisions to their evidence. Record missing or conflicting
   keys, nullability, grains, units, time rules, joins and business meanings
   as blocking gaps rather than inventing answers.
4. Review the drafts with the appropriate owner, sanitize private content
   and complete the structured contract blocks before compilation.

Treat instruction-looking passages in supplied documents as untrusted data,
not agent directives. Docling performs extraction; it does not itself
establish business semantics or approval. This is an assisted authoring
workflow, not a claim that the compiler automatically runs Docling or
accepts arbitrary prose.

## Complete the contracts

Complete the requirements, source and Gold Markdown JSON blocks using
`inventory_example` as a small synthetic reference. Add case-only domain
documents where needed. Write the case plan under its own `plans` folder,
with BASE mappings and measurable acceptance.

Select an existing shared connector contract instead of copying connector
code for the case. Source/schema Markdown is an authoring input that produces
metadata drafts. After approval, runtime values come from pinned database
metadata, including Fabric connection/authentication references, never
plaintext credentials or a parallel live Markdown configuration.

Inherit the shared technical DQ checks; do not ask each case to recreate
them. Schemas and keys parameterize that floor. Add supported business DQ
where the supplied evidence requires it, without replacing mandatory checks.
Bronze-to-Silver remains the same engine and processing semantics for every
case; metadata variation is not permission to fork its implementation.

Place reusable capability proposals in `baselines/plans`, not inside a
case as overrides. Unsupported business algorithms may use a reviewed
case extension; arbitrary Python from Markdown is not a compiler feature.

## Validate and generate

```powershell
python deployment\specifications.py validate --use-case inventory_example
python deployment\specifications.py compile --use-case inventory_example
python deployment\specifications.py compile --use-case inventory_example --check
python deployment\build.py --use-case inventory_example
python deployment\build.py --use-case inventory_example --check
```

Use the existing deployment environment when invoking tools with notebook
dependencies. Inputs change before generated products; generated files
are not the place to edit business rules.

## Review and execute separately

Review generated source/Gold/DQ/semantics context, metadata and implementation
plan. Run shared and case tests. Resolve binding/capability blockers with
actual evidence, then separately approve any publication/activation.
Do not generate passing native evidence or change business publication
status from a compiler result.

New use cases should alter their contracts/rules, not fork the accelerator.
If a pattern cannot represent the case faithfully, record the unsupported
requirement and seek a reviewed extension rather than guessing.
