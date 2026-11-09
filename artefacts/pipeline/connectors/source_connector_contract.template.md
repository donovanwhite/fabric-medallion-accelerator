---
title: Reusable source connector contract template
description: Define a shared Fabric adapter without embedding case configuration, schemas or credentials
ms.date: 2026-11-09
ms.topic: reference
---

## Purpose and status

Copy this authoring skeleton for a proposed shared connector capability,
not for every case/source table. Replace the placeholders and reference its
baseline capability plan. This Markdown does not register or enable code.
Use [the existing connector contracts](README.md) before proposing another.

* Connector/source type: `<connector_identifier>`
* Contract/adapter version: `<reviewed_version>`
* Fabric activity, source and dataset types: `<documented_shapes>`
* Supported authentication modes: `<managed_modes>`
* Implementation/template reference: `<shared_implementation_path>`
* Evidence state and capability gates: `<draft_or_scoped_evidence>`

## Metadata input bindings

Name required fields, their pinned metadata origin and the Fabric property
or expression receiving them. Specify type, validation and missing-value
failure behavior. Do not put actual source IDs, table/column inventories,
host addresses or credential values here.

| Required value                      | Pinned metadata/task origin | Fabric binding / validation |
|-------------------------------------|-----------------------------|-----------------------------|
| Connector and authentication type   | `<origin>`                  | `<binding_and_guard>`       |
| Managed connection/gateway reference | `<origin>`                 | `<binding_and_guard>`       |
| Source location or item geometry    | `<origin>`                  | `<binding_and_guard>`       |
| Object/projection or file selection | `<origin>`                  | `<binding_and_guard>`       |
| Batch/attempt and landing identity  | `<origin>`                  | `<binding_and_guard>`       |

For schema/keys/DQ, reference the existing shared metadata contract rather
than defining a second schema array in the connector.
Describe any source-format/arrival constraints and their explicit evidence.

## Fixed shared behavior

Record how the adapter preserves exact source representations, immutable
batch membership, attributable attempts, failure propagation and safe
reconciliation. Preserve standard Bronze-to-Silver and technical DQ behavior.
No automatic destructive retry, silent default or source-specific fork
may be introduced.

Separate deployment root bindings from per-run metadata values. The control
connection must be available before metadata lookup; declared case resources
must remain compatible with authorized deployment targets.

## Authentication and evidence

Reference Fabric-managed connections and supported authentication metadata.
Never add password, token, private-key or connection-secret fields.
Missing/unavailable/unauthorized connections block the operation.

List local payload checks separately from required native connector,
authentication, format/arrival, recovery and reconciliation evidence.
An authored template or generated draft cannot mark those gates passed.
