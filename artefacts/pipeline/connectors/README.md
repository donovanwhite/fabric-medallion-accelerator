---
title: Reusable Fabric source connector contracts
description: Shared connector shapes with runtime values resolved from approved pinned metadata
ms.date: 2026-11-09
ms.topic: reference
---

## Connector templates are not source inventories

A connector contract defines the Fabric connector/activity shape, supported
authentication modes, required metadata fields and parameter bindings.
Reuse it across cases and source objects. Do not copy a connector for each
database, table or customer, or embed their columns/configuration in it.

Use the [contract template](source_connector_contract.template.md) when
designing a reusable connector capability. It is an authoring/review template,
not a new runtime configuration format or an automatically enabled adapter.

At runtime, the approved version in `db_orchestrator` and its verified
immutable run snapshot supply source configuration, ordered projections,
schema/key/DQ contracts and managed connection references. The Copy helper
uses the run's pinned task page, not current Markdown or a fresh active-version
lookup for each table.

## Existing implementation contracts

| Source family | Metadata connector | Shared implementation                                     | Supported authentication |
|---------------|--------------------|-----------------------------------------------------------|--------------------------|
| `sql`         | `FabricSql`        | [Fabric SQL Database Copy](fabric_sql_database_copy.json)  | Managed `OAuth2`         |
| `sql`         | `SqlServer`        | [SQL Server Copy](sql_server_copy.json)                    | Managed `Basic`          |
| `ftp`         | `Ftp`              | [FTP Copy](ftp_copy.json)                                  | Managed `Basic`/anonymous |
| `sftp`        | `Sftp`             | [SFTP Copy](sftp_copy.json)                                | Managed `Basic`          |

Both SQL adapters are available to the case compiler under baseline 1.1.0.
The Copy helper selects `@item().connector_type`, resolved from the pinned
connection metadata. `source_type` remains the transport family, so both
SQL adapters share projection, immutable CSV landing and downstream behavior.
Unknown connector/authentication combinations fail instead of defaulting.

Deploy the updated SQL control procedures and Copy helper together:
`ctl.usp_copy_task_page` must export the pinned `connector_type` consumed
by the new Switch. An old task-page contract is not a fallback. New metadata
remains an inactive draft until its separate validation/approval/activation
and native capability gates are satisfied.

File adapters require their approved connector/authentication, arrival and
runtime evidence before use. Their presence does not enable them or prove
native acceptance. The shared definition builder composes the JSON
activities; the Markdown contract does not execute.

## Fabric SQL binding example

The Fabric SQL adapter uses `FabricSqlDatabaseSource`, a managed
`FabricSqlDatabase` connection and the standard immutable Bronze sink.
Its dataset schema is not a second hand-maintained column inventory.

| Runtime value                | Metadata authority / derivation                                                | Copy binding or use                        |
|------------------------------|--------------------------------------------------------------------------------|--------------------------------------------|
| Source family                | `cfg.source_feed.source_type`                                                  | SQL projection/landing protocol            |
| Connector selection          | `cfg.connection_ref.connector_type`                                            | `@item().connector_type` selects adapter    |
| Connector/authentication     | `cfg.connection_ref.connector_type` and `authentication_type`                   | Validate compatibility before dispatch     |
| Managed source connection    | `cfg.connection_ref.fabric_connection_id`                                       | `@item().connection_id`                     |
| Source workspace/item        | `cfg.source_feed.source_workspace_id` and `source_artifact_id`                   | `@item().source_workspace_id`, artifact ID  |
| Ordered columns and contract | `cfg.object_column` and `cfg.data_object.contract_json`                          | Validated projection in `@item().sql_query` |
| Landing folder/file          | Pinned `ctl.run_task.task_json` target and declared object                       | `@item().folder_path`, `@item().file_name`  |

`ctl.usp_copy_task_page` resolves these values from the immutable run
snapshot and reserved tasks. Its `connection_id` output is the resolved
Fabric connection UUID, not the logical metadata connection key.
SQL projection/identifier validation remains shared; this is not permission
to accept arbitrary executable SQL from source documents.

## SQL Server binding

Declare `source_type: sql`, `connector_type: SqlServer` and
`authentication_type: Basic` in the source authoring contract. Omitted
`connector_type` retains the existing `FabricSql` authoring default;
new contracts should state it explicitly.

SQL Server uses its existing Fabric-managed SQL connection for the server,
database and authentication. It does not resolve a Fabric `SQLDatabase`
item, and Fabric workspace/artifact source geometry must remain absent.
Binding requires its managed connection UUID but no entry in
`source_database_ids`. Mixed cases include database IDs only for their
Fabric SQL feeds and managed connection IDs for every feed.

Publication preflight checks the managed connection's SQL type, Basic
credential mode, online status and supported cloud/on-premises/VNet
connectivity. Gateway connections require a valid gateway reference;
an explicitly declared metadata gateway must match. Credentials remain
inside the managed connection.

`sql-server-copy-basic` is a mandatory native-evidence capability even if
an object's optional capability list is edited. Compilation/binding never
sets it verified. Other SQL Server authentication modes require a separately
implemented and evidenced capability; their Fabric platform availability
does not automatically enable them in this accelerator.

Official references:

* [SQL Server connector capabilities](https://learn.microsoft.com/fabric/data-factory/connector-sql-server-database-overview)
* [SQL Server Copy settings](https://learn.microsoft.com/fabric/data-factory/connector-sql-server-copy-activity)
* [Fabric SQL Database Copy settings](https://learn.microsoft.com/fabric/data-factory/connector-sql-database-copy-activity)
* [Managed connection types, authentication and gateway references](https://learn.microsoft.com/rest/api/fabric/core/connections/get-connection)

## Credentials and bootstrap boundary

Metadata stores connector/authentication types, managed Fabric connection
IDs and supported gateway references. Credentials/tokens remain in Fabric's
managed connection/authentication facilities, not in metadata rows, Markdown,
generated code, task payloads or logs.

The authorized control-database connection and target workspace/Lakehouse
bindings are deployment prerequisites. Templates need that root binding
before they can read metadata; source metadata cannot redirect a deployment
to an unbound target. New credential resolution mechanisms require separate
design and approval, not a plaintext metadata field.

## Source schemas remain contracts

The case [source contract template](../../../use_cases/_template/specifications/source_contract.md)
captures authoring intent, potentially derived from Docling evidence.
It declares schemas, keys and extra rules so reviewed metadata can be
generated and approved. Once running, that approved metadata is the authority;
the authored Markdown is not a competing live configuration.

Changing the connector does not create another Bronze-to-Silver path.
All supported connectors land into the same governed downstream contract
and reuse inherited technical DQ and shared processing controls.
