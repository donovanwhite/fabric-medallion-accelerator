SET NOCOUNT ON;
SET XACT_ABORT ON;
GO
IF SCHEMA_ID(N'cfg') IS NULL EXEC(N'CREATE SCHEMA cfg AUTHORIZATION dbo');
IF SCHEMA_ID(N'ctl') IS NULL EXEC(N'CREATE SCHEMA ctl AUTHORIZATION dbo');
GO
IF OBJECT_ID(N'cfg.configuration_version', N'U') IS NULL
CREATE TABLE cfg.configuration_version (
    version_id varchar(128) NOT NULL PRIMARY KEY,
    definition_json nvarchar(max) NOT NULL,
    definition_sha256 char(64) NOT NULL,
    owner_name nvarchar(128) NOT NULL,
    lifecycle_state varchar(16) NOT NULL DEFAULT 'DRAFT',
    created_utc datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    created_by nvarchar(256) NOT NULL DEFAULT ORIGINAL_LOGIN(),
    validator_sha256 char(64) NULL,
    validation_json nvarchar(max) NULL,
    validated_utc datetime2(6) NULL,
    validated_by nvarchar(256) NULL,
    approved_utc datetime2(6) NULL,
    approved_by nvarchar(256) NULL,
    CONSTRAINT ck_cfg_definition_json CHECK (ISJSON(definition_json) = 1),
    CONSTRAINT ck_cfg_lifecycle CHECK (
        lifecycle_state IN ('DRAFT', 'VALIDATED', 'APPROVED', 'RETIRED')
    )
);
GO
IF OBJECT_ID(N'cfg.connection_ref', N'U') IS NULL
CREATE TABLE cfg.connection_ref (
    version_id varchar(128) NOT NULL,
    connection_id varchar(128) NOT NULL,
    display_name nvarchar(128) NOT NULL,
    connector_type varchar(32) NOT NULL,
    authentication_type varchar(64) NOT NULL,
    fabric_connection_id uniqueidentifier NULL,
    gateway_id uniqueidentifier NULL,
    CONSTRAINT pk_cfg_connection PRIMARY KEY (version_id, connection_id),
    CONSTRAINT fk_cfg_connection_version FOREIGN KEY (version_id)
        REFERENCES cfg.configuration_version(version_id)
);
GO
IF OBJECT_ID(N'cfg.source_feed', N'U') IS NULL
CREATE TABLE cfg.source_feed (
    version_id varchar(128) NOT NULL,
    feed_id varchar(128) NOT NULL,
    source_code varchar(63) NOT NULL,
    source_instance varchar(128) NOT NULL,
    source_type varchar(16) NOT NULL,
    description_slug varchar(63) NOT NULL,
    description nvarchar(2000) NOT NULL,
    connection_id varchar(128) NOT NULL,
    silver_schema varchar(128) NOT NULL,
    quarantine_schema varchar(128) NOT NULL,
    authority_policy varchar(16) NOT NULL,
    enabled bit NOT NULL,
    legacy_contract_hash char(64) NULL,
    legacy_bronze_root varchar(512) NULL,
    source_workspace_id uniqueidentifier NULL,
    source_artifact_id uniqueidentifier NULL,
    CONSTRAINT pk_cfg_feed PRIMARY KEY (version_id, feed_id),
    CONSTRAINT fk_cfg_feed_connection FOREIGN KEY (version_id, connection_id)
        REFERENCES cfg.connection_ref(version_id, connection_id),
    CONSTRAINT ck_cfg_feed_transport CHECK (source_type IN ('sql', 'ftp', 'sftp')),
    CONSTRAINT ck_cfg_feed_authority CHECK (
        authority_policy IN ('authoritative', 'supplemental')
    )
);
GO
IF COL_LENGTH(N'cfg.source_feed', N'source_workspace_id') IS NULL
    ALTER TABLE cfg.source_feed ADD source_workspace_id uniqueidentifier NULL;
IF COL_LENGTH(N'cfg.source_feed', N'source_artifact_id') IS NULL
    ALTER TABLE cfg.source_feed ADD source_artifact_id uniqueidentifier NULL;
GO
IF OBJECT_ID(N'cfg.worker', N'U') IS NULL
CREATE TABLE cfg.worker (
    version_id varchar(128) NOT NULL,
    worker_id varchar(128) NOT NULL,
    notebook_name nvarchar(128) NOT NULL,
    lakehouse_name nvarchar(128) NOT NULL,
    code_sha256 char(64) NOT NULL,
    CONSTRAINT pk_cfg_worker PRIMARY KEY (version_id, worker_id),
    CONSTRAINT fk_cfg_worker_version FOREIGN KEY (version_id)
        REFERENCES cfg.configuration_version(version_id)
);
GO
IF OBJECT_ID(N'cfg.data_object', N'U') IS NULL
CREATE TABLE cfg.data_object (
    version_id varchar(128) NOT NULL,
    object_id varchar(128) NOT NULL,
    feed_id varchar(128) NULL,
    layer varchar(8) NOT NULL,
    object_name nvarchar(128) NOT NULL,
    target_schema nvarchar(128) NULL,
    lakehouse_name nvarchar(128) NOT NULL,
    materialization varchar(32) NOT NULL,
    enabled bit NOT NULL,
    execution_priority int NOT NULL,
    business_keys_json nvarchar(max) NOT NULL,
    contract_json nvarchar(max) NOT NULL,
    worker_id varchar(128) NULL,
    required_capabilities_json nvarchar(max) NOT NULL,
    existing_target bit NOT NULL,
    CONSTRAINT pk_cfg_object PRIMARY KEY (version_id, object_id),
    CONSTRAINT fk_cfg_object_version FOREIGN KEY (version_id)
        REFERENCES cfg.configuration_version(version_id),
    CONSTRAINT fk_cfg_object_feed FOREIGN KEY (version_id, feed_id)
        REFERENCES cfg.source_feed(version_id, feed_id),
    CONSTRAINT fk_cfg_object_worker FOREIGN KEY (version_id, worker_id)
        REFERENCES cfg.worker(version_id, worker_id),
    CONSTRAINT ck_cfg_object_layer CHECK (layer IN ('bronze', 'silver', 'gold')),
    CONSTRAINT ck_cfg_object_priority CHECK (execution_priority >= 0),
    CONSTRAINT ck_cfg_object_contract CHECK (ISJSON(contract_json) = 1),
    CONSTRAINT ck_cfg_object_keys CHECK (ISJSON(business_keys_json) = 1),
    CONSTRAINT ck_cfg_object_capabilities CHECK (ISJSON(required_capabilities_json) = 1)
);
GO
IF OBJECT_ID(N'cfg.object_column', N'U') IS NULL
CREATE TABLE cfg.object_column (
    version_id varchar(128) NOT NULL,
    object_id varchar(128) NOT NULL,
    column_ordinal int NOT NULL,
    column_name nvarchar(128) NOT NULL,
    data_type varchar(32) NOT NULL,
    nullable bit NOT NULL,
    definition_json nvarchar(max) NOT NULL,
    CONSTRAINT pk_cfg_column PRIMARY KEY (version_id, object_id, column_ordinal),
    CONSTRAINT uq_cfg_column UNIQUE (version_id, object_id, column_name),
    CONSTRAINT fk_cfg_column_object FOREIGN KEY (version_id, object_id)
        REFERENCES cfg.data_object(version_id, object_id),
    CONSTRAINT ck_cfg_column_ordinal CHECK (column_ordinal > 0),
    CONSTRAINT ck_cfg_column_json CHECK (ISJSON(definition_json) = 1)
);
GO
IF OBJECT_ID(N'cfg.object_dependency', N'U') IS NULL
CREATE TABLE cfg.object_dependency (
    version_id varchar(128) NOT NULL,
    upstream_id varchar(128) NOT NULL,
    downstream_id varchar(128) NOT NULL,
    readiness_condition varchar(32) NOT NULL,
    CONSTRAINT pk_cfg_dependency PRIMARY KEY (version_id, upstream_id, downstream_id),
    CONSTRAINT fk_cfg_dependency_upstream FOREIGN KEY (version_id, upstream_id)
        REFERENCES cfg.data_object(version_id, object_id),
    CONSTRAINT fk_cfg_dependency_downstream FOREIGN KEY (version_id, downstream_id)
        REFERENCES cfg.data_object(version_id, object_id),
    CONSTRAINT ck_cfg_dependency_condition CHECK (
        readiness_condition IN ('succeeded', 'source_committed', 'facts_ready')
    ),
    CONSTRAINT ck_cfg_dependency_self CHECK (upstream_id <> downstream_id)
);
GO
IF OBJECT_ID(N'cfg.dq_rule', N'U') IS NULL
CREATE TABLE cfg.dq_rule (
    version_id varchar(128) NOT NULL,
    rule_id varchar(128) NOT NULL,
    object_id varchar(128) NOT NULL,
    handler varchar(128) NOT NULL,
    rule_version varchar(128) NOT NULL,
    severity varchar(16) NOT NULL,
    failure_action varchar(16) NOT NULL,
    parameters_json nvarchar(max) NOT NULL,
    CONSTRAINT pk_cfg_rule PRIMARY KEY (version_id, rule_id),
    CONSTRAINT fk_cfg_rule_object FOREIGN KEY (version_id, object_id)
        REFERENCES cfg.data_object(version_id, object_id),
    CONSTRAINT ck_cfg_rule_severity CHECK (severity IN ('error', 'warning')),
    CONSTRAINT ck_cfg_rule_action CHECK (
        failure_action IN ('fail', 'quarantine', 'warn')
    ),
    CONSTRAINT ck_cfg_rule_parameters CHECK (ISJSON(parameters_json) = 1)
);
GO
IF OBJECT_ID(N'cfg.constraint_definition', N'U') IS NULL
CREATE TABLE cfg.constraint_definition (
    version_id varchar(128) NOT NULL,
    constraint_id varchar(128) NOT NULL,
    object_id varchar(128) NOT NULL,
    constraint_kind varchar(32) NOT NULL,
    enforcement varchar(16) NOT NULL,
    definition_json nvarchar(max) NOT NULL,
    CONSTRAINT pk_cfg_constraint PRIMARY KEY (version_id, constraint_id),
    CONSTRAINT fk_cfg_constraint_object FOREIGN KEY (version_id, object_id)
        REFERENCES cfg.data_object(version_id, object_id),
    CONSTRAINT ck_cfg_constraint_kind CHECK (
        constraint_kind IN ('check', 'unique', 'relationship', 'reconciliation')
    ),
    CONSTRAINT ck_cfg_constraint_enforcement CHECK (enforcement IN ('runtime', 'engine')),
    CONSTRAINT ck_cfg_constraint_json CHECK (ISJSON(definition_json) = 1)
);
GO
IF OBJECT_ID(N'cfg.transformation', N'U') IS NULL
CREATE TABLE cfg.transformation (
    version_id varchar(128) NOT NULL,
    transformation_id varchar(128) NOT NULL,
    object_id varchar(128) NOT NULL,
    transformation_kind varchar(16) NOT NULL,
    transformation_version varchar(128) NOT NULL,
    definition_json nvarchar(max) NOT NULL,
    definition_sha256 char(64) NOT NULL,
    CONSTRAINT pk_cfg_transformation PRIMARY KEY (version_id, transformation_id),
    CONSTRAINT fk_cfg_transformation_object FOREIGN KEY (version_id, object_id)
        REFERENCES cfg.data_object(version_id, object_id),
    CONSTRAINT ck_cfg_transformation_kind CHECK (
        transformation_kind IN ('pattern', 'sql', 'notebook')
    ),
    CONSTRAINT ck_cfg_transformation_json CHECK (ISJSON(definition_json) = 1)
);
GO
IF OBJECT_ID(N'cfg.storage_layout', N'U') IS NULL
CREATE TABLE cfg.storage_layout (
    version_id varchar(128) NOT NULL,
    object_id varchar(128) NOT NULL,
    layout_strategy varchar(16) NOT NULL,
    partition_columns_json nvarchar(max) NOT NULL,
    cluster_by_columns_json nvarchar(max) NOT NULL,
    CONSTRAINT pk_cfg_layout PRIMARY KEY (version_id, object_id),
    CONSTRAINT fk_cfg_layout_object FOREIGN KEY (version_id, object_id)
        REFERENCES cfg.data_object(version_id, object_id),
    CONSTRAINT ck_cfg_layout_flag CHECK (layout_strategy IN ('partitioned', 'liquid')),
    CONSTRAINT ck_cfg_layout_columns CHECK (
        ISJSON(partition_columns_json) = 1 AND ISJSON(cluster_by_columns_json) = 1
        AND (
            (layout_strategy = 'partitioned'
             AND partition_columns_json <> N'[]' AND cluster_by_columns_json = N'[]')
            OR (layout_strategy = 'liquid'
                AND partition_columns_json = N'[]' AND cluster_by_columns_json <> N'[]')
        )
    )
);
GO
IF OBJECT_ID(N'cfg.capability', N'U') IS NULL
CREATE TABLE cfg.capability (
    version_id varchar(128) NOT NULL,
    capability_id varchar(128) NOT NULL,
    verified bit NOT NULL,
    evidence_sha256 char(64) NULL,
    CONSTRAINT pk_cfg_capability PRIMARY KEY (version_id, capability_id),
    CONSTRAINT fk_cfg_capability_version FOREIGN KEY (version_id)
        REFERENCES cfg.configuration_version(version_id),
    CONSTRAINT ck_cfg_capability_evidence CHECK (verified = 0 OR evidence_sha256 IS NOT NULL)
);
GO
IF OBJECT_ID(N'cfg.active_configuration', N'U') IS NULL
CREATE TABLE cfg.active_configuration (
    singleton_id tinyint NOT NULL PRIMARY KEY,
    version_id varchar(128) NOT NULL,
    activated_utc datetime2(6) NOT NULL,
    CONSTRAINT ck_cfg_active_singleton CHECK (singleton_id = 1),
    CONSTRAINT fk_cfg_active_version FOREIGN KEY (version_id)
        REFERENCES cfg.configuration_version(version_id)
);
GO
IF OBJECT_ID(N'cfg.activation_history', N'U') IS NULL
CREATE TABLE cfg.activation_history (
    activation_id bigint IDENTITY NOT NULL PRIMARY KEY,
    previous_version_id varchar(128) NULL,
    version_id varchar(128) NOT NULL,
    definition_sha256 char(64) NOT NULL,
    activated_utc datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    activated_by nvarchar(256) NOT NULL DEFAULT ORIGINAL_LOGIN(),
    CONSTRAINT fk_cfg_activation_version FOREIGN KEY (version_id)
        REFERENCES cfg.configuration_version(version_id)
);
GO
IF OBJECT_ID(N'ctl.run', N'U') IS NULL
CREATE TABLE ctl.[run] (
    run_id varchar(128) NOT NULL PRIMARY KEY,
    version_id varchar(128) NOT NULL,
    definition_sha256 char(64) NOT NULL,
    selection_sha256 char(64) NOT NULL,
    snapshot_sha256 char(64) NOT NULL,
    snapshot_json nvarchar(max) NOT NULL,
    status varchar(32) NOT NULL,
    observed_utc datetime2(6) NOT NULL,
    created_utc datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_ctl_run_version FOREIGN KEY (version_id)
        REFERENCES cfg.configuration_version(version_id),
    CONSTRAINT ck_ctl_run_snapshot CHECK (ISJSON(snapshot_json) = 1)
);
GO
IF OBJECT_ID(N'ctl.run_task', N'U') IS NULL
CREATE TABLE ctl.run_task (
    run_id varchar(128) NOT NULL,
    task_id char(64) NOT NULL,
    object_id varchar(128) NOT NULL,
    task_ordinal int NOT NULL,
    task_json nvarchar(max) NOT NULL,
    status varchar(32) NOT NULL,
    CONSTRAINT pk_ctl_task PRIMARY KEY (run_id, task_id),
    CONSTRAINT uq_ctl_task_order UNIQUE (run_id, task_ordinal),
    CONSTRAINT fk_ctl_task_run FOREIGN KEY (run_id) REFERENCES ctl.[run](run_id),
    CONSTRAINT ck_ctl_task_json CHECK (ISJSON(task_json) = 1)
);
GO
IF OBJECT_ID(N'ctl.task_attempt', N'U') IS NULL
CREATE TABLE ctl.task_attempt (
    run_id varchar(128) NOT NULL,
    task_id char(64) NOT NULL,
    attempt_id uniqueidentifier NOT NULL,
    job_id varchar(128) NULL,
    job_location nvarchar(2000) NULL,
    fence bigint NOT NULL,
    source_fence bigint NULL,
    submission_evidence_sha256 char(64) NULL,
    observed_job_status varchar(32) NULL,
    job_evidence_sha256 char(64) NULL,
    status varchar(32) NOT NULL,
    result_sha256 char(64) NULL,
    started_utc datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    completed_utc datetime2(6) NULL,
    CONSTRAINT pk_ctl_attempt PRIMARY KEY (run_id, task_id, attempt_id),
    CONSTRAINT fk_ctl_attempt_task FOREIGN KEY (run_id, task_id)
        REFERENCES ctl.run_task(run_id, task_id)
);
GO
IF COL_LENGTH(N'ctl.task_attempt', N'source_fence') IS NULL
    ALTER TABLE ctl.task_attempt ADD source_fence bigint NULL;
IF COL_LENGTH(N'ctl.task_attempt', N'submission_evidence_sha256') IS NULL
    ALTER TABLE ctl.task_attempt ADD submission_evidence_sha256 char(64) NULL;
IF COL_LENGTH(N'ctl.task_attempt', N'observed_job_status') IS NULL
    ALTER TABLE ctl.task_attempt ADD observed_job_status varchar(32) NULL;
IF COL_LENGTH(N'ctl.task_attempt', N'job_evidence_sha256') IS NULL
    ALTER TABLE ctl.task_attempt ADD job_evidence_sha256 char(64) NULL;
GO
IF OBJECT_ID(N'ctl.target_lease', N'U') IS NULL
CREATE TABLE ctl.target_lease (
    target_key char(64) NOT NULL PRIMARY KEY,
    run_id varchar(128) NOT NULL,
    fence bigint NOT NULL,
    expires_utc datetime2(6) NOT NULL,
    last_job_id varchar(128) NULL,
    reconciliation_required bit NOT NULL DEFAULT 0,
    released bit NOT NULL DEFAULT 0,
    release_evidence_sha256 char(64) NULL,
    CONSTRAINT fk_ctl_lease_run FOREIGN KEY (run_id) REFERENCES ctl.[run](run_id)
);
GO
IF COL_LENGTH(N'ctl.target_lease', N'released') IS NULL
    ALTER TABLE ctl.target_lease ADD released bit NOT NULL DEFAULT 0;
IF COL_LENGTH(N'ctl.target_lease', N'release_evidence_sha256') IS NULL
    ALTER TABLE ctl.target_lease ADD release_evidence_sha256 char(64) NULL;
GO
IF OBJECT_ID(N'ctl.task_result_inbox', N'U') IS NULL
CREATE TABLE ctl.task_result_inbox (
    result_id char(64) NOT NULL PRIMARY KEY,
    run_id varchar(128) NOT NULL,
    task_id char(64) NOT NULL,
    attempt_id uniqueidentifier NOT NULL,
    fence bigint NOT NULL,
    result_sha256 char(64) NOT NULL,
    result_json nvarchar(max) NOT NULL,
    received_utc datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    reconciled_utc datetime2(6) NULL,
    CONSTRAINT fk_ctl_result_attempt FOREIGN KEY (run_id, task_id, attempt_id)
        REFERENCES ctl.task_attempt(run_id, task_id, attempt_id),
    CONSTRAINT ck_ctl_result_json CHECK (ISJSON(result_json) = 1)
);
GO
IF DATABASE_PRINCIPAL_ID(N'metadata_reader') IS NULL CREATE ROLE metadata_reader;
IF DATABASE_PRINCIPAL_ID(N'metadata_editor') IS NULL CREATE ROLE metadata_editor;
IF DATABASE_PRINCIPAL_ID(N'metadata_validator') IS NULL CREATE ROLE metadata_validator;
IF DATABASE_PRINCIPAL_ID(N'config_approver') IS NULL CREATE ROLE config_approver;
IF DATABASE_PRINCIPAL_ID(N'runtime_executor') IS NULL CREATE ROLE runtime_executor;
GO
GRANT SELECT ON SCHEMA::cfg TO metadata_reader;
GRANT SELECT ON SCHEMA::cfg TO metadata_editor;
GRANT SELECT ON SCHEMA::cfg TO metadata_validator;
GRANT SELECT ON SCHEMA::cfg TO config_approver;
DENY INSERT, UPDATE, DELETE ON SCHEMA::cfg TO metadata_editor;
DENY INSERT, UPDATE, DELETE ON SCHEMA::cfg TO metadata_validator;
DENY INSERT, UPDATE, DELETE ON SCHEMA::cfg TO config_approver;
DENY INSERT, UPDATE, DELETE ON SCHEMA::ctl TO runtime_executor;
GO
