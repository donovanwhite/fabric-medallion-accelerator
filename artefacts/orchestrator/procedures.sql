CREATE OR ALTER PROCEDURE cfg.usp_register_version
    @version_id varchar(128),
    @definition_json nvarchar(max),
    @definition_sha256 char(64)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF COALESCE(ISJSON(@definition_json), 0) <> 1
        THROW 51000, 'A complete JSON metadata definition is required.', 1;
    IF @definition_json COLLATE Latin1_General_100_BIN2 LIKE N'%[^ -~]%'
        THROW 51001, 'Use the canonical ASCII-escaped metadata serialization.', 1;
    IF @version_id IS NULL
       OR COALESCE(JSON_VALUE(@definition_json, '$.version_id'), '') <> @version_id
       OR COALESCE(JSON_VALUE(@definition_json, '$.schema_version'), '') <> '1'
        THROW 51002, 'Metadata version identity or schema version differs.', 1;
    IF @definition_sha256 IS NULL OR LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
        CONVERT(varchar(max), @definition_json)), 2)) <> @definition_sha256
        THROW 51003, 'Metadata definition hash mismatch.', 1;

    DECLARE @nodes TABLE (
        node_id int IDENTITY PRIMARY KEY,
        node_json nvarchar(max) NOT NULL,
        processed bit NOT NULL DEFAULT 0
    );
    INSERT @nodes(node_json) VALUES (@definition_json);
    DECLARE @node_id int, @node_json nvarchar(max), @node_count int = 0;
    WHILE EXISTS (SELECT 1 FROM @nodes WHERE processed = 0)
    BEGIN
        SELECT TOP (1) @node_id = node_id, @node_json = node_json
        FROM @nodes WHERE processed = 0 ORDER BY node_id;
        UPDATE @nodes SET processed = 1 WHERE node_id = @node_id;
        SET @node_count += 1;
        IF @node_count > 100000
            THROW 51004, 'Metadata definition exceeds the bounded validation size.', 1;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@node_json)
            WHERE LOWER([key]) IN (
                'password', 'accesstoken', 'access_token', 'refreshtoken',
                'refresh_token', 'clientsecret', 'client_secret', 'privatekey',
                'private_key', 'connectionstring', 'connection_string'
            )
        )
            THROW 51005, 'Credentials are prohibited in metadata definitions.', 1;
        INSERT @nodes(node_json)
        SELECT [value] FROM OPENJSON(@node_json) WHERE [type] IN (4, 5);
    END;

    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @existing_hash char(64);
        SELECT @existing_hash = definition_sha256
        FROM cfg.configuration_version WITH (UPDLOCK, HOLDLOCK)
        WHERE version_id = @version_id;
        IF @existing_hash IS NOT NULL
        BEGIN
            IF @existing_hash <> @definition_sha256
                THROW 51006, 'An existing version cannot be rebound to changed metadata.', 1;
            COMMIT;
            SELECT version_id, definition_sha256, lifecycle_state
            FROM cfg.configuration_version WHERE version_id = @version_id;
            RETURN;
        END;

        INSERT cfg.configuration_version (
            version_id, definition_json, definition_sha256, owner_name
        )
        VALUES (
            @version_id, @definition_json, @definition_sha256,
            JSON_VALUE(@definition_json, '$.owner')
        );

        INSERT cfg.connection_ref (
            version_id, connection_id, display_name, connector_type,
            authentication_type, fabric_connection_id, gateway_id
        )
        SELECT @version_id, id, name, connector_type, authentication_type,
               fabric_connection_id, gateway_id
        FROM OPENJSON(@definition_json, '$.connections')
        WITH (
            id varchar(128), name nvarchar(128), connector_type varchar(32),
            authentication_type varchar(64),
            fabric_connection_id uniqueidentifier, gateway_id uniqueidentifier
        );

        INSERT cfg.source_feed (
            version_id, feed_id, source_code, source_instance, source_type,
            description_slug, description, connection_id, silver_schema,
            quarantine_schema, authority_policy, enabled,
            legacy_contract_hash, legacy_bronze_root, source_workspace_id, source_artifact_id
        )
        SELECT @version_id, id, source_code, source_instance, source_type,
               description_slug, description, connection_id, silver_schema,
               quarantine_schema, authority, enabled,
               legacy_contract_hash, legacy_bronze_root, source_workspace_id, source_artifact_id
        FROM OPENJSON(@definition_json, '$.feeds')
        WITH (
            id varchar(128), source_code varchar(63), source_instance varchar(128),
            source_type varchar(16), description_slug varchar(63),
            description nvarchar(2000), connection_id varchar(128),
            silver_schema varchar(128), quarantine_schema varchar(128),
            authority varchar(16), enabled bit, legacy_contract_hash char(64),
            legacy_bronze_root varchar(512),
            source_workspace_id uniqueidentifier '$.geometry.workspace_id',
            source_artifact_id uniqueidentifier '$.geometry.artifact_id'
        );

        INSERT cfg.worker (
            version_id, worker_id, notebook_name, lakehouse_name, code_sha256
        )
        SELECT @version_id, id, notebook_name, lakehouse, code_sha256
        FROM OPENJSON(@definition_json, '$.workers')
        WITH (
            id varchar(128), notebook_name nvarchar(128),
            lakehouse nvarchar(128), code_sha256 char(64)
        );

        INSERT cfg.data_object (
            version_id, object_id, feed_id, layer, object_name, target_schema,
            lakehouse_name, materialization, enabled, execution_priority,
            business_keys_json, contract_json, worker_id,
            required_capabilities_json, existing_target
        )
        SELECT @version_id, id, feed_id, layer, name, target_schema,
               lakehouse, materialization, enabled, execution_priority,
               business_keys, contract, worker_id, required_capabilities,
               existing_target
        FROM OPENJSON(@definition_json, '$.objects')
        WITH (
            id varchar(128), feed_id varchar(128), layer varchar(8),
            name nvarchar(128), target_schema nvarchar(128),
            lakehouse nvarchar(128), materialization varchar(32), enabled bit,
            execution_priority int, business_keys nvarchar(max) AS JSON,
            contract nvarchar(max) AS JSON, worker_id varchar(128),
            required_capabilities nvarchar(max) AS JSON, existing_target bit
        );

        INSERT cfg.object_column (
            version_id, object_id, column_ordinal, column_name,
            data_type, nullable, definition_json
        )
        SELECT @version_id, JSON_VALUE(obj.[value], '$.id'),
               CONVERT(int, col.[key]) + 1,
               JSON_VALUE(col.[value], '$.name'),
               JSON_VALUE(col.[value], '$.data_type'),
               CONVERT(bit, CASE JSON_VALUE(col.[value], '$.nullable')
                   WHEN 'true' THEN 1 WHEN 'false' THEN 0 END),
               col.[value]
        FROM OPENJSON(@definition_json, '$.objects') AS obj
        CROSS APPLY OPENJSON(obj.[value], '$.columns') AS col;

        INSERT cfg.object_dependency (
            version_id, upstream_id, downstream_id, readiness_condition
        )
        SELECT @version_id, upstream_id, downstream_id, condition
        FROM OPENJSON(@definition_json, '$.dependencies')
        WITH (
            upstream_id varchar(128), downstream_id varchar(128),
            condition varchar(32)
        );

        INSERT cfg.dq_rule (
            version_id, rule_id, object_id, handler, rule_version,
            severity, failure_action, parameters_json
        )
        SELECT @version_id, id, object_id, handler, version, severity, action, parameters
        FROM OPENJSON(@definition_json, '$.rules')
        WITH (
            id varchar(128), object_id varchar(128), handler varchar(128),
            version varchar(128), severity varchar(16), action varchar(16),
            parameters nvarchar(max) AS JSON
        );

        INSERT cfg.constraint_definition (
            version_id, constraint_id, object_id, constraint_kind,
            enforcement, definition_json
        )
        SELECT @version_id, id, object_id, kind, enforcement, definition
        FROM OPENJSON(@definition_json, '$.constraints')
        WITH (
            id varchar(128), object_id varchar(128), kind varchar(32),
            enforcement varchar(16), definition nvarchar(max) AS JSON
        );

        INSERT cfg.transformation (
            version_id, transformation_id, object_id, transformation_kind,
            transformation_version, definition_json, definition_sha256
        )
        SELECT @version_id, id, object_id, kind, version, definition, definition_sha256
        FROM OPENJSON(@definition_json, '$.transformations')
        WITH (
            id varchar(128), object_id varchar(128), kind varchar(16),
            version varchar(128), definition nvarchar(max) AS JSON,
            definition_sha256 char(64)
        );

        INSERT cfg.storage_layout (
            version_id, object_id, layout_strategy,
            partition_columns_json, cluster_by_columns_json
        )
        SELECT @version_id, JSON_VALUE(obj.[value], '$.id'),
               layout.layout_strategy, layout.partition_columns, layout.cluster_by_columns
        FROM OPENJSON(@definition_json, '$.objects') AS obj
        CROSS APPLY OPENJSON(obj.[value], '$.layout')
        WITH (
            layout_strategy varchar(16),
            partition_columns nvarchar(max) AS JSON,
            cluster_by_columns nvarchar(max) AS JSON
        ) AS layout;

        INSERT cfg.capability (version_id, capability_id, verified, evidence_sha256)
        SELECT @version_id, id, verified, evidence_sha256
        FROM OPENJSON(@definition_json, '$.capabilities')
        WITH (id varchar(128), verified bit, evidence_sha256 char(64));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
    SELECT version_id, definition_sha256, lifecycle_state
    FROM cfg.configuration_version WHERE version_id = @version_id;
END;
GO
CREATE OR ALTER PROCEDURE cfg.usp_validate_version
    @version_id varchar(128),
    @expected_hash char(64),
    @validator_hash char(64),
    @validation_json nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF COALESCE(ISJSON(@validation_json), 0) <> 1
        THROW 51010, 'Successful attributable validator evidence is required.', 1;
    IF NOT EXISTS (
        SELECT 1 FROM OPENJSON(@validation_json)
        WHERE [key] = 'passed' AND [type] = 3 AND [value] = 'true'
    )
        THROW 51010, 'Successful attributable validator evidence is required.', 1;
    IF @validator_hash IS NULL OR @validator_hash COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
        THROW 51011, 'An explicit validator implementation hash is required.', 1;
    UPDATE cfg.configuration_version
    SET lifecycle_state = 'VALIDATED', validator_sha256 = @validator_hash,
        validation_json = @validation_json, validated_utc = SYSUTCDATETIME(),
        validated_by = ORIGINAL_LOGIN()
    WHERE version_id = @version_id AND definition_sha256 = @expected_hash
      AND lifecycle_state = 'DRAFT';
    IF @@ROWCOUNT <> 1
        THROW 51012, 'Validation requires the exact unmodified draft version.', 1;
END;
GO
CREATE OR ALTER PROCEDURE cfg.usp_approve_version
    @version_id varchar(128),
    @expected_hash char(64)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    BEGIN TRY
        IF NOT EXISTS (
            SELECT 1 FROM cfg.configuration_version WITH (UPDLOCK, HOLDLOCK)
            WHERE version_id = @version_id AND definition_sha256 = @expected_hash
              AND lifecycle_state = 'VALIDATED'
        )
            THROW 51020, 'Approval requires the exact validated version.', 1;
        IF EXISTS (
            SELECT 1 FROM cfg.data_object AS obj
            JOIN cfg.source_feed AS feed
              ON feed.version_id = obj.version_id AND feed.feed_id = obj.feed_id
            JOIN cfg.connection_ref AS conn
              ON conn.version_id = feed.version_id AND conn.connection_id = feed.connection_id
            WHERE obj.version_id = @version_id AND obj.enabled = 1
              AND feed.enabled = 1 AND conn.fabric_connection_id IS NULL
        )
            THROW 51021, 'Enabled feed connections must be explicitly bound.', 1;
        IF EXISTS (
            SELECT 1 FROM cfg.data_object AS obj
            JOIN cfg.source_feed AS feed
              ON feed.version_id = obj.version_id AND feed.feed_id = obj.feed_id
            JOIN cfg.connection_ref AS conn
              ON conn.version_id = feed.version_id AND conn.connection_id = feed.connection_id
            WHERE obj.version_id = @version_id AND obj.enabled = 1 AND feed.enabled = 1
              AND obj.layer = 'bronze' AND feed.source_type = 'sql'
              AND (conn.connector_type NOT IN ('FabricSql', 'SqlServer')
                   OR (conn.connector_type = 'FabricSql' AND (
                       conn.authentication_type <> 'OAuth2'
                       OR feed.source_workspace_id IS NULL OR feed.source_artifact_id IS NULL))
                   OR (conn.connector_type = 'SqlServer' AND (
                       conn.authentication_type <> 'Basic'
                       OR feed.source_workspace_id IS NOT NULL OR feed.source_artifact_id IS NOT NULL))
                   OR JSON_VALUE(obj.contract_json, '$.projection_sql') IS NULL))
            THROW 51021, 'SQL connector, authentication, projection or geometry is not ready.', 1;
        IF EXISTS (
            SELECT 1 FROM cfg.data_object AS obj
            JOIN cfg.source_feed AS feed
              ON feed.version_id = obj.version_id AND feed.feed_id = obj.feed_id
            JOIN cfg.connection_ref AS conn
              ON conn.version_id = feed.version_id AND conn.connection_id = feed.connection_id
            LEFT JOIN cfg.capability AS cap
              ON cap.version_id = obj.version_id AND cap.capability_id = 'sql-server-copy-basic'
            WHERE obj.version_id = @version_id AND obj.enabled = 1 AND feed.enabled = 1
              AND obj.layer = 'bronze' AND conn.connector_type = 'SqlServer'
              AND (cap.verified IS NULL OR cap.verified = 0 OR cap.evidence_sha256 IS NULL
                   OR LEN(cap.evidence_sha256) <> 64
                   OR cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'))
            THROW 51022, 'SQL Server Copy requires verified connector/authentication evidence.', 1;
        IF EXISTS (
            SELECT 1 FROM cfg.data_object AS obj
            CROSS APPLY OPENJSON(obj.required_capabilities_json) AS needed
            LEFT JOIN cfg.capability AS cap
              ON cap.version_id = obj.version_id AND cap.capability_id = needed.[value]
            WHERE obj.version_id = @version_id AND obj.enabled = 1
              AND (cap.capability_id IS NULL OR cap.verified = 0
                   OR cap.evidence_sha256 IS NULL OR LEN(cap.evidence_sha256) <> 64
                   OR cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%')
        )
            THROW 51022, 'Enabled objects require verified capability evidence.', 1;
        IF EXISTS (
            SELECT 1 FROM cfg.data_object AS obj
            JOIN cfg.source_feed AS feed
              ON feed.version_id = obj.version_id AND feed.feed_id = obj.feed_id
            JOIN cfg.connection_ref AS conn
              ON conn.version_id = feed.version_id AND conn.connection_id = feed.connection_id
            WHERE obj.version_id = @version_id AND obj.enabled = 1 AND feed.enabled = 1
              AND obj.layer = 'bronze' AND feed.source_type IN ('ftp', 'sftp')
              AND (
                  (feed.source_type = 'ftp' AND (conn.connector_type <> 'Ftp'
                      OR conn.authentication_type NOT IN ('Basic', 'Anonymous')))
                  OR (feed.source_type = 'sftp' AND (conn.connector_type <> 'Sftp'
                      OR conn.authentication_type <> 'Basic'))
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'source_directory' AND [type] = 1)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'file_pattern' AND [type] = 1)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'recursive' AND [type] = 3)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'file_format' AND [type] = 1)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'arrival_policy' AND [type] = 5)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json, '$.arrival_policy')
                      WHERE [key] = 'kind' AND [type] = 1)
                  OR NULLIF(JSON_VALUE(obj.contract_json, '$.source_directory'), '') IS NULL
                  OR NULLIF(JSON_VALUE(obj.contract_json, '$.file_pattern'), '') IS NULL
                  OR COALESCE(JSON_VALUE(obj.contract_json, '$.recursive'), '') NOT IN ('true', 'false')
                  OR COALESCE(JSON_VALUE(obj.contract_json, '$.file_format'), '') COLLATE Latin1_General_100_BIN2 <> 'csv'
                  OR COALESCE(JSON_VALUE(obj.contract_json, '$.arrival_policy.kind'), '') COLLATE Latin1_General_100_BIN2 NOT IN ('marker', 'stable')
                  OR (JSON_VALUE(obj.contract_json, '$.arrival_policy.kind') = 'marker'
                      AND (NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json, '$.arrival_policy')
                               WHERE [key] = 'marker' AND [type] = 1)
                           OR NULLIF(JSON_VALUE(obj.contract_json, '$.arrival_policy.marker'), '') IS NULL))
                  OR (JSON_VALUE(obj.contract_json, '$.arrival_policy.kind') = 'stable' AND (
                      NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json, '$.arrival_policy')
                          WHERE [key] = 'minimum_age_seconds' AND [type] = 2)
                      OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json, '$.arrival_policy')
                          WHERE [key] = 'stable_for_seconds' AND [type] = 2)
                      OR COALESCE(TRY_CONVERT(int, JSON_VALUE(obj.contract_json, '$.arrival_policy.minimum_age_seconds')), 0) NOT BETWEEN 1 AND 604800
                      OR COALESCE(TRY_CONVERT(int, JSON_VALUE(obj.contract_json, '$.arrival_policy.stable_for_seconds')), 0) NOT BETWEEN 1 AND 604800))
                  OR NOT EXISTS (
                      SELECT 1 FROM cfg.capability AS cap
                      WHERE cap.version_id = obj.version_id
                        AND cap.capability_id = CONCAT(feed.source_type, '-copy-', LOWER(conn.authentication_type))
                        AND cap.verified = 1 AND cap.evidence_sha256 IS NOT NULL
                        AND LEN(cap.evidence_sha256) = 64
                        AND cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 NOT LIKE '%[^0-9a-f]%')
                  OR NOT EXISTS (
                      SELECT 1 FROM cfg.capability AS cap
                      WHERE cap.version_id = obj.version_id
                        AND cap.capability_id = CONCAT(feed.source_type, '-arrival-observer')
                        AND cap.verified = 1 AND cap.evidence_sha256 IS NOT NULL
                        AND LEN(cap.evidence_sha256) = 64
                        AND cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 NOT LIKE '%[^0-9a-f]%')
              )
        )
            THROW 51023, 'Enabled file sources require complete arrival contracts and verified connector and observer evidence.', 1;
        UPDATE cfg.configuration_version
        SET lifecycle_state = 'APPROVED', approved_utc = SYSUTCDATETIME(),
            approved_by = ORIGINAL_LOGIN()
        WHERE version_id = @version_id;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE cfg.usp_activate_version
    @version_id varchar(128),
    @expected_hash char(64),
    @expected_active_version varchar(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @current varchar(128);
        SELECT @current = version_id
        FROM cfg.active_configuration WITH (UPDLOCK, HOLDLOCK)
        WHERE singleton_id = 1;
        IF (@current <> @expected_active_version)
           OR (@current IS NULL AND @expected_active_version IS NOT NULL)
           OR (@current IS NOT NULL AND @expected_active_version IS NULL)
            THROW 51030, 'Active configuration changed; refresh the activation preview.', 1;
        IF NOT EXISTS (
            SELECT 1 FROM cfg.configuration_version WITH (UPDLOCK, HOLDLOCK)
            WHERE version_id = @version_id AND definition_sha256 = @expected_hash
              AND lifecycle_state = 'APPROVED'
        )
            THROW 51031, 'Only the exact approved configuration can be activated.', 1;
        IF @current = @version_id
        BEGIN
            COMMIT;
            RETURN;
        END;
        IF @current IS NULL
            INSERT cfg.active_configuration(singleton_id, version_id, activated_utc)
            VALUES (1, @version_id, SYSUTCDATETIME());
        ELSE
            UPDATE cfg.active_configuration
            SET version_id = @version_id, activated_utc = SYSUTCDATETIME()
            WHERE singleton_id = 1;
        INSERT cfg.activation_history(previous_version_id, version_id, definition_sha256)
        VALUES (@current, @version_id, @expected_hash);
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE cfg.usp_preview_version
    @version_id varchar(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SELECT version.version_id, version.definition_sha256, version.lifecycle_state,
           version.definition_json
    FROM cfg.configuration_version AS version
    WHERE version.version_id = COALESCE(
        @version_id,
        (SELECT version_id FROM cfg.active_configuration WHERE singleton_id = 1)
    );
END;
GO
GRANT EXECUTE ON OBJECT::cfg.usp_register_version TO metadata_editor;
GRANT EXECUTE ON OBJECT::cfg.usp_validate_version TO metadata_validator;
GRANT EXECUTE ON OBJECT::cfg.usp_approve_version TO config_approver;
GRANT EXECUTE ON OBJECT::cfg.usp_activate_version TO config_approver;
GRANT EXECUTE ON OBJECT::cfg.usp_preview_version TO metadata_reader;
GRANT EXECUTE ON OBJECT::cfg.usp_preview_version TO runtime_executor;
GO
CREATE OR ALTER PROCEDURE ctl.usp_prepare_run
    @run_id varchar(128),
    @version_id varchar(128),
    @expected_definition_sha256 char(64),
    @observed_utc varchar(32),
    @requested_objects_json nvarchar(max) = NULL,
    @expected_snapshot_sha256 char(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @instant datetime2(6) = TRY_CONVERT(datetime2(6), @observed_utc, 127);
    IF @run_id IS NULL OR LEN(@run_id) = 0
       OR @run_id COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_]%'
       OR @instant IS NULL OR RIGHT(@observed_utc, 1) <> 'Z'
        THROW 51100, 'A path-safe run identity and explicit UTC instant are required.', 1;
    IF @requested_objects_json IS NOT NULL AND (
        ISJSON(@requested_objects_json) <> 1
        OR LEFT(@requested_objects_json, 1) <> '['
        OR NOT EXISTS (SELECT 1 FROM OPENJSON(@requested_objects_json))
        OR EXISTS (SELECT 1 FROM OPENJSON(@requested_objects_json) WHERE [type] <> 1)
        OR EXISTS (SELECT [value] FROM OPENJSON(@requested_objects_json)
                   GROUP BY [value] HAVING COUNT(*) > 1)
    )
        THROW 51100, 'Requested object identities must be a nonempty distinct array.', 1;

    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @definition nvarchar(max), @definition_hash char(64),
                @existing_snapshot char(64), @selection nvarchar(max),
                @selection_hash char(64);
        SELECT @definition = definition_json, @definition_hash = definition_sha256
        FROM cfg.configuration_version WITH (HOLDLOCK)
        WHERE version_id = @version_id AND definition_sha256 = @expected_definition_sha256
          AND lifecycle_state = 'APPROVED';
        IF @definition IS NULL
            THROW 51101, 'Run preparation requires the exact approved configuration.', 1;

        DECLARE @roots TABLE (object_id varchar(128) NOT NULL PRIMARY KEY);
        IF @requested_objects_json IS NULL
            INSERT @roots SELECT object_id FROM cfg.data_object
            WHERE version_id = @version_id AND enabled = 1;
        ELSE
            INSERT @roots SELECT [value] FROM OPENJSON(@requested_objects_json);
        IF NOT EXISTS (SELECT 1 FROM @roots)
           OR EXISTS (
               SELECT 1 FROM @roots AS root LEFT JOIN cfg.data_object AS obj
                 ON obj.version_id = @version_id AND obj.object_id = root.object_id
               WHERE obj.object_id IS NULL OR obj.enabled = 0
           )
            THROW 51103, 'Requested objects are missing or disabled.', 1;
        SELECT @selection = N'[' + STRING_AGG(CONVERT(nvarchar(max),
            N'"' + object_id + N'"'), N',') WITHIN GROUP (ORDER BY object_id) + N']'
        FROM @roots;
        SET @selection_hash = LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
            CONVERT(varchar(max), @selection)), 2));
        DECLARE @selected TABLE (object_id varchar(128) NOT NULL PRIMARY KEY);
        INSERT @selected SELECT object_id FROM @roots;
        WHILE 1 = 1
        BEGIN
            INSERT @selected
            SELECT DISTINCT edge.upstream_id
            FROM cfg.object_dependency AS edge
            JOIN @selected AS child ON child.object_id = edge.downstream_id
            WHERE edge.version_id = @version_id
              AND NOT EXISTS (SELECT 1 FROM @selected AS parent
                              WHERE parent.object_id = edge.upstream_id);
            IF @@ROWCOUNT = 0 BREAK;
        END;
        IF EXISTS (
            SELECT 1 FROM @selected AS chosen JOIN cfg.data_object AS obj
              ON obj.version_id = @version_id AND obj.object_id = chosen.object_id
            LEFT JOIN cfg.source_feed AS feed
              ON feed.version_id = obj.version_id AND feed.feed_id = obj.feed_id
            LEFT JOIN cfg.connection_ref AS conn
              ON conn.version_id = feed.version_id AND conn.connection_id = feed.connection_id
            WHERE obj.enabled = 0 OR (obj.feed_id IS NOT NULL AND (
                feed.enabled = 0 OR feed.authority_policy <> 'authoritative'
                OR conn.fabric_connection_id IS NULL))
        )
            THROW 51103, 'Selected dependencies, feed authority or connections are not ready.', 1;
        IF EXISTS (
            SELECT 1 FROM @selected AS chosen JOIN cfg.data_object AS obj
              ON obj.version_id = @version_id AND obj.object_id = chosen.object_id
            JOIN cfg.source_feed AS feed
              ON feed.version_id = obj.version_id AND feed.feed_id = obj.feed_id
            JOIN cfg.connection_ref AS conn
              ON conn.version_id = feed.version_id AND conn.connection_id = feed.connection_id
            WHERE obj.layer = 'bronze' AND feed.source_type = 'sql'
              AND (conn.connector_type NOT IN ('FabricSql', 'SqlServer')
                   OR (conn.connector_type = 'FabricSql' AND (
                       conn.authentication_type <> 'OAuth2'
                       OR feed.source_workspace_id IS NULL OR feed.source_artifact_id IS NULL))
                   OR (conn.connector_type = 'SqlServer' AND (
                       conn.authentication_type <> 'Basic'
                       OR feed.source_workspace_id IS NOT NULL OR feed.source_artifact_id IS NOT NULL))
                   OR JSON_VALUE(obj.contract_json, '$.projection_sql') IS NULL))
            THROW 51103, 'SQL Copy requires a compatible connector, authentication, projection and geometry.', 1;
        IF EXISTS (
            SELECT 1 FROM @selected AS chosen JOIN cfg.data_object AS obj
              ON obj.version_id = @version_id AND obj.object_id = chosen.object_id
            JOIN cfg.source_feed AS feed
              ON feed.version_id = obj.version_id AND feed.feed_id = obj.feed_id
            JOIN cfg.connection_ref AS conn
              ON conn.version_id = feed.version_id AND conn.connection_id = feed.connection_id
            LEFT JOIN cfg.capability AS cap
              ON cap.version_id = obj.version_id AND cap.capability_id = 'sql-server-copy-basic'
            WHERE obj.layer = 'bronze' AND conn.connector_type = 'SqlServer'
              AND (cap.verified IS NULL OR cap.verified = 0 OR cap.evidence_sha256 IS NULL
                   OR LEN(cap.evidence_sha256) <> 64
                   OR cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'))
            THROW 51103, 'SQL Server Copy requires verified connector/authentication evidence.', 1;
        IF EXISTS (
            SELECT 1 FROM @selected AS chosen JOIN cfg.data_object AS obj
              ON obj.version_id = @version_id AND obj.object_id = chosen.object_id
            CROSS APPLY OPENJSON(obj.required_capabilities_json) AS needed
            LEFT JOIN cfg.capability AS cap
              ON cap.version_id = @version_id AND cap.capability_id = needed.[value]
            WHERE cap.capability_id IS NULL OR cap.verified = 0
               OR cap.evidence_sha256 IS NULL OR LEN(cap.evidence_sha256) <> 64
               OR cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
        )
            THROW 51103, 'Selected tasks require verified capability evidence.', 1;
        IF EXISTS (
            SELECT 1 FROM @selected AS chosen
            JOIN cfg.data_object AS obj
              ON obj.version_id = @version_id AND obj.object_id = chosen.object_id
            JOIN cfg.source_feed AS feed
              ON feed.version_id = obj.version_id AND feed.feed_id = obj.feed_id
            JOIN cfg.connection_ref AS conn
              ON conn.version_id = feed.version_id AND conn.connection_id = feed.connection_id
            WHERE obj.layer = 'bronze' AND feed.source_type IN ('ftp', 'sftp')
              AND (
                  (feed.source_type = 'ftp' AND (conn.connector_type <> 'Ftp'
                      OR conn.authentication_type NOT IN ('Basic', 'Anonymous')))
                  OR (feed.source_type = 'sftp' AND (conn.connector_type <> 'Sftp'
                      OR conn.authentication_type <> 'Basic'))
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'source_directory' AND [type] = 1)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'file_pattern' AND [type] = 1)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'recursive' AND [type] = 3)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'file_format' AND [type] = 1)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json)
                      WHERE [key] = 'arrival_policy' AND [type] = 5)
                  OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json, '$.arrival_policy')
                      WHERE [key] = 'kind' AND [type] = 1)
                  OR NULLIF(JSON_VALUE(obj.contract_json, '$.source_directory'), '') IS NULL
                  OR NULLIF(JSON_VALUE(obj.contract_json, '$.file_pattern'), '') IS NULL
                  OR COALESCE(JSON_VALUE(obj.contract_json, '$.recursive'), '') NOT IN ('true', 'false')
                  OR COALESCE(JSON_VALUE(obj.contract_json, '$.file_format'), '') COLLATE Latin1_General_100_BIN2 <> 'csv'
                  OR COALESCE(JSON_VALUE(obj.contract_json, '$.arrival_policy.kind'), '') COLLATE Latin1_General_100_BIN2 NOT IN ('marker', 'stable')
                  OR (JSON_VALUE(obj.contract_json, '$.arrival_policy.kind') = 'marker'
                      AND (NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json, '$.arrival_policy')
                               WHERE [key] = 'marker' AND [type] = 1)
                           OR NULLIF(JSON_VALUE(obj.contract_json, '$.arrival_policy.marker'), '') IS NULL))
                  OR (JSON_VALUE(obj.contract_json, '$.arrival_policy.kind') = 'stable' AND (
                      NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json, '$.arrival_policy')
                          WHERE [key] = 'minimum_age_seconds' AND [type] = 2)
                      OR NOT EXISTS (SELECT 1 FROM OPENJSON(obj.contract_json, '$.arrival_policy')
                          WHERE [key] = 'stable_for_seconds' AND [type] = 2)
                      OR COALESCE(TRY_CONVERT(int, JSON_VALUE(obj.contract_json, '$.arrival_policy.minimum_age_seconds')), 0) NOT BETWEEN 1 AND 604800
                      OR COALESCE(TRY_CONVERT(int, JSON_VALUE(obj.contract_json, '$.arrival_policy.stable_for_seconds')), 0) NOT BETWEEN 1 AND 604800))
                  OR NOT EXISTS (
                      SELECT 1 FROM cfg.capability AS cap
                      WHERE cap.version_id = obj.version_id
                        AND cap.capability_id = CONCAT(feed.source_type, '-copy-', LOWER(conn.authentication_type))
                        AND cap.verified = 1 AND cap.evidence_sha256 IS NOT NULL
                        AND LEN(cap.evidence_sha256) = 64
                        AND cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 NOT LIKE '%[^0-9a-f]%')
                  OR NOT EXISTS (
                      SELECT 1 FROM cfg.capability AS cap
                      WHERE cap.version_id = obj.version_id
                        AND cap.capability_id = CONCAT(feed.source_type, '-arrival-observer')
                        AND cap.verified = 1 AND cap.evidence_sha256 IS NOT NULL
                        AND LEN(cap.evidence_sha256) = 64
                        AND cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 NOT LIKE '%[^0-9a-f]%')
              )
        )
            THROW 51103, 'Selected file sources require complete arrival contracts and verified connector and observer evidence.', 1;
        IF EXISTS (
            SELECT 1 FROM @selected AS chosen JOIN cfg.data_object AS obj
              ON obj.version_id = @version_id AND obj.object_id = chosen.object_id
            WHERE obj.layer = 'silver' AND NOT EXISTS (
                SELECT 1 FROM OPENJSON(@definition, '$.extensions.hash_policies') AS policy
                WHERE policy.[key] COLLATE DATABASE_DEFAULT = obj.object_id AND policy.[type] = 5)
        )
            THROW 51103, 'Selected Silver tasks require stable business hash policies.', 1;

        SELECT @existing_snapshot = snapshot_sha256
        FROM ctl.[run] WITH (UPDLOCK, HOLDLOCK) WHERE run_id = @run_id;
        IF @existing_snapshot IS NOT NULL
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM ctl.[run] WHERE run_id = @run_id AND version_id = @version_id
                  AND definition_sha256 = @definition_hash
                  AND selection_sha256 = @selection_hash AND observed_utc = @instant
                  AND (@expected_snapshot_sha256 IS NULL
                       OR snapshot_sha256 = @expected_snapshot_sha256)
            )
                THROW 51104, 'A run identity cannot be rebound to changed inputs.', 1;
            COMMIT;
            SELECT run_id, version_id, snapshot_sha256, status
            FROM ctl.[run] WHERE run_id = @run_id;
            RETURN;
        END;

        DECLARE @ordered TABLE (
            ordinal int IDENTITY NOT NULL PRIMARY KEY,
            object_id varchar(128) NOT NULL UNIQUE,
            task_id char(64) NOT NULL,
            task_json nvarchar(max) NULL
        );
        DECLARE @object_id varchar(128), @task_id char(64), @identity_json nvarchar(max);
        WHILE (SELECT COUNT(*) FROM @ordered) < (SELECT COUNT(*) FROM @selected)
        BEGIN
            SET @object_id = NULL;
            SELECT TOP (1) @object_id = obj.object_id
            FROM @selected AS chosen JOIN cfg.data_object AS obj
              ON obj.version_id = @version_id AND obj.object_id = chosen.object_id
            WHERE NOT EXISTS (SELECT 1 FROM @ordered WHERE object_id = obj.object_id)
              AND NOT EXISTS (
                  SELECT 1 FROM cfg.object_dependency AS edge
                  WHERE edge.version_id = @version_id AND edge.downstream_id = obj.object_id
                    AND NOT EXISTS (SELECT 1 FROM @ordered WHERE object_id = edge.upstream_id))
            ORDER BY CASE obj.layer WHEN 'bronze' THEN 0 WHEN 'silver' THEN 1 ELSE 2 END,
                     obj.execution_priority, obj.object_id;
            IF @object_id IS NULL
                THROW 51103, 'Selected metadata contains a dependency cycle.', 1;
            SET @identity_json = (
                SELECT @object_id AS object_id, @run_id AS run_id
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            );
            SET @task_id = LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
                CONVERT(varchar(max), @identity_json)), 2));
            INSERT @ordered(object_id, task_id) VALUES (@object_id, @task_id);
        END;

        DECLARE @ordinal int = 1, @feed_id varchar(128), @layer varchar(8),
                @kind varchar(32), @name nvarchar(128), @schema nvarchar(128),
                @lakehouse nvarchar(128), @worker_id varchar(128),
                @prefix varchar(512), @source_type varchar(16),
                @source_code varchar(63), @silver_schema varchar(128),
                @batch_id char(64), @target_key char(64), @source_key char(64),
                @target_path nvarchar(2000), @dependencies nvarchar(max),
                @task_json nvarchar(max);
        WHILE @ordinal <= (SELECT COUNT(*) FROM @ordered)
        BEGIN
            SELECT @object_id = ordered.object_id, @task_id = ordered.task_id,
                   @feed_id = obj.feed_id, @layer = obj.layer,
                   @kind = obj.materialization, @name = obj.object_name,
                   @schema = obj.target_schema, @lakehouse = obj.lakehouse_name,
                   @worker_id = obj.worker_id, @source_type = feed.source_type,
                   @source_code = feed.source_code, @silver_schema = feed.silver_schema,
                   @prefix = 'Files/bronze/' + feed.source_code + '_'
                       + feed.description_slug + '_' + feed.source_type
            FROM @ordered AS ordered JOIN cfg.data_object AS obj
              ON obj.version_id = @version_id AND obj.object_id = ordered.object_id
            LEFT JOIN cfg.source_feed AS feed
              ON feed.version_id = @version_id AND feed.feed_id = obj.feed_id
            WHERE ordered.ordinal = @ordinal;
            SET @batch_id = NULL;
            SET @source_key = NULL;
            IF @feed_id IS NOT NULL
            BEGIN
                SET @identity_json = (SELECT @feed_id AS feed_id, @run_id AS run_id
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
                SET @batch_id = LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
                    CONVERT(varchar(max), @identity_json)), 2));
                SET @identity_json = (
                    SELECT @lakehouse AS lakehouse, 'source' AS scope,
                           @silver_schema AS silver_schema, @source_code AS source_code
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
                SET @source_key = LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
                    CONVERT(varchar(max), @identity_json)), 2));
            END;
            SET @identity_json = REPLACE((
                SELECT @lakehouse AS lakehouse, @name AS name,
                       CASE WHEN @layer = 'bronze' THEN @prefix ELSE @schema END AS scope
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), N'\/', N'/');
            SET @target_key = LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
                CONVERT(varchar(max), @identity_json)), 2));
            SET @target_path = CASE WHEN @layer = 'bronze'
                THEN @prefix + '/' + CONVERT(char(8), @instant, 112) + '/' + @batch_id
                     + CASE WHEN @source_type = 'sql' THEN '/' + @name + '.csv' ELSE '/' + @name END
                ELSE @lakehouse + '.' + @schema + '.' + @name END;
            SET @dependencies = COALESCE((
                SELECT edge.readiness_condition AS condition, parent.task_id AS task_id
                FROM cfg.object_dependency AS edge
                JOIN @ordered AS parent ON parent.object_id = edge.upstream_id
                WHERE edge.version_id = @version_id AND edge.downstream_id = @object_id
                ORDER BY parent.object_id FOR JSON PATH
            ), N'[]');
            SET @task_json = REPLACE((
                SELECT JSON_QUERY(@dependencies) AS dependencies,
                       @feed_id AS feed_id, @layer AS layer, @kind AS materialization,
                       @object_id AS object_id, @ordinal AS ordinal,
                       @batch_id AS source_batch_id, @source_key AS source_lock_key,
                       @target_key AS target_lock_key, @target_path AS target_path,
                       @task_id AS task_id, @worker_id AS worker_id
                FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER), N'\/', N'/');
            UPDATE @ordered SET task_json = @task_json WHERE ordinal = @ordinal;
            SET @ordinal += 1;
        END;
        DECLARE @tasks nvarchar(max), @snapshot nvarchar(max), @snapshot_hash char(64),
                @observed_text varchar(32);
        SELECT @tasks = N'[' + STRING_AGG(task_json, N',')
            WITHIN GROUP (ORDER BY ordinal) + N']' FROM @ordered;
        SET @observed_text = CONVERT(char(19), @instant, 126) + '.'
            + RIGHT('000000' + CONVERT(varchar(6), DATEPART(MICROSECOND, @instant)), 6) + 'Z';
        SET @snapshot = REPLACE((
            SELECT JSON_QUERY(N'[]') AS blockers, CONVERT(bit, 1) AS can_execute,
                   NULL AS configuration, @definition_hash AS definition_sha256,
                   1 AS format_version, @observed_text AS observed_utc,
                   JSON_QUERY(@selection) AS requested_objects, @run_id AS run_id,
                   JSON_QUERY(@tasks) AS tasks, @version_id AS version_id
            FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER), N'\/', N'/');
        SET @snapshot = REPLACE(@snapshot, N'"configuration":null',
            N'"configuration":' + @definition);
        IF DATALENGTH(CONVERT(varchar(max), @snapshot)) > 16777216
            THROW 51100, 'The run snapshot requires an explicitly supported chunked contract.', 1;
        SET @snapshot_hash = LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
            CONVERT(varchar(max), @snapshot)), 2));
        IF @expected_snapshot_sha256 IS NOT NULL AND @snapshot_hash <> @expected_snapshot_sha256
            THROW 51102, 'SQL run compilation differs from the expected canonical plan.', 1;
        INSERT ctl.[run] (
            run_id, version_id, definition_sha256, selection_sha256,
            snapshot_sha256, snapshot_json, status, observed_utc
        )
        VALUES (
            @run_id, @version_id, @definition_hash, @selection_hash,
            @snapshot_hash, @snapshot, 'PREPARED', @instant
        );
        INSERT ctl.run_task(run_id, task_id, object_id, task_ordinal, task_json, status)
        SELECT @run_id, task_id, object_id, ordinal, task_json, 'PENDING' FROM @ordered;
        COMMIT;
        SELECT run_id, version_id, snapshot_sha256, status
        FROM ctl.[run] WHERE run_id = @run_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_task_page
    @run_id varchar(128),
    @after_ordinal int = 0,
    @page_size int = 1000,
    @layer varchar(8) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @page_size IS NULL OR @page_size NOT BETWEEN 1 AND 5000
       OR @after_ordinal IS NULL OR @after_ordinal < 0
       OR (@layer IS NOT NULL AND @layer NOT IN ('bronze', 'silver', 'gold'))
        THROW 51105, 'Invalid bounded task-page arguments.', 1;
    IF NOT EXISTS (SELECT 1 FROM ctl.[run] WHERE run_id = @run_id)
        THROW 51105, 'The pinned run does not exist.', 1;
    IF EXISTS (
        SELECT 1 FROM ctl.run_task
        WHERE run_id = @run_id AND task_ordinal > @after_ordinal
          AND (@layer IS NULL OR JSON_VALUE(task_json, '$.layer') = @layer)
          AND DATALENGTH(task_json) + 256 > 4193280
    )
        THROW 51105, 'A task exceeds the Lookup response bound.', 1;
    ;WITH candidates AS (
        SELECT TOP (@page_size) task_ordinal, task_id, task_json,
               SUM(CONVERT(bigint, DATALENGTH(task_json)) + 256)
                   OVER (ORDER BY task_ordinal) AS response_bound
        FROM ctl.run_task
        WHERE run_id = @run_id AND task_ordinal > @after_ordinal
          AND (@layer IS NULL OR JSON_VALUE(task_json, '$.layer') = @layer)
        ORDER BY task_ordinal
    )
    SELECT task_ordinal, task_id, task_json FROM candidates
    WHERE response_bound <= 4193280 ORDER BY task_ordinal;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_acquire_lease
    @run_id varchar(128),
    @lease_key char(64),
    @expected_fence bigint = NULL,
    @lease_seconds int = 900,
    @emit_result bit = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @emit_result IS NULL OR @lease_seconds IS NULL OR @lease_seconds NOT BETWEEN 30 AND 86400
       OR NOT EXISTS (
           SELECT 1 FROM ctl.run_task WHERE run_id = @run_id
             AND (@lease_key = JSON_VALUE(task_json, '$.target_lock_key')
                  OR @lease_key = JSON_VALUE(task_json, '$.source_lock_key')))
        THROW 51110, 'The lease must belong to a pinned run target or source.', 1;
    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @owner varchar(128), @fence bigint, @released bit;
        SELECT @owner = run_id, @fence = fence, @released = released
        FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK) WHERE target_key = @lease_key;
        IF @owner IS NULL
        BEGIN
            IF @expected_fence IS NOT NULL
                THROW 51111, 'The requested lease fence does not exist.', 1;
            SET @fence = 1;
            INSERT ctl.target_lease(target_key, run_id, fence, expires_utc)
            VALUES (@lease_key, @run_id, @fence,
                DATEADD(SECOND, @lease_seconds, SYSUTCDATETIME()));
        END
        ELSE IF @released = 0
        BEGIN
            IF @owner <> @run_id OR @expected_fence IS NULL OR @expected_fence <> @fence
                THROW 51111, 'Lease ownership is active or uncertain; expiry does not prove a stopped job.', 1;
            UPDATE ctl.target_lease
            SET expires_utc = DATEADD(SECOND, @lease_seconds, SYSUTCDATETIME())
            WHERE target_key = @lease_key AND reconciliation_required = 0;
            IF @@ROWCOUNT <> 1
                THROW 51111, 'The lease requires service-job reconciliation before renewal.', 1;
        END
        ELSE
        BEGIN
            IF @expected_fence IS NOT NULL
                THROW 51111, 'A released lease must be acquired as a new fenced ownership.', 1;
            IF @fence = 9223372036854775807
                THROW 51111, 'Lease fence exhausted; an explicit migration is required.', 1;
            SET @fence += 1;
            UPDATE ctl.target_lease
            SET run_id = @run_id, fence = @fence, released = 0,
                expires_utc = DATEADD(SECOND, @lease_seconds, SYSUTCDATETIME()),
                last_job_id = NULL, reconciliation_required = 0,
                release_evidence_sha256 = NULL
            WHERE target_key = @lease_key;
        END;
        UPDATE ctl.[run] SET status = 'RUNNING'
        WHERE run_id = @run_id AND status = 'PREPARED';
        COMMIT;
        IF @emit_result = 1
            SELECT target_key, run_id, fence, expires_utc, released
            FROM ctl.target_lease WHERE target_key = @lease_key;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_run_snapshot
    @run_id varchar(128)
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM ctl.[run] WHERE run_id = @run_id)
        THROW 51105, 'The pinned run does not exist.', 1;
    SELECT snapshot_json, snapshot_sha256 FROM ctl.[run] WHERE run_id = @run_id;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_attempt_state
    @run_id varchar(128),
    @task_id char(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM ctl.[run] WHERE run_id = @run_id)
        THROW 51105, 'The pinned run does not exist.', 1;
    SELECT attempt.run_id, attempt.task_id, attempt.attempt_id, attempt.fence,
           attempt.source_fence, attempt.status, attempt.job_id, attempt.job_location,
           attempt.result_sha256, attempt.observed_job_status,
           JSON_VALUE(task.task_json, '$.target_lock_key') AS target_lock_key,
           JSON_VALUE(task.task_json, '$.source_lock_key') AS source_lock_key
    FROM ctl.task_attempt AS attempt
    JOIN ctl.run_task AS task ON task.run_id = attempt.run_id AND task.task_id = attempt.task_id
    WHERE attempt.run_id = @run_id AND (@task_id IS NULL OR attempt.task_id = @task_id)
    ORDER BY task.task_ordinal, attempt.started_utc, attempt.attempt_id;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_lease_state
    @run_id varchar(128),
    @lease_key char(64)
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (
        SELECT 1 FROM ctl.run_task WHERE run_id = @run_id
          AND (@lease_key = JSON_VALUE(task_json, '$.target_lock_key')
               OR @lease_key = JSON_VALUE(task_json, '$.source_lock_key')))
        THROW 51110, 'The lease key is outside the pinned run.', 1;
    SELECT target_key, run_id, fence, expires_utc, released,
           last_job_id, reconciliation_required, release_evidence_sha256
    FROM ctl.target_lease WHERE target_key = @lease_key;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_release_lease
    @run_id varchar(128),
    @lease_key char(64),
    @fence bigint,
    @release_evidence_sha256 char(64)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @release_evidence_sha256 IS NULL
       OR @release_evidence_sha256 COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
        THROW 51112, 'Release requires attributable service-job reconciliation evidence.', 1;
    BEGIN TRANSACTION;
    BEGIN TRY
        IF NOT EXISTS (
            SELECT 1 FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK)
            WHERE target_key = @lease_key AND run_id = @run_id AND fence = @fence)
            THROW 51112, 'Lease ownership or fence differs.', 1;
        IF EXISTS (
            SELECT 1 FROM ctl.task_attempt AS attempt
            JOIN ctl.run_task AS task ON task.run_id = attempt.run_id AND task.task_id = attempt.task_id
            WHERE attempt.run_id = @run_id
              AND (@lease_key = JSON_VALUE(task.task_json, '$.target_lock_key')
                   OR @lease_key = JSON_VALUE(task.task_json, '$.source_lock_key'))
              AND attempt.status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'NOT_SUBMITTED'))
            THROW 51112, 'A service job is active or uncertain; the lease cannot be released.', 1;
        UPDATE ctl.target_lease
        SET released = 1, reconciliation_required = 0,
            release_evidence_sha256 = @release_evidence_sha256
        WHERE target_key = @lease_key AND run_id = @run_id AND fence = @fence;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_begin_attempt
    @run_id varchar(128),
    @task_id char(64),
    @attempt_id uniqueidentifier,
    @fence bigint,
    @source_fence bigint = NULL,
    @emit_result bit = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @task nvarchar(max), @target_key char(64), @source_key char(64);
        SELECT @task = task_json
        FROM ctl.run_task WITH (UPDLOCK, HOLDLOCK)
        WHERE run_id = @run_id AND task_id = @task_id;
        IF @emit_result IS NULL OR @task IS NULL OR @attempt_id IS NULL
            THROW 51120, 'Attempt requires an exact pinned task and identity.', 1;
        IF NOT EXISTS (
            SELECT 1 FROM ctl.task_attempt
            WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id)
           AND EXISTS (
               SELECT 1 FROM ctl.task_attempt WHERE run_id = @run_id AND task_id = @task_id
                 AND status NOT IN ('FAILED', 'CANCELLED', 'NOT_SUBMITTED'))
            THROW 51123, 'An active, uncertain or successful attempt prevents another submission.', 1;
        SET @target_key = JSON_VALUE(@task, '$.target_lock_key');
        SET @source_key = JSON_VALUE(@task, '$.source_lock_key');
        IF NOT EXISTS (
            SELECT 1 FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK)
            WHERE target_key = @target_key AND run_id = @run_id AND fence = @fence
              AND released = 0 AND reconciliation_required = 0 AND expires_utc > SYSUTCDATETIME())
           OR (@source_key IS NOT NULL AND NOT EXISTS (
               SELECT 1 FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK)
               WHERE target_key = @source_key AND run_id = @run_id AND fence = @source_fence
                 AND released = 0 AND reconciliation_required = 0 AND expires_utc > SYSUTCDATETIME()))
            THROW 51121, 'Attempt requires current source and target lease fences.', 1;
        IF EXISTS (
            SELECT 1 FROM ctl.task_attempt
            WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id)
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM ctl.task_attempt
                WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id
                  AND fence = @fence AND (source_fence = @source_fence
                       OR (source_fence IS NULL AND @source_fence IS NULL)))
                THROW 51121, 'An attempt identity cannot be rebound to another fence.', 1;
            COMMIT;
            IF @emit_result = 1
                SELECT attempt_id, job_id, fence, source_fence, status
                FROM ctl.task_attempt
                WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id;
            RETURN;
        END;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@task, '$.dependencies')
            WITH (task_id char(64), condition varchar(32)) AS dependency
            LEFT JOIN ctl.run_task AS parent
              ON parent.run_id = @run_id AND parent.task_id = dependency.task_id
            WHERE parent.status IS NULL OR parent.status <> 'SUCCEEDED'
               OR (dependency.condition <> 'succeeded' AND NOT EXISTS (
                   SELECT 1 FROM ctl.task_result_inbox AS result
                   CROSS APPLY OPENJSON(result.result_json, '$.readiness') AS ready
                   WHERE result.run_id = @run_id AND result.task_id = dependency.task_id
                     AND result.reconciled_utc IS NOT NULL AND ready.[value] = dependency.condition))
        )
            THROW 51122, 'Upstream success and publication readiness are not satisfied.', 1;
        IF EXISTS (
            SELECT 1 FROM ctl.task_attempt WHERE run_id = @run_id AND task_id = @task_id
              AND status NOT IN ('FAILED', 'CANCELLED', 'NOT_SUBMITTED'))
            THROW 51123, 'An active, uncertain or successful attempt prevents another submission.', 1;
        INSERT ctl.task_attempt(run_id, task_id, attempt_id, fence, source_fence, status)
        VALUES (@run_id, @task_id, @attempt_id, @fence, @source_fence, 'SUBMISSION_PENDING');
        UPDATE ctl.run_task SET status = 'RUNNING' WHERE run_id = @run_id AND task_id = @task_id;
        COMMIT;
        IF @emit_result = 1
            SELECT attempt_id, job_id, fence, source_fence, status
            FROM ctl.task_attempt
            WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_record_submission
    @run_id varchar(128),
    @task_id char(64),
    @attempt_id uniqueidentifier,
    @fence bigint,
    @submission_state varchar(32),
    @evidence_sha256 char(64),
    @job_id varchar(128) = NULL,
    @job_location nvarchar(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @submission_state IS NULL OR @submission_state NOT IN ('SUBMITTED', 'UNCERTAIN', 'NOT_SUBMITTED')
       OR @evidence_sha256 IS NULL
       OR @evidence_sha256 COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
       OR (@submission_state = 'SUBMITTED' AND @job_id IS NULL)
       OR (@submission_state = 'NOT_SUBMITTED' AND @job_id IS NOT NULL)
       OR (@job_id IS NOT NULL AND (
           LEN(@job_id) = 0 OR @job_id COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_]%'))
       OR (@job_location IS NOT NULL AND (
           @job_location NOT LIKE N'https://api.fabric.microsoft.com/v1/%'
           OR @job_location LIKE N'%?%' OR @job_location LIKE N'%#%'))
        THROW 51124, 'Invalid attributable submission outcome or public job reference.', 1;
    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @old_job varchar(128), @old_status varchar(32), @task nvarchar(max),
                @source_fence bigint;
        SELECT @old_job = job_id, @old_status = status, @source_fence = source_fence
        FROM ctl.task_attempt WITH (UPDLOCK, HOLDLOCK)
        WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id AND fence = @fence;
        IF @old_status IS NULL
           OR (@old_job IS NOT NULL AND (@job_id IS NULL OR @old_job <> @job_id))
           OR (@old_status NOT IN ('SUBMISSION_PENDING', 'SUBMITTED', 'UNCERTAIN')
               AND @old_status <> @submission_state)
           OR (@submission_state = 'NOT_SUBMITTED' AND @old_status <> 'SUBMISSION_PENDING'
               AND @old_status <> 'NOT_SUBMITTED')
            THROW 51125, 'Submission identity or outcome conflicts with the recorded attempt.', 1;
        SELECT @task = task_json FROM ctl.run_task WHERE run_id = @run_id AND task_id = @task_id;
        IF NOT EXISTS (
            SELECT 1 FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK)
            WHERE target_key = JSON_VALUE(@task, '$.target_lock_key')
              AND run_id = @run_id AND fence = @fence AND released = 0)
            THROW 51121, 'Submission is no longer owned by this fenced attempt.', 1;
        UPDATE ctl.task_attempt
        SET status = @submission_state, job_id = @job_id, job_location = @job_location,
            submission_evidence_sha256 = @evidence_sha256,
            completed_utc = CASE WHEN @submission_state = 'NOT_SUBMITTED' THEN SYSUTCDATETIME() END
        WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id;
        UPDATE lease
        SET last_job_id = COALESCE(@job_id, last_job_id),
            reconciliation_required = CASE WHEN EXISTS (
                SELECT 1 FROM ctl.task_attempt AS pending
                JOIN ctl.run_task AS task ON task.run_id = pending.run_id AND task.task_id = pending.task_id
                WHERE pending.run_id = @run_id AND pending.status = 'UNCERTAIN'
                  AND ((lease.target_key = JSON_VALUE(task.task_json, '$.target_lock_key')
                        AND lease.fence = pending.fence)
                       OR (lease.target_key = JSON_VALUE(task.task_json, '$.source_lock_key')
                           AND lease.fence = pending.source_fence)))
                THEN 1 ELSE 0 END
        FROM ctl.target_lease AS lease
        WHERE lease.run_id = @run_id AND (
            (lease.target_key = JSON_VALUE(@task, '$.target_lock_key') AND lease.fence = @fence)
            OR (lease.target_key = JSON_VALUE(@task, '$.source_lock_key') AND lease.fence = @source_fence));
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_accept_result
    @result_json nvarchar(max),
    @result_sha256 char(64),
    @observed_job_status varchar(32),
    @job_evidence_sha256 char(64)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF COALESCE(ISJSON(@result_json), 0) <> 1
       OR DATALENGTH(CONVERT(varchar(max), @result_json)) > 1048576
       OR @result_json COLLATE Latin1_General_100_BIN2 LIKE N'%[^ -~]%'
       OR @result_sha256 IS NULL
       OR LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
            CONVERT(varchar(max), @result_json)), 2)) <> @result_sha256
       OR @observed_job_status IS NULL
       OR @observed_job_status NOT IN ('Completed', 'Failed', 'Cancelled')
       OR @job_evidence_sha256 IS NULL
       OR @job_evidence_sha256 COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
        THROW 51130, 'A bounded result, matching hash and terminal service-job evidence are required.', 1;
    DECLARE @run_id varchar(128) = JSON_VALUE(@result_json, '$.run_id'),
            @task_id char(64) = JSON_VALUE(@result_json, '$.task_id'),
            @attempt_id uniqueidentifier = TRY_CONVERT(uniqueidentifier, JSON_VALUE(@result_json, '$.attempt_id')),
            @fence bigint = TRY_CONVERT(bigint, JSON_VALUE(@result_json, '$.fence')),
            @job_id varchar(128) = JSON_VALUE(@result_json, '$.job_id'),
            @status varchar(32) = JSON_VALUE(@result_json, '$.status'),
            @finished datetime2(6) = TRY_CONVERT(datetime2(6), JSON_VALUE(@result_json, '$.finished_utc'), 127),
            @result_id char(64);
    IF COALESCE(JSON_VALUE(@result_json, '$.format_version'), '') <> '1'
       OR @attempt_id IS NULL OR @fence IS NULL OR @fence < 1 OR @finished IS NULL
       OR @status IS NULL OR @status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
       OR (@status = 'SUCCEEDED' AND (
           @observed_job_status <> 'Completed'
           OR JSON_VALUE(@result_json, '$.error_code') IS NOT NULL
           OR NOT EXISTS (SELECT 1 FROM OPENJSON(@result_json, '$.artifact_refs'))
           OR NOT EXISTS (SELECT 1 FROM OPENJSON(@result_json, '$.readiness') WHERE [value] = 'succeeded')))
       OR (@status <> 'SUCCEEDED' AND (
           JSON_VALUE(@result_json, '$.error_code') IS NULL
           OR LEN(JSON_VALUE(@result_json, '$.error_code')) NOT BETWEEN 1 AND 128
           OR LEFT(JSON_VALUE(@result_json, '$.error_code'), 1)
              COLLATE Latin1_General_100_BIN2 NOT LIKE '[A-Z]'
           OR JSON_VALUE(@result_json, '$.error_code') COLLATE Latin1_General_100_BIN2 LIKE '%[^A-Z0-9_]%'
           OR EXISTS (SELECT 1 FROM OPENJSON(@result_json, '$.readiness'))))
       OR (SELECT COUNT(*) FROM OPENJSON(@result_json)) <> 13
       OR (SELECT COUNT(DISTINCT [key]) FROM OPENJSON(@result_json)) <> 13
       OR NOT EXISTS (
           SELECT 1 FROM OPENJSON(@result_json)
           WHERE [key] = 'artifact_refs' AND [type] = 4)
       OR NOT EXISTS (
           SELECT 1 FROM OPENJSON(@result_json)
           WHERE [key] = 'readiness' AND [type] = 4)
       OR NOT EXISTS (
           SELECT 1 FROM OPENJSON(@result_json)
           WHERE [key] = 'metrics' AND [type] = 5)
       OR EXISTS (
           SELECT 1 FROM OPENJSON(@result_json)
           WHERE ([key] IN ('format_version','fence') AND [type] <> 2)
              OR ([key] IN ('run_id','task_id','attempt_id','job_id','status','finished_utc') AND [type] <> 1)
              OR ([key] IN ('source_batch_id','error_code') AND [type] NOT IN (0,1)))
       OR EXISTS (
           SELECT 1 FROM OPENJSON(@result_json) WHERE [key] NOT IN (
               'format_version','run_id','task_id','source_batch_id','attempt_id',
               'fence','job_id','status','finished_utc','artifact_refs','readiness','error_code','metrics'))
        THROW 51130, 'Result fields, failure code or publication readiness are invalid.', 1;
    IF EXISTS (SELECT 1 FROM OPENJSON(@result_json, '$.artifact_refs') WHERE [type] <> 5)
        THROW 51130, 'Artifact evidence must contain metadata objects only.', 1;
    IF LEN(JSON_VALUE(@result_json, '$.task_id')) <> 64
       OR JSON_VALUE(@result_json, '$.task_id') COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
       OR LEN(JSON_VALUE(@result_json, '$.run_id')) NOT BETWEEN 1 AND 128
       OR JSON_VALUE(@result_json, '$.run_id') COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_]%'
       OR LEN(JSON_VALUE(@result_json, '$.job_id')) NOT BETWEEN 1 AND 128
       OR JSON_VALUE(@result_json, '$.job_id') COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_]%'
       OR RIGHT(JSON_VALUE(@result_json, '$.finished_utc'), 1) <> 'Z'
       OR (SELECT COUNT(*) FROM OPENJSON(@result_json, '$.artifact_refs')) > 1000
       OR EXISTS (
           SELECT 1 FROM OPENJSON(@result_json, '$.readiness')
           WHERE [type] <> 1 OR [value] NOT IN ('succeeded','source_committed','facts_ready','gold_released'))
       OR EXISTS (
           SELECT [value] FROM OPENJSON(@result_json, '$.readiness')
           GROUP BY [value] HAVING COUNT(*) > 1)
       OR EXISTS (
           SELECT 1 FROM OPENJSON(@result_json, '$.metrics')
           WHERE [key] NOT IN ('rows_read','rows_written','rows_quarantined')
              OR [type] <> 2 OR TRY_CONVERT(bigint, [value]) IS NULL OR TRY_CONVERT(bigint, [value]) < 0)
       OR EXISTS (
           SELECT 1 FROM OPENJSON(@result_json, '$.artifact_refs') AS ref
           WHERE ref.[type] <> 5
              OR (SELECT COUNT(*) FROM OPENJSON(ref.[value])) <> 4
              OR (SELECT COUNT(DISTINCT [key]) FROM OPENJSON(ref.[value])) <> 4
              OR EXISTS (SELECT 1 FROM OPENJSON(ref.[value])
                         WHERE [key] NOT IN ('kind','location','version','sha256'))
              OR EXISTS (SELECT 1 FROM OPENJSON(ref.[value])
                         WHERE ([key] IN ('kind','location','sha256') AND [type] <> 1)
                            OR ([key] = 'version' AND [type] NOT IN (0,2)))
              OR COALESCE(JSON_VALUE(ref.[value], '$.kind'), '') NOT IN (
                  'raw_batch','delta_version','source_commit','gold_release')
              OR COALESCE(LEN(JSON_VALUE(ref.[value], '$.location')), 0) NOT BETWEEN 1 AND 2000
              OR JSON_VALUE(ref.[value], '$.location') COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_./]%'
              OR JSON_VALUE(ref.[value], '$.location') LIKE '%..%'
              OR COALESCE(LEN(JSON_VALUE(ref.[value], '$.sha256')), 0) <> 64
              OR JSON_VALUE(ref.[value], '$.sha256') COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
              OR (JSON_VALUE(ref.[value], '$.kind') = 'raw_batch' AND (
                  JSON_VALUE(ref.[value], '$.location') NOT LIKE 'Files/bronze/%'
                  OR JSON_VALUE(ref.[value], '$.version') IS NOT NULL))
              OR (JSON_VALUE(ref.[value], '$.kind') <> 'raw_batch' AND (
                  TRY_CONVERT(bigint, JSON_VALUE(ref.[value], '$.version')) IS NULL
                  OR TRY_CONVERT(bigint, JSON_VALUE(ref.[value], '$.version')) < 0
                  OR PARSENAME(JSON_VALUE(ref.[value], '$.location'), 1) IS NULL
                  OR PARSENAME(JSON_VALUE(ref.[value], '$.location'), 2) IS NULL
                  OR PARSENAME(JSON_VALUE(ref.[value], '$.location'), 3) IS NULL
                  OR PARSENAME(JSON_VALUE(ref.[value], '$.location'), 4) IS NOT NULL)))
        THROW 51130, 'Result identities, metrics or artifact evidence violate their contract.', 1;
    IF EXISTS (
        SELECT 1 FROM OPENJSON(@result_json, '$.readiness') AS ready
        WHERE ready.[value] IN ('source_committed','facts_ready','gold_released')
          AND NOT EXISTS (
              SELECT 1 FROM OPENJSON(@result_json, '$.artifact_refs') AS ref
              WHERE JSON_VALUE(ref.[value], '$.kind') = CASE ready.[value]
                  WHEN 'source_committed' THEN 'source_commit'
                  WHEN 'facts_ready' THEN 'delta_version' ELSE 'gold_release' END))
        THROW 51130, 'Publication readiness requires matching artifact references.', 1;
    SET @result_id = LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
        @run_id + '|' + @task_id + '|' + CONVERT(varchar(36), @attempt_id)), 2));
    BEGIN TRANSACTION;
    BEGIN TRY
        IF EXISTS (SELECT 1 FROM ctl.task_result_inbox WITH (UPDLOCK, HOLDLOCK)
                   WHERE result_id = @result_id)
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM ctl.task_result_inbox
                           WHERE result_id = @result_id AND result_sha256 = @result_sha256)
                THROW 51132, 'An accepted attempt result cannot be rebound to changed evidence.', 1;
            COMMIT;
            RETURN;
        END;
        DECLARE @task nvarchar(max), @source_fence bigint;
        SELECT @task = task.task_json, @source_fence = attempt.source_fence
        FROM ctl.task_attempt AS attempt WITH (UPDLOCK, HOLDLOCK)
        JOIN ctl.run_task AS task ON task.run_id = attempt.run_id AND task.task_id = attempt.task_id
        WHERE attempt.run_id = @run_id AND attempt.task_id = @task_id
          AND attempt.attempt_id = @attempt_id AND attempt.fence = @fence
          AND attempt.job_id = @job_id AND attempt.status IN ('SUBMITTED', 'UNCERTAIN');
        IF @task IS NULL OR NOT EXISTS (
            SELECT 1 FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK)
            WHERE target_key = JSON_VALUE(@task, '$.target_lock_key')
              AND run_id = @run_id AND fence = @fence AND released = 0)
           OR (JSON_VALUE(@task, '$.source_lock_key') IS NOT NULL AND NOT EXISTS (
               SELECT 1 FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK)
               WHERE target_key = JSON_VALUE(@task, '$.source_lock_key')
                 AND run_id = @run_id AND fence = @source_fence AND released = 0))
            THROW 51131, 'Result is not owned by the recorded source and target fences.', 1;
        IF COALESCE(JSON_VALUE(@result_json, '$.source_batch_id'), '') <>
           COALESCE(JSON_VALUE(@task, '$.source_batch_id'), '')
            THROW 51130, 'Result source batch differs from the immutable task.', 1;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@result_json, '$.artifact_refs') AS ref
            WHERE JSON_VALUE(ref.[value], '$.kind') IN ('raw_batch','delta_version')
              AND JSON_VALUE(ref.[value], '$.location') <> JSON_VALUE(@task, '$.target_path')
              AND NOT (
                  JSON_VALUE(ref.[value], '$.kind') = 'raw_batch'
                  AND LEFT(JSON_VALUE(ref.[value], '$.location'),
                           LEN(JSON_VALUE(@task, '$.target_path')) + 1)
                      = JSON_VALUE(@task, '$.target_path') + '/'))
            THROW 51130, 'Artifact evidence is outside the pinned task target.', 1;
        INSERT ctl.task_result_inbox (
            result_id, run_id, task_id, attempt_id, fence,
            result_sha256, result_json, reconciled_utc
        )
        VALUES (
            @result_id, @run_id, @task_id, @attempt_id, @fence,
            @result_sha256, @result_json, SYSUTCDATETIME()
        );
        UPDATE ctl.task_attempt
        SET status = @status, result_sha256 = @result_sha256,
            completed_utc = @finished, observed_job_status = @observed_job_status,
            job_evidence_sha256 = @job_evidence_sha256
        WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id;
        UPDATE ctl.run_task SET status = @status WHERE run_id = @run_id AND task_id = @task_id;
        UPDATE lease SET reconciliation_required = CASE WHEN EXISTS (
            SELECT 1 FROM ctl.task_attempt AS pending
            JOIN ctl.run_task AS task ON task.run_id = pending.run_id AND task.task_id = pending.task_id
            WHERE pending.run_id = @run_id AND pending.status = 'UNCERTAIN'
              AND ((lease.target_key = JSON_VALUE(task.task_json, '$.target_lock_key') AND lease.fence = pending.fence)
                   OR (lease.target_key = JSON_VALUE(task.task_json, '$.source_lock_key') AND lease.fence = pending.source_fence)))
            THEN 1 ELSE 0 END
        FROM ctl.target_lease AS lease
        WHERE lease.run_id = @run_id AND (
            (lease.target_key = JSON_VALUE(@task, '$.target_lock_key') AND lease.fence = @fence)
            OR (lease.target_key = JSON_VALUE(@task, '$.source_lock_key') AND lease.fence = @source_fence));
        UPDATE ctl.[run]
        SET status = CASE
            WHEN EXISTS (SELECT 1 FROM ctl.run_task WHERE run_id = @run_id AND status IN ('FAILED', 'CANCELLED'))
                THEN 'ATTENTION_REQUIRED'
            WHEN NOT EXISTS (SELECT 1 FROM ctl.run_task WHERE run_id = @run_id AND status <> 'SUCCEEDED')
                THEN 'TASKS_COMPLETE'
            ELSE 'RUNNING' END
        WHERE run_id = @run_id;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
GRANT EXECUTE ON OBJECT::ctl.usp_prepare_run TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_task_page TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_run_snapshot TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_attempt_state TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_lease_state TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_acquire_lease TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_release_lease TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_begin_attempt TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_record_submission TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_accept_result TO runtime_executor;
GO
CREATE OR ALTER PROCEDURE ctl.usp_export_run
    @run_id varchar(128)
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM ctl.[run] WHERE run_id = @run_id)
        THROW 51105, 'The pinned run does not exist.', 1;
    SELECT snapshot_json FROM ctl.[run] WHERE run_id = @run_id;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_prepare_pipeline_run
    @run_id varchar(128),
    @version_id varchar(128),
    @observed_utc varchar(32),
    @selected_objects_json nvarchar(max),
    @resume_run_id varchar(128)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF NULLIF(@resume_run_id, '') IS NOT NULL
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM ctl.[run] WHERE run_id = @resume_run_id)
            THROW 51106, 'The retained run does not exist.', 1;
        IF EXISTS (
            SELECT 1 FROM ctl.run_task WHERE run_id = @resume_run_id
              AND JSON_VALUE(task_json, '$.layer') = 'bronze' AND status <> 'SUCCEEDED')
            THROW 51106, 'Retained-run replay requires completely reconciled Bronze; do not overwrite partial files.', 1;
        IF NOT EXISTS (
            SELECT 1 FROM ctl.[run] AS pinned JOIN cfg.capability AS cap
              ON cap.version_id = pinned.version_id
            WHERE pinned.run_id = @resume_run_id
              AND cap.capability_id = 'metadata-pipeline-runtime' AND cap.verified = 1
              AND cap.evidence_sha256 IS NOT NULL AND LEN(cap.evidence_sha256) = 64
              AND cap.evidence_sha256 COLLATE Latin1_General_100_BIN2 NOT LIKE '%[^0-9a-f]%')
            THROW 51106, 'The pinned replay runtime has not passed its integration gate.', 1;
        SELECT run_id, version_id, snapshot_sha256, status
        FROM ctl.[run] WHERE run_id = @resume_run_id;
        RETURN;
    END;
    SET @version_id = COALESCE(NULLIF(@version_id, ''),
        (SELECT version_id FROM cfg.active_configuration WHERE singleton_id = 1));
    DECLARE @definition_hash char(64);
    SELECT @definition_hash = definition_sha256
    FROM cfg.configuration_version WHERE version_id = @version_id AND lifecycle_state = 'APPROVED';
    IF @definition_hash IS NULL OR NOT EXISTS (
        SELECT 1 FROM cfg.capability WHERE version_id = @version_id
          AND capability_id = 'metadata-pipeline-runtime' AND verified = 1
          AND evidence_sha256 IS NOT NULL AND LEN(evidence_sha256) = 64
          AND evidence_sha256 COLLATE Latin1_General_100_BIN2 NOT LIKE '%[^0-9a-f]%')
        THROW 51106, 'The integrated medallion runtime is not verified for this approved configuration.', 1;
    SET @selected_objects_json = NULLIF(@selected_objects_json, '');
    EXEC ctl.usp_prepare_run @run_id, @version_id, @definition_hash,
        @observed_utc, @selected_objects_json;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_copy_task_page
    @run_id varchar(128),
    @after_ordinal int = 0,
    @page_size int = 1000,
    @operation varchar(16) = 'copy'
AS
BEGIN
    SET NOCOUNT ON;
    IF @after_ordinal IS NULL OR @after_ordinal < 0 OR @page_size IS NULL
       OR @page_size NOT BETWEEN 1 AND 5000
        THROW 51105, 'Invalid bounded Copy-page arguments.', 1;
    DECLARE @snapshot nvarchar(max);
    SELECT @snapshot = snapshot_json FROM ctl.[run] WHERE run_id = @run_id;
    IF @snapshot IS NULL
        THROW 51105, 'The pinned run does not exist.', 1;
    IF @operation IS NULL OR @operation NOT IN ('copy', 'reconcile')
        THROW 51105, 'Unsupported reusable Copy pipeline operation.', 1;
    IF @operation = 'reconcile'
    BEGIN
        SELECT CAST(NULL AS varchar(64)) AS authentication_type,
               CAST(NULL AS nvarchar(max)) AS arrival_policy_json,
               CAST(NULL AS varchar(36)) AS connection_id,
               CAST(NULL AS varchar(32)) AS connector_type,
               CAST(NULL AS varchar(128)) AS feed_id,
               CAST(NULL AS varchar(256)) AS file_name,
               CAST(NULL AS varchar(256)) AS file_pattern,
               CAST(NULL AS varchar(16)) AS file_format,
               CAST(NULL AS nvarchar(2000)) AS folder_path,
               CAST(NULL AS varchar(36)) AS source_artifact_id,
               CAST(NULL AS char(64)) AS source_batch_id,
               CAST(NULL AS char(64)) AS source_lock_key,
               CAST(NULL AS varchar(16)) AS source_type,
               CAST(NULL AS varchar(36)) AS source_workspace_id,
               CAST(NULL AS nvarchar(2000)) AS source_directory,
               CAST(NULL AS bit) AS recursive,
               CAST(NULL AS nvarchar(max)) AS sql_query,
               CAST(NULL AS char(64)) AS target_lock_key,
               CAST(NULL AS char(64)) AS task_id,
               CAST(NULL AS int) AS task_ordinal
        WHERE 1 = 0;
        RETURN;
    END;
    DECLARE @rows TABLE (ordinal int PRIMARY KEY, descriptor nvarchar(max) NOT NULL);
    INSERT @rows(ordinal, descriptor)
    SELECT task.task_ordinal, REPLACE((
        SELECT conn.authentication_type AS authentication_type,
               JSON_QUERY(obj.arrival_policy_json) AS arrival_policy_json,
               conn.fabric_connection_id AS connection_id,
               conn.connector_type AS connector_type, feed.id AS feed_id,
               CASE WHEN feed.source_type = 'sql' THEN obj.name + '.csv' END AS file_name,
               obj.file_pattern AS file_pattern, obj.file_format AS file_format,
               CASE WHEN feed.source_type = 'sql'
                   THEN SUBSTRING(JSON_VALUE(task.task_json, '$.target_path'), 7,
                       LEN(JSON_VALUE(task.task_json, '$.target_path')) - 6 - LEN(obj.name) - 5)
                   ELSE SUBSTRING(JSON_VALUE(task.task_json, '$.target_path'), 7,
                       LEN(JSON_VALUE(task.task_json, '$.target_path')) - 6) END AS folder_path,
               feed.artifact_id AS source_artifact_id,
               JSON_VALUE(task.task_json, '$.source_batch_id') AS source_batch_id,
               JSON_VALUE(task.task_json, '$.source_lock_key') AS source_lock_key,
               feed.source_type AS source_type, feed.workspace_id AS source_workspace_id,
               obj.source_directory AS source_directory, obj.recursive AS recursive,
               CASE WHEN feed.source_type = 'sql' THEN N'SELECT ' + obj.projection_sql + N' FROM '
                   + QUOTENAME(obj.source_schema) + N'.' + QUOTENAME(obj.source_object) END AS sql_query,
               JSON_VALUE(task.task_json, '$.target_lock_key') AS target_lock_key,
               task.task_id AS task_id, task.task_ordinal AS task_ordinal
        FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER), N'\/', N'/')
    FROM ctl.run_task AS task
    JOIN OPENJSON(@snapshot, '$.configuration.objects') WITH (
        id varchar(128), name varchar(128), feed_id varchar(128), layer varchar(8),
        source_schema varchar(128) '$.contract.source_schema',
        source_object varchar(128) '$.contract.source_object',
        projection_sql nvarchar(max) '$.contract.projection_sql',
        source_directory nvarchar(2000) '$.contract.source_directory',
        file_pattern varchar(256) '$.contract.file_pattern',
        file_format varchar(16) '$.contract.file_format',
        recursive bit '$.contract.recursive',
        arrival_policy_json nvarchar(max) '$.contract.arrival_policy' AS JSON
    ) AS obj ON obj.id = task.object_id
    JOIN OPENJSON(@snapshot, '$.configuration.feeds') WITH (
        id varchar(128), connection_id varchar(128), source_type varchar(16),
        workspace_id varchar(36) '$.geometry.workspace_id',
        artifact_id varchar(36) '$.geometry.artifact_id'
    ) AS feed ON feed.id = obj.feed_id
    JOIN OPENJSON(@snapshot, '$.configuration.connections') WITH (
        id varchar(128), authentication_type varchar(64), fabric_connection_id varchar(36),
        connector_type varchar(32)
    ) AS conn ON conn.id = feed.connection_id
    WHERE task.run_id = @run_id AND obj.layer = 'bronze'
      AND task.task_ordinal > @after_ordinal
      AND CONVERT(bigint, task.task_ordinal) <= CONVERT(bigint, @after_ordinal) + @page_size
      AND task.status <> 'SUCCEEDED';
    IF EXISTS (
        SELECT 1 FROM @rows
        WHERE JSON_VALUE(descriptor, '$.source_type') NOT IN ('sql', 'ftp', 'sftp')
           OR JSON_VALUE(descriptor, '$.connection_id') IS NULL
           OR COALESCE(JSON_VALUE(descriptor, '$.connector_type'), '') NOT IN ('FabricSql', 'SqlServer', 'Ftp', 'Sftp')
           OR (JSON_VALUE(descriptor, '$.source_type') = 'sql' AND (
               JSON_VALUE(descriptor, '$.connector_type') NOT IN ('FabricSql', 'SqlServer')
               OR (JSON_VALUE(descriptor, '$.connector_type') = 'FabricSql' AND (
                   JSON_VALUE(descriptor, '$.source_artifact_id') IS NULL
                   OR JSON_VALUE(descriptor, '$.source_workspace_id') IS NULL))
               OR (JSON_VALUE(descriptor, '$.connector_type') = 'SqlServer' AND (
                   JSON_VALUE(descriptor, '$.source_artifact_id') IS NOT NULL
                   OR JSON_VALUE(descriptor, '$.source_workspace_id') IS NOT NULL))
               OR JSON_VALUE(descriptor, '$.sql_query') IS NULL))
           OR (JSON_VALUE(descriptor, '$.source_type') IN ('ftp', 'sftp') AND (
               JSON_VALUE(descriptor, '$.connector_type') <> CASE JSON_VALUE(descriptor, '$.source_type') WHEN 'ftp' THEN 'Ftp' ELSE 'Sftp' END
               OR
               JSON_VALUE(descriptor, '$.source_directory') IS NULL
               OR JSON_VALUE(descriptor, '$.file_pattern') IS NULL
               OR COALESCE(JSON_VALUE(descriptor, '$.file_format'), '') <> 'csv'
               OR JSON_VALUE(descriptor, '$.recursive') IS NULL
               OR JSON_QUERY(descriptor, '$.arrival_policy_json') IS NULL)))
        THROW 51107, 'Copy adapter, source geometry or typed projection is not ready.', 1;
    IF COALESCE((SELECT SUM(CONVERT(bigint, DATALENGTH(descriptor)) + 256) FROM @rows), 0) > 4193280
        THROW 51105, 'The requested Copy page exceeds the Lookup response bound.', 1;
    SELECT value.authentication_type, value.arrival_policy_json, value.connection_id,
           value.connector_type, value.feed_id,
           value.file_name, value.file_pattern, value.file_format, value.folder_path, value.source_artifact_id,
           value.source_batch_id, value.source_lock_key, value.source_type,
           value.source_workspace_id, value.source_directory, value.recursive,
           value.sql_query, value.target_lock_key,
           value.task_id, value.task_ordinal
    FROM @rows AS row
    CROSS APPLY OPENJSON(row.descriptor) WITH (
        authentication_type varchar(64), arrival_policy_json nvarchar(max) AS JSON,
        connection_id varchar(36), connector_type varchar(32), feed_id varchar(128),
        file_name varchar(256), file_pattern varchar(256), file_format varchar(16),
        folder_path nvarchar(2000), source_artifact_id varchar(36),
        source_batch_id char(64), source_lock_key char(64), source_type varchar(16),
        source_workspace_id varchar(36), source_directory nvarchar(2000), recursive bit,
        sql_query nvarchar(max),
        target_lock_key char(64), task_id char(64), task_ordinal int
    ) AS value ORDER BY row.ordinal;
END;
GO
GRANT EXECUTE ON OBJECT::ctl.usp_export_run TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_prepare_pipeline_run TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_copy_task_page TO runtime_executor;
GO
CREATE OR ALTER PROCEDURE ctl.usp_prepare_copy_attempt
    @run_id varchar(128),
    @task_id char(64),
    @execution_run_id uniqueidentifier,
    @workspace_id uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @execution_run_id IS NULL OR @workspace_id IS NULL
        THROW 51140, 'Copy preparation requires its actual workspace and pipeline execution.', 1;
    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @task nvarchar(max), @attempt_id uniqueidentifier = NEWID(),
                @target_key char(64), @source_key char(64),
                @target_fence bigint, @source_fence bigint,
                @location nvarchar(2000) = N'https://api.fabric.microsoft.com/v1/workspaces/'
                    + CONVERT(nvarchar(36), @workspace_id) + N'/datapipelines/pipelineruns/'
                    + CONVERT(nvarchar(36), @execution_run_id) + N'/queryactivityruns';
        SELECT @task = task_json
        FROM ctl.run_task WITH (UPDLOCK, HOLDLOCK)
        WHERE run_id = @run_id AND task_id = @task_id AND status <> 'SUCCEEDED';
        IF @task IS NULL OR JSON_VALUE(@task, '$.layer') <> 'bronze'
            THROW 51140, 'Copy preparation requires an unfinished pinned Bronze task.', 1;
        IF EXISTS (
            SELECT 1 FROM ctl.task_attempt
            WHERE run_id = @run_id AND task_id = @task_id)
            THROW 51140, 'A prepared Copy cannot be rebound or blindly resubmitted; reconcile the original execution.', 1;
        SET @target_key = JSON_VALUE(@task, '$.target_lock_key');
        SET @source_key = JSON_VALUE(@task, '$.source_lock_key');
        DECLARE @keys TABLE (lease_key char(64) PRIMARY KEY);
        INSERT @keys(lease_key) VALUES (@target_key);
        IF @source_key IS NOT NULL AND @source_key <> @target_key
            INSERT @keys(lease_key) VALUES (@source_key);
        DECLARE @lease_key char(64), @expected bigint;
        WHILE EXISTS (SELECT 1 FROM @keys)
        BEGIN
            SELECT TOP (1) @lease_key = lease_key FROM @keys ORDER BY lease_key;
            SET @expected = NULL;
            SELECT @expected = fence FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK)
            WHERE target_key = @lease_key AND run_id = @run_id AND released = 0;
            EXEC ctl.usp_acquire_lease
                @run_id = @run_id, @lease_key = @lease_key,
                @expected_fence = @expected, @emit_result = 0;
            DELETE FROM @keys WHERE lease_key = @lease_key;
        END;
        SELECT @target_fence = fence FROM ctl.target_lease WHERE target_key = @target_key;
        SELECT @source_fence = fence FROM ctl.target_lease WHERE target_key = @source_key;
        EXEC ctl.usp_begin_attempt
            @run_id = @run_id, @task_id = @task_id, @attempt_id = @attempt_id,
            @fence = @target_fence, @source_fence = @source_fence, @emit_result = 0;
        UPDATE ctl.task_attempt SET job_location = @location
        WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id;
        COMMIT;
        SELECT @run_id AS run_id, @task_id AS task_id,
               CONVERT(varchar(36), @attempt_id) AS attempt_id,
               CONVERT(varchar(20), @target_fence) AS fence,
               CONVERT(varchar(20), @source_fence) AS source_fence;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_export_pipeline_dispatch
    @run_id varchar(128),
    @layer varchar(8),
    @include_payload bit = 1
AS
BEGIN
    SET NOCOUNT ON;
    IF @layer IS NULL OR @layer NOT IN ('bronze', 'silver', 'gold')
       OR @include_payload IS NULL
        THROW 51141, 'An explicit pinned pipeline layer is required.', 1;
    DECLARE @version varchar(128), @snapshot_hash char(64),
            @attempts nvarchar(max), @dispatch nvarchar(max), @count int,
            @undispatched nvarchar(max);
    SELECT @version = version_id, @snapshot_hash = snapshot_sha256
    FROM ctl.[run] WHERE run_id = @run_id;
    IF @version IS NULL
        THROW 51141, 'The pinned pipeline run does not exist.', 1;
    SELECT @count = COUNT(*) FROM ctl.run_task
    WHERE run_id = @run_id AND JSON_VALUE(task_json, '$.layer') = @layer;
    IF @count = 0 OR (@layer <> 'bronze' AND EXISTS (
        SELECT 1 FROM ctl.run_task AS task
        WHERE task.run_id = @run_id AND JSON_VALUE(task.task_json, '$.layer') = @layer
          AND NOT EXISTS (SELECT 1 FROM ctl.task_attempt AS attempt
              WHERE attempt.run_id = task.run_id AND attempt.task_id = task.task_id
                AND attempt.job_location IS NOT NULL)))
        THROW 51141, 'Pipeline dispatch omits prepared task execution scope.', 1;
    SELECT @undispatched = N'[' + COALESCE(
        STRING_AGG(CONVERT(nvarchar(max), N'"' + task.task_id + N'"'), N',')
            WITHIN GROUP (ORDER BY task.task_ordinal), N'') + N']'
    FROM ctl.run_task AS task
    WHERE task.run_id = @run_id AND JSON_VALUE(task.task_json, '$.layer') = @layer
      AND NOT EXISTS (SELECT 1 FROM ctl.task_attempt AS attempt
          WHERE attempt.run_id = task.run_id AND attempt.task_id = task.task_id);
    SET @attempts = REPLACE((
        SELECT CONVERT(varchar(36), attempt.attempt_id) AS attempt_id,
               attempt.fence AS fence, attempt.job_location AS job_location,
               attempt.source_fence AS source_fence, attempt.status AS status,
               task.task_id AS task_id
        FROM ctl.run_task AS task
        CROSS APPLY (SELECT TOP (1) value.attempt_id, value.fence, value.job_location,
                            value.source_fence, value.status
                     FROM ctl.task_attempt AS value
                     WHERE value.run_id = task.run_id AND value.task_id = task.task_id
                     ORDER BY value.started_utc DESC, value.attempt_id DESC) AS attempt
        WHERE task.run_id = @run_id AND JSON_VALUE(task.task_json, '$.layer') = @layer
        ORDER BY task.task_ordinal
        FOR JSON PATH, INCLUDE_NULL_VALUES), N'\/', N'/');
    SET @dispatch = REPLACE((
        SELECT JSON_QUERY(@attempts) AS attempts,
               CASE WHEN @layer = 'bronze' THEN 2 ELSE 1 END AS format_version,
               @layer AS layer, @run_id AS run_id, @snapshot_hash AS snapshot_sha256,
               JSON_QUERY(CASE WHEN @layer = 'bronze' THEN @undispatched END) AS undispatched_task_ids,
               @version AS version_id
        FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER), N'\/', N'/');
    IF DATALENGTH(CONVERT(varchar(max), @dispatch)) > 16777216
        THROW 51141, 'Pipeline dispatch exceeds the immutable transport bound.', 1;
    IF @include_payload = 1
        SELECT @dispatch AS dispatch_json;
    ELSE
        SELECT @run_id AS run_id, @layer AS layer, @count AS task_count,
               LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
                   CONVERT(varchar(max), @dispatch)), 2)) AS dispatch_sha256;
END;
GO
CREATE OR ALTER PROCEDURE ctl.usp_finish_bronze_dispatch
    @run_id varchar(128),
    @recovery_sha256 char(64)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @recovery_sha256 IS NULL OR LEN(@recovery_sha256) <> 64
       OR @recovery_sha256 COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
        THROW 51144, 'Bronze recovery requires its persisted observation hash.', 1;
    BEGIN TRANSACTION;
    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM ctl.run_task WITH (UPDLOCK, HOLDLOCK)
                       WHERE run_id = @run_id AND JSON_VALUE(task_json, '$.layer') = 'bronze')
            THROW 51144, 'No pinned Bronze tasks belong to this recovery.', 1;
        IF EXISTS (
            SELECT 1 FROM ctl.task_attempt AS attempt
            JOIN ctl.run_task AS task ON task.run_id = attempt.run_id AND task.task_id = attempt.task_id
            WHERE attempt.run_id = @run_id AND JSON_VALUE(task.task_json, '$.layer') = 'bronze'
              AND attempt.status IN ('SUBMISSION_PENDING', 'SUBMITTED', 'UNCERTAIN')
              AND (NOT EXISTS (SELECT 1 FROM ctl.target_lease AS lease WITH (UPDLOCK, HOLDLOCK)
                  WHERE lease.target_key = JSON_VALUE(task.task_json, '$.target_lock_key')
                    AND lease.run_id = @run_id AND lease.fence = attempt.fence AND lease.released = 0)
                OR (JSON_VALUE(task.task_json, '$.source_lock_key') IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM ctl.target_lease AS lease WITH (UPDLOCK, HOLDLOCK)
                      WHERE lease.target_key = JSON_VALUE(task.task_json, '$.source_lock_key')
                        AND lease.run_id = @run_id AND lease.fence = attempt.source_fence AND lease.released = 0))))
            THROW 51144, 'Unresolved Copy ownership must be reconciled before recovery.', 1;
        UPDATE attempt SET status = 'UNCERTAIN', submission_evidence_sha256 = @recovery_sha256
        FROM ctl.task_attempt AS attempt
        JOIN ctl.run_task AS task ON task.run_id = attempt.run_id AND task.task_id = attempt.task_id
        WHERE attempt.run_id = @run_id AND JSON_VALUE(task.task_json, '$.layer') = 'bronze'
          AND attempt.status IN ('SUBMISSION_PENDING', 'SUBMITTED', 'UNCERTAIN');
        UPDATE lease SET reconciliation_required = 1
        FROM ctl.target_lease AS lease
        WHERE lease.run_id = @run_id AND lease.released = 0 AND EXISTS (
            SELECT 1 FROM ctl.task_attempt AS attempt
            JOIN ctl.run_task AS task ON task.run_id = attempt.run_id AND task.task_id = attempt.task_id
            WHERE attempt.run_id = @run_id AND attempt.status = 'UNCERTAIN'
              AND ((lease.target_key = JSON_VALUE(task.task_json, '$.target_lock_key') AND lease.fence = attempt.fence)
                OR (lease.target_key = JSON_VALUE(task.task_json, '$.source_lock_key') AND lease.fence = attempt.source_fence)));
        UPDATE ctl.[run] SET status = 'ATTENTION_REQUIRED'
        WHERE run_id = @run_id AND EXISTS (
            SELECT 1 FROM ctl.run_task WHERE run_id = @run_id
              AND JSON_VALUE(task_json, '$.layer') = 'bronze' AND status <> 'SUCCEEDED');
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
GRANT EXECUTE ON OBJECT::ctl.usp_finish_bronze_dispatch TO runtime_executor;
GO
CREATE OR ALTER PROCEDURE ctl.usp_reconcile_pipeline_result
    @result_json nvarchar(max),
    @result_sha256 char(64),
    @observed_job_status varchar(32),
    @job_evidence_sha256 char(64),
    @job_location nvarchar(2000)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF COALESCE(ISJSON(@result_json), 0) <> 1 OR @job_location IS NULL
        THROW 51142, 'Pipeline reconciliation requires immutable result and execution scope.', 1;
    DECLARE @run_id varchar(128) = JSON_VALUE(@result_json, '$.run_id'),
            @task_id char(64) = JSON_VALUE(@result_json, '$.task_id'),
            @attempt_id uniqueidentifier = TRY_CONVERT(uniqueidentifier, JSON_VALUE(@result_json, '$.attempt_id')),
            @fence bigint = TRY_CONVERT(bigint, JSON_VALUE(@result_json, '$.fence')),
            @job_id varchar(128) = JSON_VALUE(@result_json, '$.job_id'),
            @status varchar(32);
    BEGIN TRANSACTION;
    BEGIN TRY
        SELECT @status = status FROM ctl.task_attempt WITH (UPDLOCK, HOLDLOCK)
        WHERE run_id = @run_id AND task_id = @task_id AND attempt_id = @attempt_id
          AND fence = @fence AND job_location = @job_location;
        IF @status IS NULL
            THROW 51142, 'Pipeline result differs from the reserved execution scope.', 1;
        IF @status IN ('SUBMISSION_PENDING', 'SUBMITTED', 'UNCERTAIN')
            EXEC ctl.usp_record_submission
                @run_id = @run_id, @task_id = @task_id, @attempt_id = @attempt_id,
                @fence = @fence, @submission_state = 'SUBMITTED',
                @evidence_sha256 = @job_evidence_sha256,
                @job_id = @job_id, @job_location = @job_location;
        EXEC ctl.usp_accept_result
            @result_json = @result_json, @result_sha256 = @result_sha256,
            @observed_job_status = @observed_job_status,
            @job_evidence_sha256 = @job_evidence_sha256;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
GRANT EXECUTE ON OBJECT::ctl.usp_prepare_copy_attempt TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_export_pipeline_dispatch TO runtime_executor;
GRANT EXECUTE ON OBJECT::ctl.usp_reconcile_pipeline_result TO runtime_executor;
GO
CREATE OR ALTER PROCEDURE ctl.usp_prepare_notebook_layer
    @run_id varchar(128),
    @layer varchar(8),
    @execution_run_id uniqueidentifier,
    @workspace_id uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @layer IS NULL OR @layer NOT IN ('silver', 'gold')
       OR @execution_run_id IS NULL OR @workspace_id IS NULL
        THROW 51143, 'Notebook preparation requires its layer and actual pipeline execution.', 1;
    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @tasks TABLE (task_id char(64) PRIMARY KEY, task_ordinal int, task_json nvarchar(max));
        INSERT @tasks(task_id, task_ordinal, task_json)
        SELECT task_id, task_ordinal, task_json FROM ctl.run_task WITH (UPDLOCK, HOLDLOCK)
        WHERE run_id = @run_id AND JSON_VALUE(task_json, '$.layer') = @layer;
        IF NOT EXISTS (SELECT 1 FROM @tasks)
            THROW 51143, 'No pinned notebook work belongs to this layer.', 1;
        IF EXISTS (
            SELECT 1 FROM ctl.task_attempt AS attempt
            JOIN @tasks AS task ON task.task_id = attempt.task_id
            WHERE attempt.run_id = @run_id)
            THROW 51143, 'Existing notebook attempts require service and output reconciliation before another dispatch.', 1;
        IF EXISTS (
            SELECT 1 FROM @tasks AS task
            CROSS APPLY OPENJSON(task.task_json, '$.dependencies')
            WITH (task_id char(64), condition varchar(32)) AS dependency
            LEFT JOIN ctl.run_task AS parent
              ON parent.run_id = @run_id AND parent.task_id = dependency.task_id
            WHERE parent.task_id IS NULL
               OR (JSON_VALUE(parent.task_json, '$.layer') = @layer
                   AND dependency.condition <> 'succeeded')
               OR (JSON_VALUE(parent.task_json, '$.layer') <> @layer
                   AND (parent.status <> 'SUCCEEDED' OR (
                       dependency.condition <> 'succeeded' AND NOT EXISTS (
                           SELECT 1 FROM ctl.task_result_inbox AS result
                           CROSS APPLY OPENJSON(result.result_json, '$.readiness') AS ready
                           WHERE result.run_id = @run_id AND result.task_id = dependency.task_id
                             AND result.reconciled_utc IS NOT NULL
                             AND ready.[value] = dependency.condition)))))
            THROW 51143, 'External publication prerequisites or delegated DAG conditions are unsatisfied.', 1;
        DECLARE @keys TABLE (lease_key char(64) PRIMARY KEY);
        INSERT @keys(lease_key)
        SELECT JSON_VALUE(task_json, '$.target_lock_key') FROM @tasks
        UNION
        SELECT JSON_VALUE(task_json, '$.source_lock_key') FROM @tasks
        WHERE JSON_VALUE(task_json, '$.source_lock_key') IS NOT NULL;
        DECLARE @lease_key char(64), @expected bigint;
        WHILE EXISTS (SELECT 1 FROM @keys)
        BEGIN
            SELECT TOP (1) @lease_key = lease_key FROM @keys ORDER BY lease_key;
            SET @expected = NULL;
            SELECT @expected = fence FROM ctl.target_lease WITH (UPDLOCK, HOLDLOCK)
            WHERE target_key = @lease_key AND run_id = @run_id AND released = 0;
            EXEC ctl.usp_acquire_lease
                @run_id = @run_id, @lease_key = @lease_key,
                @expected_fence = @expected, @emit_result = 0;
            DELETE FROM @keys WHERE lease_key = @lease_key;
        END;
        DECLARE @location nvarchar(2000) = N'https://api.fabric.microsoft.com/v1/workspaces/'
            + CONVERT(nvarchar(36), @workspace_id) + N'/datapipelines/pipelineruns/'
            + CONVERT(nvarchar(36), @execution_run_id) + N'/queryactivityruns';
        INSERT ctl.task_attempt (
            run_id, task_id, attempt_id, fence, source_fence, status, job_location)
        SELECT @run_id, task.task_id, NEWID(), target_lease.fence, source_lease.fence,
               'SUBMISSION_PENDING', @location
        FROM @tasks AS task
        JOIN ctl.target_lease AS target_lease
          ON target_lease.target_key = JSON_VALUE(task.task_json, '$.target_lock_key')
         AND target_lease.run_id = @run_id AND target_lease.released = 0
        LEFT JOIN ctl.target_lease AS source_lease
          ON source_lease.target_key = JSON_VALUE(task.task_json, '$.source_lock_key')
         AND source_lease.run_id = @run_id AND source_lease.released = 0;
        IF @@ROWCOUNT <> (SELECT COUNT(*) FROM @tasks)
            THROW 51143, 'Layer reservation lost target ownership.', 1;
        UPDATE task SET status = 'RUNNING'
        FROM ctl.run_task AS task JOIN @tasks AS selected ON selected.task_id = task.task_id
        WHERE task.run_id = @run_id;
        COMMIT;
        EXEC ctl.usp_export_pipeline_dispatch
            @run_id = @run_id, @layer = @layer, @include_payload = 0;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        THROW;
    END CATCH;
END;
GO
GRANT EXECUTE ON OBJECT::ctl.usp_prepare_notebook_layer TO runtime_executor;
GO
