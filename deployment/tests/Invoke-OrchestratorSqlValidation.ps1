#!/usr/bin/env pwsh
# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
#Requires -Version 7.0
<#
.SYNOPSIS
    Validate the approved control database using rollback-only metadata fixtures.
.DESCRIPTION
    Resolves the OLTP database through Fabric, uses an encrypted Entra-token
    connection and never executes business-source or permission-assignment DML.
.PARAMETER ContextPath
    Private JSON with tenant_id, workspace_id and database_id.
.PARAMETER FixturePath
    Private generated canonical metadata fixtures.
.PARAMETER OutputPath
    Destination for non-sensitive validation results.
.EXAMPLE
    ./Invoke-OrchestratorSqlValidation.ps1 -ContextPath context.json -FixturePath fixtures.json -OutputPath results.json
.NOTES
    Requires explicit A2 approval; performs no database creation or deployment.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$ContextPath,
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$FixturePath,
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$OutputPath
)
$ErrorActionPreference = 'Stop'

#region Main Execution
if ($MyInvocation.InvocationName -ne '.') {
    $Connection = $null
    try {
        $Context = Get-Content -Raw -LiteralPath $ContextPath | ConvertFrom-Json
        $Fixtures = Get-Content -Raw -LiteralPath $FixturePath | ConvertFrom-Json
        foreach ($Property in @('tenant_id', 'workspace_id', 'database_id')) {
            $Parsed = [guid]::Empty
            if (-not [guid]::TryParse($Context.$Property, [ref]$Parsed)) {
                throw 'Invalid explicit control-database context'
            }
        }
        $Tenant = az account show --query tenantId -o tsv
        if ($LASTEXITCODE -ne 0 -or $Tenant.Trim() -ne $Context.tenant_id) {
            throw 'Azure CLI tenant differs from the approved context'
        }
        $Url = "https://api.fabric.microsoft.com/v1/workspaces/$($Context.workspace_id)/sqlDatabases/$($Context.database_id)"
        $Details = az rest --method get --resource 'https://api.fabric.microsoft.com' `
            --headers 'x-ms-fabric-skill=sqldb-cli' --url $Url -o json
        if ($LASTEXITCODE -ne 0) { throw 'Control-database discovery failed' }
        $Database = ($Details -join "`n") | ConvertFrom-Json
        if ($Database.displayName -ne 'db_orchestrator') {
            throw 'Refusing to validate a database outside db_orchestrator'
        }
        $Token = az account get-access-token --tenant $Context.tenant_id `
            --resource 'https://database.windows.net' --query accessToken -o tsv
        if ($LASTEXITCODE -ne 0) { throw 'SQL token acquisition failed' }
        Add-Type -AssemblyName System.Data
        $Server = $Database.properties.serverFqdn
        $Catalog = $Database.properties.databaseName
        $Connection = [System.Data.SqlClient.SqlConnection]::new(
            "Server=tcp:$Server;Database=$Catalog;Encrypt=True;TrustServerCertificate=False;Connect Timeout=60"
        )
        $Connection.AccessToken = $Token.Trim()
        $Token = $null
        $Connection.Open()
        $Guard = $Connection.CreateCommand()
        $Guard.CommandText = @'
SELECT CASE WHEN
    (SELECT COUNT(*) FROM cfg.active_configuration) = 0
    AND EXISTS (SELECT 1 FROM cfg.configuration_version
                WHERE version_id LIKE 'treasury-bootstrap-%' AND lifecycle_state='DRAFT')
    THEN 1 ELSE 0 END;
'@
        if ($Guard.ExecuteScalar() -ne 1) {
            throw 'Control database is not in the expected inactive draft state'
        }
        $FingerprintQuery = @'
SELECT LOWER(CONVERT(char(64), HASHBYTES('SHA2_256', CONVERT(varchar(max),
    (SELECT version_id, definition_sha256, lifecycle_state,
            LOWER(CONVERT(char(64), HASHBYTES('SHA2_256',
                CONVERT(varchar(max), definition_json)), 2)) AS actual_definition_hash
     FROM cfg.configuration_version ORDER BY version_id FOR JSON PATH)
)), 2));
'@
        $Before = $Connection.CreateCommand()
        $Before.CommandText = $FingerprintQuery
        $BeforeFingerprint = [string]$Before.ExecuteScalar()
        $Before.CommandText = 'SELECT COUNT(*) FROM cfg.configuration_version;'
        $BeforeVersions = [int]$Before.ExecuteScalar()
        $Before.CommandText = 'SELECT COUNT(*) FROM cfg.activation_history;'
        $BeforeHistory = [int]$Before.ExecuteScalar()
        $RoleQuery = @'
SELECT COUNT(*) FROM sys.database_role_members AS membership
JOIN sys.database_principals AS roles ON roles.principal_id=membership.role_principal_id
WHERE roles.name IN ('metadata_reader','metadata_editor','metadata_validator',
                     'config_approver','runtime_executor');
'@
        $Before.CommandText = $RoleQuery
        $BeforeRoles = [int]$Before.ExecuteScalar()
        $LedgerQuery = @'
SELECT (SELECT COUNT(*) FROM ctl.[run])
     + (SELECT COUNT(*) FROM ctl.run_task)
     + (SELECT COUNT(*) FROM ctl.task_attempt)
     + (SELECT COUNT(*) FROM ctl.target_lease)
     + (SELECT COUNT(*) FROM ctl.task_result_inbox);
'@
        $Before.CommandText = $LedgerQuery
        $BeforeLedger = [int]$Before.ExecuteScalar()
        if ($BeforeLedger -ne 0) {
            throw 'Rollback-only foundation validation requires an unused operational ledger'
        }
        $Passed = [System.Collections.Generic.List[string]]::new()
        foreach ($Fixture in $Fixtures) {
            $Command = $Connection.CreateCommand()
            $Command.CommandTimeout = 240
            $Preamble = @'
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRY
    BEGIN TRANSACTION;
    EXEC cfg.usp_register_version @version_id, @definition_json, @definition_sha256;
'@
            if ($Fixture.case_name.StartsWith('run_')) {
                if ($Fixture.case_name -ne 'run_unapproved') {
                    $Preamble += @'

    EXEC cfg.usp_validate_version @version_id, @definition_sha256, @validator_hash, @validation_json;
    EXEC cfg.usp_approve_version @version_id, @definition_sha256;
'@
                }
                if ($Fixture.case_name -eq 'run_prepare_idempotent') {
                    $Preamble += @'

    EXEC ctl.usp_prepare_run @run_id, @version_id, @definition_sha256,
         @observed_utc, NULL, NULL;
    IF (SELECT snapshot_sha256 FROM ctl.[run] WHERE run_id=@run_id) <> @expected_snapshot_sha256
    BEGIN
        DECLARE @actual_snapshot nvarchar(max) = (SELECT snapshot_json FROM ctl.[run] WHERE run_id=@run_id);
        DECLARE @difference nvarchar(128), @difference_message nvarchar(2048);
        SELECT TOP (1) @difference = expected.[key]
        FROM OPENJSON(@expected_snapshot_json) AS expected
        LEFT JOIN OPENJSON(@actual_snapshot) AS actual ON actual.[key]=expected.[key]
        WHERE actual.[key] IS NULL OR actual.[value]<>expected.[value]
           OR actual.[type]<>expected.[type]
        ORDER BY expected.[key];
        IF @difference = 'tasks'
            SELECT TOP (1) @difference = N'tasks[0].' + expected.[key]
            FROM OPENJSON(@expected_snapshot_json, '$.tasks[0]') AS expected
            LEFT JOIN OPENJSON(@actual_snapshot, '$.tasks[0]') AS actual ON actual.[key]=expected.[key]
            WHERE actual.[key] IS NULL OR actual.[value]<>expected.[value]
               OR actual.[type]<>expected.[type]
            ORDER BY expected.[key];
        SET @difference_message = N'Canonical SQL snapshot differs at '
            + COALESCE(@difference, N'raw serialization') + N'.';
        THROW 51102, @difference_message, 1;
    END;
'@
                } else {
                    $Preamble += @'

    EXEC ctl.usp_prepare_run @run_id, @version_id, @definition_sha256,
         @observed_utc, NULL, @expected_snapshot_sha256;
'@
                }
            }
            $Acquire = @'
    EXEC ctl.usp_acquire_lease @run_id, @target_key;
    EXEC ctl.usp_acquire_lease @run_id, @source_key;
'@
            $Begin = @'
    EXEC ctl.usp_begin_attempt @run_id, @task_id, @attempt_id, 1, 1;
'@
            $Submit = @'
    EXEC ctl.usp_record_submission @run_id, @task_id, @attempt_id, 1,
         'SUBMITTED', @evidence_sha256, @job_id;
'@
            $Accept = @'
    EXEC ctl.usp_accept_result @result_json, @result_sha256, 'Completed', @evidence_sha256;
'@
            $Action = switch ($Fixture.case_name) {
                'idempotent_registration' {
                    @'
    EXEC cfg.usp_register_version @version_id, @definition_json, @definition_sha256;
    IF (SELECT COUNT(*) FROM cfg.configuration_version WHERE version_id=@version_id) <> 1
        THROW 51990, 'Repeated registration duplicated the version.', 1;
    IF (SELECT COUNT(*) FROM cfg.data_object WHERE version_id=@version_id) <> 2
        THROW 51990, 'Repeated registration duplicated metadata objects.', 1;
'@
                }
                'changed_definition_rejected' {
                    'EXEC cfg.usp_register_version @version_id, @changed_json, @changed_sha256;'
                }
                'wrong_hash_rejected' { '' }
                'credentials_rejected' { '' }
                'failed_validation_rejected' {
                    'EXEC cfg.usp_validate_version @version_id, @definition_sha256, @validator_hash, @validation_json;'
                }
                'string_true_validation_rejected' {
                    'EXEC cfg.usp_validate_version @version_id, @definition_sha256, @validator_hash, @validation_json;'
                }
                { $_ -in @('unbound_connection_rejected', 'unverified_capability_rejected') -or $_ -like 'file_*' } {
                    @'
    EXEC cfg.usp_validate_version @version_id, @definition_sha256, @validator_hash, @validation_json;
    EXEC cfg.usp_approve_version @version_id, @definition_sha256;
    IF NOT EXISTS (SELECT 1 FROM cfg.configuration_version
                   WHERE version_id=@version_id AND lifecycle_state='APPROVED')
        THROW 51990, 'Version approval was not recorded.', 1;
'@
                }
                'unapproved_activation_rejected' {
                    'EXEC cfg.usp_activate_version @version_id, @definition_sha256, NULL;'
                }
                { $_ -in @('activation_rollback', 'stale_activation_rejected') } {
                    @'
    EXEC cfg.usp_validate_version @version_id, @definition_sha256, @validator_hash, @validation_json;
    EXEC cfg.usp_approve_version @version_id, @definition_sha256;
    EXEC cfg.usp_activate_version @version_id, @definition_sha256, @expected_active;
    IF (SELECT version_id FROM cfg.active_configuration WHERE singleton_id=1) <> @version_id
        THROW 51990, 'Activation did not select the exact approved version.', 1;
'@
                }
                'run_prepare_idempotent' {
                    @'
    EXEC ctl.usp_prepare_run @run_id, @version_id, @definition_sha256,
         @observed_utc, NULL, @expected_snapshot_sha256;
    IF (SELECT COUNT(*) FROM ctl.run_task WHERE run_id=@run_id) <> 2
        THROW 51990, 'Repeated preparation changed the pinned task membership.', 1;
'@
                }
                'run_prepare_changed' {
                    @'
    EXEC ctl.usp_prepare_run @run_id, @version_id, @definition_sha256,
         '2026-11-07T08:30:00Z', NULL, NULL;
'@
                }
                { $_ -in @('run_unapproved','run_wrong_plan_hash','run_disabled_dependency') } { '' }
                'run_bounded_pages' {
                    @'
    DECLARE @page TABLE (task_ordinal int, task_id char(64), task_json nvarchar(max));
    INSERT @page EXEC ctl.usp_task_page @run_id, 0, 1;
    IF (SELECT COUNT(*) FROM @page) <> 1
       OR NOT EXISTS (SELECT 1 FROM @page WHERE task_id=@task_id AND task_ordinal=1)
        THROW 51990, 'First bounded page differs from the pinned task.', 1;
    INSERT @page EXEC ctl.usp_task_page @run_id, 1, 1;
    IF (SELECT COUNT(*) FROM @page) <> 2
       OR NOT EXISTS (SELECT 1 FROM @page WHERE task_id=@downstream_task_id AND task_ordinal=2)
        THROW 51990, 'Task pagination omitted or duplicated an object.', 1;
'@
                }
                { $_ -in @('run_same_target_writer', 'run_expired_lease') } {
                    $Expiry = if ($_ -eq 'run_expired_lease') {
                        'UPDATE ctl.target_lease SET expires_utc=DATEADD(SECOND,-60,SYSUTCDATETIME()) WHERE target_key=@target_key;'
                    } else { '' }
                    @'
    EXEC ctl.usp_prepare_run @second_run_id, @version_id, @definition_sha256,
         @observed_utc, NULL, NULL;
    EXEC ctl.usp_acquire_lease @run_id, @target_key;
'@ + "`n" + $Expiry + @'

    EXEC ctl.usp_acquire_lease @second_run_id, @target_key;
'@
                }
                'run_stale_fence' {
                    $Acquire + @'

    EXEC ctl.usp_begin_attempt @run_id, @task_id, @attempt_id, 2, 1;
'@
                }
                'run_missing_source_lease' {
                    @'
    EXEC ctl.usp_acquire_lease @run_id, @target_key;
    EXEC ctl.usp_begin_attempt @run_id, @task_id, @attempt_id, 1, NULL;
'@
                }
                'run_dependency_not_ready' {
                    $Acquire + @'

    EXEC ctl.usp_acquire_lease @run_id, @downstream_target_key;
    EXEC ctl.usp_begin_attempt @run_id, @downstream_task_id, @attempt_id, 1, 1;
'@
                }
                'run_duplicate_attempt' {
                    $Acquire + $Begin + $Begin + @'

    IF (SELECT COUNT(*) FROM ctl.task_attempt WHERE run_id=@run_id) <> 1
        THROW 51990, 'Repeated attempt preparation duplicated a submission.', 1;
'@
                }
                { $_ -in @('run_uncertain_blocks_retry','run_uncertain_blocks_release') } {
                    $Acquire + $Begin + @'

    EXEC ctl.usp_record_submission @run_id, @task_id, @attempt_id, 1,
         'UNCERTAIN', @evidence_sha256, NULL;
'@ + $(if ($_ -eq 'run_uncertain_blocks_retry') {
                        "`nEXEC ctl.usp_begin_attempt @run_id, @task_id, @second_attempt_id, 1, 1;"
                    } else {
                        "`nEXEC ctl.usp_release_lease @run_id, @target_key, 1, @evidence_sha256;"
                    })
                }
                'run_definite_rejection_retry' {
                    $Acquire + $Begin + @'

    EXEC ctl.usp_record_submission @run_id, @task_id, @attempt_id, 1,
         'NOT_SUBMITTED', @evidence_sha256, NULL;
    EXEC ctl.usp_begin_attempt @run_id, @task_id, @second_attempt_id, 1, 1;
    IF (SELECT COUNT(*) FROM ctl.task_attempt WHERE run_id=@run_id) <> 2
        THROW 51990, 'A definite rejected submission could not be retried explicitly.', 1;
'@
                }
                'run_duplicate_result' {
                    $Acquire + $Begin + $Submit + $Accept + $Accept + @'

    IF (SELECT COUNT(*) FROM ctl.task_result_inbox WHERE run_id=@run_id) <> 1
       OR NOT EXISTS (SELECT 1 FROM ctl.run_task
                      WHERE run_id=@run_id AND task_id=@task_id AND status='SUCCEEDED')
        THROW 51990, 'Duplicate reconciliation changed task completion or result membership.', 1;
'@
                }
                'run_changed_result' {
                    $Acquire + $Begin + $Submit + $Accept + @'

    EXEC ctl.usp_accept_result @changed_result_json, @changed_result_sha256,
         'Completed', @evidence_sha256;
'@
                }
                'run_failed_blocks_downstream' {
                    $Acquire + $Begin + $Submit + @'

    EXEC ctl.usp_accept_result @result_json, @result_sha256, 'Failed', @evidence_sha256;
    EXEC ctl.usp_acquire_lease @run_id, @downstream_target_key;
    EXEC ctl.usp_begin_attempt @run_id, @downstream_task_id, @second_attempt_id, 1, 1;
'@
                }
                'run_released_lease_next_fence' {
                    @'
    EXEC ctl.usp_prepare_run @second_run_id, @version_id, @definition_sha256,
         @observed_utc, NULL, NULL;
    EXEC ctl.usp_acquire_lease @run_id, @target_key;
    EXEC ctl.usp_release_lease @run_id, @target_key, 1, @evidence_sha256;
    EXEC ctl.usp_acquire_lease @second_run_id, @target_key;
    IF NOT EXISTS (SELECT 1 FROM ctl.target_lease WHERE target_key=@target_key
                   AND run_id=@second_run_id AND fence=2 AND released=0)
        THROW 51990, 'Released ownership did not advance its fencing token.', 1;
'@
                }
                'run_job_conflict' {
                    $Acquire + $Begin + $Submit + @'

    EXEC ctl.usp_record_submission @run_id, @task_id, @attempt_id, 1,
         'SUBMITTED', @evidence_sha256, @second_attempt_id;
'@
                }
                { $_ -in @(
                    'run_success_requires_evidence','run_result_wrong_target','run_result_wrong_batch',
                    'run_result_bad_metrics','run_missing_publication_evidence'
                ) } { $Acquire + $Begin + $Submit + $Accept }
                'run_uncertain_job_resolved' {
                    $Acquire + $Begin + @'

    EXEC ctl.usp_record_submission @run_id, @task_id, @attempt_id, 1,
         'UNCERTAIN', @evidence_sha256, NULL;
'@ + $Submit + $Accept + @'

    IF EXISTS (SELECT 1 FROM ctl.target_lease WHERE run_id=@run_id
               AND reconciliation_required=1)
        THROW 51990, 'Recorded job recovery did not clear its exact reconciliation requirement.', 1;
'@
                }
                'run_invalid_page' { 'EXEC ctl.usp_task_page @run_id, 0, 5001;' }
                default { throw 'Unknown metadata-only SQL validation case' }
            }
            $Command.CommandText = $Preamble + "`n" + $Action + @'

    IF @expected_error IS NOT NULL
        THROW 51990, 'An expected rejection did not occur.', 1;
    ROLLBACK TRANSACTION;
END TRY
BEGIN CATCH
    DECLARE @actual_error int = ERROR_NUMBER();
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    IF @expected_error IS NULL OR @actual_error <> @expected_error
        THROW;
END CATCH;
IF @@TRANCOUNT <> 0
    THROW 51991, 'Validation left an open transaction.', 1;
'@
            foreach ($Name in @(
                'version_id', 'definition_sha256', 'changed_sha256', 'validator_hash', 'expected_active',
                'run_id', 'second_run_id', 'observed_utc', 'expected_snapshot_sha256',
                'task_id', 'downstream_task_id', 'target_key', 'downstream_target_key', 'source_key',
                'attempt_id', 'second_attempt_id', 'job_id', 'evidence_sha256',
                'result_sha256', 'changed_result_sha256'
            )) {
                $Parameter = $Command.Parameters.Add("@$Name", [System.Data.SqlDbType]::VarChar, 128)
                $Value = $Fixture.$Name
                if ($Name -eq 'observed_utc' -and $Value -is [datetime]) {
                    $Value = $Value.ToUniversalTime().ToString(
                        "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", [cultureinfo]::InvariantCulture
                    )
                }
                $Parameter.Value = if ($null -eq $Value) { [DBNull]::Value } else { [string]$Value }
            }
            foreach ($Name in @(
                'definition_json', 'changed_json', 'validation_json', 'result_json', 'changed_result_json',
                'expected_snapshot_json'
            )) {
                $Parameter = $Command.Parameters.Add("@$Name", [System.Data.SqlDbType]::NVarChar, -1)
                $Parameter.Value = [string]$Fixture.$Name
            }
            $Parameter = $Command.Parameters.Add('@expected_error', [System.Data.SqlDbType]::Int)
            $Parameter.Value = if ($null -eq $Fixture.expected_error) { [DBNull]::Value } else { [int]$Fixture.expected_error }
            [void]$Command.ExecuteNonQuery()
            $Passed.Add([string]$Fixture.case_name)
        }
        $After = $Connection.CreateCommand()
        $After.CommandText = @'
SELECT CASE WHEN
    (SELECT COUNT(*) FROM cfg.active_configuration)=0
    AND (SELECT COUNT(*) FROM cfg.activation_history)=@before_history
    AND (SELECT COUNT(*) FROM cfg.configuration_version)=@before_versions
    THEN 1 ELSE 0 END;
'@
        [void]$After.Parameters.AddWithValue('@before_history', $BeforeHistory)
        [void]$After.Parameters.AddWithValue('@before_versions', $BeforeVersions)
        if ($After.ExecuteScalar() -ne 1) {
            throw 'Validation changed persistent configuration, activation or role membership'
        }
        $Before.CommandText = $FingerprintQuery
        if ([string]$Before.ExecuteScalar() -ne $BeforeFingerprint) {
            throw 'Validation changed pre-existing metadata definitions or lifecycle states'
        }
        $Before.CommandText = $RoleQuery
        if ([int]$Before.ExecuteScalar() -ne $BeforeRoles) {
            throw 'Validation changed custom role membership'
        }
        $Before.CommandText = $LedgerQuery
        if ([int]$Before.ExecuteScalar() -ne $BeforeLedger) {
            throw 'Validation retained operational runs, attempts, leases or results'
        }
        @{
            status = 'passed'
            passed = $Passed.Count
            cases = @($Passed)
            active_configurations = 0
            fixture_versions_retained = 0
            custom_role_memberships = $BeforeRoles
            preserved_version_count = $BeforeVersions
            preserved_versions_sha256 = $BeforeFingerprint
            operational_fixture_rows_retained = 0
            permission_impersonation_tested = $false
        } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $OutputPath -Encoding utf8
        Write-Output "Control SQL validation passed: $($Passed.Count) rollback-only cases"
        exit 0
    }
    catch {
        $Inner = $_.Exception
        while ($null -ne $Inner.InnerException) { $Inner = $Inner.InnerException }
        $Code = if ($Inner -is [System.Data.SqlClient.SqlException]) { $Inner.Number } else { 'not_sql' }
        $Detail = if ($Inner -is [System.Data.SqlClient.SqlException] -and $Code -ge 51000 -and $Code -lt 52000) {
            $Inner.Message
        } else { 'Native or client error; inspect the approved private context.' }
        Write-Error -ErrorAction Continue "Control SQL validation failed; error code=$Code; case=$($Fixture.case_name); detail=$Detail"
        exit 1
    }
    finally {
        if ($null -ne $Connection) { $Connection.Dispose() }
        $Token = $null
    }
}
#endregion Main Execution
