#!/usr/bin/env pwsh
# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
#Requires -Version 7.0
<#
.SYNOPSIS
    Validate or deploy the KZN Treasury Bronze and Silver accelerator.
.DESCRIPTION
    Local validation does not authenticate. Cloud deployment requires an explicit
    workspace and preauthorized source connections. No pipelines are started.
.PARAMETER Workspace
    Existing Fabric workspace name or ID.
.PARAMETER BasSqlConnection
    BAS FabricSql OAuth2 connection name or ID.
.PARAMETER CsdSqlConnection
    CSD FabricSql OAuth2 connection name or ID.
.PARAMETER PersalSqlConnection
    PERSAL FabricSql OAuth2 connection name or ID.
.PARAMETER ValidateLocal
    Compile all assets and validate seeds without accessing Fabric.
.PARAMETER Overwrite
    Update only compatible existing owned items.
.PARAMETER DryRun
    Run cloud preflight without creating or updating workspace items.
.PARAMETER BootstrapOrchestrator
    Bootstrap only db_orchestrator and an inactive draft; never reseed source databases.
.PARAMETER MetadataAssets
    Publish gated reusable metadata assets without activating configuration or reseeding sources.
    Requires BootstrapOrchestrator and a StateFile for mutating publication.
.PARAMETER OrchestratorSqlConnection
    Dedicated FabricSql OAuth2 control connection name or ID.
.PARAMETER Interactive
    Guide connection setup and recheck in a TTY.
.PARAMETER StateFile
    Persistent control bootstrap operation journal.
.PARAMETER Resume
    Reconcile an existing control bootstrap journal before further changes.
.EXAMPLE
    .\deployment\deploy.ps1 -ValidateLocal
.EXAMPLE
    .\deployment\deploy.ps1 -Workspace ws_kzn_treasury -DryRun
.EXAMPLE
    .\deployment\deploy.ps1 -Workspace ws_kzn_treasury -BootstrapOrchestrator -MetadataAssets -DryRun
.NOTES
    Python 3.11-3.13 is required. Deployment and pipeline execution are separate.
#>
[CmdletBinding()]
param(
    [string]$Workspace,
    [string]$BasSqlConnection = "conn_bas_finance_sql",
    [string]$CsdSqlConnection = "conn_csd_finance_sql",
    [string]$PersalSqlConnection = "conn_persal_finance_sql",
    [switch]$ValidateLocal,
    [switch]$Overwrite,
    [switch]$DryRun,
    [switch]$BootstrapOrchestrator,
    [switch]$MetadataAssets,
    [string]$OrchestratorSqlConnection = "conn_orchestrator_sql",
    [switch]$Interactive,
    [string]$StateFile,
    [switch]$Resume
)

$ErrorActionPreference = 'Stop'

#region Main Execution
if ($MyInvocation.InvocationName -ne '.') {
    try {
        $Arguments = @((Join-Path $PSScriptRoot 'deploy.py'))
        if ($BootstrapOrchestrator) {
            $Arguments += '--bootstrap-orchestrator'
        }
        if ($MetadataAssets) { $Arguments += '--metadata-assets' }
        $Arguments += @('--orchestrator-sql-connection', $OrchestratorSqlConnection)
        if ($Interactive) { $Arguments += '--interactive' }
        if ($StateFile) { $Arguments += @('--state-file', $StateFile) }
        if ($Resume) { $Arguments += '--resume' }
        if ($ValidateLocal) {
            if ($Workspace -or $Overwrite -or $DryRun) {
                throw 'ValidateLocal cannot be combined with cloud deployment options.'
            }
            $Arguments += '--validate-local'
        }
        else {
            if ([string]::IsNullOrWhiteSpace($Workspace)) {
                throw 'Workspace is required unless ValidateLocal is selected.'
            }
            $Arguments += @(
                '--workspace', $Workspace,
                '--bas-sql-connection', $BasSqlConnection,
                '--csd-sql-connection', $CsdSqlConnection,
                '--persal-sql-connection', $PersalSqlConnection
            )
            if ($Overwrite) { $Arguments += '--overwrite' }
            if ($DryRun) { $Arguments += '--dry-run' }
        }
        & python @Arguments
        exit $LASTEXITCODE
    }
    catch {
        Write-Error -ErrorAction Continue $_
        exit 1
    }
}
#endregion Main Execution
