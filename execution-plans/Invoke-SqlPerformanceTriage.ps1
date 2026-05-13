<#
.SYNOPSIS
  A unified diagnostic tool for SQL Server combining live sp_whoisactive data, Query Store telemetry, 
  and deep execution plan heuristics.

.DESCRIPTION
  This script acts as a one-stop-shop for troubleshooting active or recent SQL Server performance issues.
  Phase 1: Live Engine Diagnostics via sp_whoisactive (captures running queries, locks, waits, and live plans).
  Phase 2: Recent History via Query Store (captures top CPU/IO consumers from the last 1 hour).
  Phase 3: Deep Heuristic Analysis (extracts the XML plans from Phase 1 & 2 and feeds them into Get-SqlPlanInsights).

.REQUIREMENTS
  - dbatools PowerShell module
  - sp_whoisactive installed on the target instance (master or user DB)
  - executionPlanReport.v3.ps1--SUPERSEDED BY V4 available in the same directory

.EXAMPLE
  .\Invoke-SqlPerformanceTriage.ps1 -SqlInstance "PROD-SQL-01" -Database "StackOverflow" -MinDurationSeconds 10
#>

param(
    [Parameter(Mandatory=$true)][string]$SqlInstance,
    [Parameter(Mandatory=$true)][string]$Database,
    [int]$MinDurationSeconds = 5,
    [int]$TopQueryStoreQueries = 3,
    [string]$InsightsScriptPath = ".\Get-SqlPlanInsights.ps1",
    [pscredential]$SqlCredential
)

$ErrorActionPreference = "Stop"

# 1. Load the deep plan parser
if (-not (Test-Path $InsightsScriptPath)) {
    throw "Could not find the Get-SqlPlanInsights script at $InsightsScriptPath. Please verify the path."
}
. $InsightsScriptPath

# Working directory for temporary XML plans
$tempDir = Join-Path $env:TEMP "SqlDiagnosticPlans"
if (-not (Test-Path $tempDir)) { New-Item -ItemType Directory -Path $tempDir | Out-Null }
Remove-Item "$tempDir\*" -Include "*.sqlplan" -Force -Recurse -ErrorAction SilentlyContinue

$connectParams = @{
    SqlInstance = $SqlInstance
    Database = $Database
}
if ($SqlCredential) { $connectParams.SqlCredential = $SqlCredential }

Write-Host "`n=================================================================" -ForegroundColor Cyan
Write-Host " STARTING UNIFIED SQL PERFORMANCE TRIAGE " -ForegroundColor White
Write-Host " Instance: $SqlInstance | Database: $Database" -ForegroundColor White
Write-Host "=================================================================`n" -ForegroundColor Cyan

$analyzedPlans = @()

# ============================================================================
# PHASE 1: Live Engine Diagnostics (sp_whoisactive)
# ============================================================================
# ============================================================================
# PHASE 1: Live Engine Diagnostics (sp_whoisactive)
# ============================================================================
Write-Host "[*] Phase 1: Polling live engine via sp_whoisactive (Waits, Locks, Plans)..." -ForegroundColor Yellow

# Execute directly. No temp tables, no dynamic SQL, no swallowed result sets.
$whoIsActiveQuery = "EXEC master..sp_whoisactive @get_transaction_info = 1, @get_task_info = 2, @get_locks = 1, @get_plans = 1;"

try {
    # 1. Fetch ALL live activity directly into PowerShell objects
    $allLiveQueries = @(Invoke-DbaQuery @connectParams -Query $whoIsActiveQuery -WarningAction SilentlyContinue)
    
    # 2. Filter mathematically in PowerShell using the native SQL Server timestamps
    $liveQueries = @($allLiveQueries | Where-Object {
        $null -ne $_.start_time -and $null -ne $_.collection_time -and 
        ($_.collection_time - $_.start_time).TotalSeconds -ge $MinDurationSeconds
    })
    
    if ($liveQueries.Count -gt 0) {
        Write-Host "    -> Found $($liveQueries.Count) query/queries running longer than $MinDurationSeconds seconds." -ForegroundColor Green
        
        foreach ($lq in $liveQueries) {
            Write-Host "    -> [SPID: $($lq.session_id)] Wait Info: $($lq.wait_info) | Blocking SPID: $($lq.blocking_session_id)" -ForegroundColor DarkGray
            
            # Export the live execution plan
            if ($null -ne $lq.query_plan -and $lq.query_plan -ne '') {
                $planPath = Join-Path $tempDir "LivePlan_SPID_$($lq.session_id)_$(Get-Date -Format 'HHmmss').sqlplan"
                $lq.query_plan | Out-File -FilePath $planPath -Encoding utf8
                $analyzedPlans += [PSCustomObject]@{ Source = "Live (SPID $($lq.session_id))"; Path = $planPath }
            }
        }
    } else {
        Write-Host "    -> No queries currently exceeding the $MinDurationSeconds second threshold." -ForegroundColor DarkGray
    }
} catch {
    Write-Host "    [!] Failed to execute sp_whoisactive. Ensure it is installed on the instance. Error: $($_.Exception.Message)" -ForegroundColor Red
}

# ============================================================================
# PHASE 2: Recent History (Query Store)
# ============================================================================
Write-Host "`n[*] Phase 2: Polling Query Store for recent heavy hitters (Last 1 Hour)..." -ForegroundColor Yellow

$qdsQuery = @"
    SELECT TOP ($TopQueryStoreQueries)
        qsq.query_id,
        qsp.plan_id,
        qsq.query_hash,
        qsp.query_plan_hash,
        CAST(qsp.query_plan AS NVARCHAR(MAX)) AS query_plan,
        SUM(qsrs.count_executions) AS total_executions,
        MAX(qsrs.avg_cpu_time) / 1000.0 AS max_avg_cpu_ms,
        MAX(qsrs.avg_logical_io_reads) AS max_avg_reads
    FROM sys.query_store_query qsq
    JOIN sys.query_store_plan qsp ON qsq.query_id = qsp.query_id
    JOIN sys.query_store_runtime_stats qsrs ON qsp.plan_id = qsrs.plan_id
    JOIN sys.query_store_runtime_stats_interval qsrsi ON qsrs.runtime_stats_interval_id = qsrsi.runtime_stats_interval_id
    WHERE qsrsi.start_time >= DATEADD(HOUR, -1, GETUTCDATE())
    GROUP BY qsq.query_id, qsp.plan_id, qsq.query_hash, qsp.query_plan_hash, CAST(qsp.query_plan AS NVARCHAR(MAX))
    ORDER BY max_avg_cpu_ms DESC;
"@

try {
    $qdsQueries = Invoke-DbaQuery @connectParams -Query $qdsQuery
    
    if ($qdsQueries -and $qdsQueries.Count -gt 0) {
        Write-Host "    -> Extracted top $($qdsQueries.Count) expensive plans from QDS." -ForegroundColor Green
        
        foreach ($q in $qdsQueries) {
            Write-Host "    -> [QueryID: $($q.query_id) | PlanID: $($q.plan_id)] Executions: $($q.total_executions) | Avg CPU: $([math]::Round($q.max_avg_cpu_ms, 2))ms | Avg Reads: $($q.max_avg_reads)" -ForegroundColor DarkGray
            
            if ($null -ne $q.query_plan -and $q.query_plan -ne '') {
                $planPath = Join-Path $tempDir "QDSPlan_QID_$($q.query_id)_PID_$($q.plan_id).sqlplan"
                $q.query_plan | Out-File -FilePath $planPath -Encoding utf8
                $analyzedPlans += [PSCustomObject]@{ Source = "QDS (PlanID $($q.plan_id))"; Path = $planPath }
            }
        }
    } else {
         Write-Host "    -> No Query Store data found for the last hour, or QDS is not enabled." -ForegroundColor DarkGray
    }
} catch {
    Write-Host "    [!] Failed to query Query Store. Error: $($_.Exception.Message)" -ForegroundColor Red
}

# ============================================================================
# PHASE 3: Deep Heuristic Analysis
# ============================================================================
Write-Host "`n[*] Phase 3: Executing Deep Execution Plan Heuristics..." -ForegroundColor Yellow

if ($analyzedPlans.Count -eq 0) {
    Write-Host "    -> No execution plans were captured to analyze. Exiting." -ForegroundColor Yellow
} else {
    foreach ($plan in $analyzedPlans) {
        Write-Host "`n-----------------------------------------------------------------" -ForegroundColor Cyan
        Write-Host " ANALYZING PLAN: $($plan.Source)" -ForegroundColor White
        Write-Host "-----------------------------------------------------------------" -ForegroundColor Cyan
        
        # Invoke your uploaded function
        try {
            Get-SqlPlanInsights -Path $plan.Path -ServerInstance $SqlInstance -Database $Database -InspectDatabase -SqlCredential $SqlCredential
        } catch {
            Write-Host "    [!] Failed to parse plan $($plan.Path). Error: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
}

Write-Host "`n=================================================================" -ForegroundColor Cyan
Write-Host " TRIAGE COMPLETE " -ForegroundColor White
Write-Host " Temporary plans saved to: $tempDir" -ForegroundColor DarkGray
Write-Host "=================================================================`n" -ForegroundColor Cyan
