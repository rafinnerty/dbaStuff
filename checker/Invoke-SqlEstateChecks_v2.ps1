<#
.SYNOPSIS
    Runs daily health, configuration and security checks across one or more SQL Server instances.

.DESCRIPTION
    A generic, JSON-emitting estate monitoring script. Instances are supplied as a
    comma-separated list. Estate topology (volume hosts, uptime hosts, log-shipping pairs,
    thresholds, email) is optional and lives in an external JSON config file.

    Every check records a structured result object. Those objects drive both the coloured
    console output and an optional JSON export (-OutputJsonPath) for a companion HTML visualizer.

    Status values: OK | Warning | Critical | Info

.PARAMETER Instances
    Comma-separated list of SQL Server instances to check. Default: ssms\win11

.PARAMETER Exclude
    A single instance name to drop from the list.

.PARAMETER ConfigPath
    Optional. Path to estate config JSON (volume hosts, uptime hosts, log shipping, thresholds,
    email). If omitted, only the per-instance checks run, using built-in default thresholds.

.PARAMETER OutputJsonPath
    Optional. If supplied, results are written here as JSON for the visualizer.

.PARAMETER SqlCredential
    Optional PSCredential used to authenticate to SQL Server. Required for SQL-auth or
    Azure SQL Database / Managed Instance (including Azure AD password auth). Windows-auth
    on-prem instances need no credential, so run Azure instances in a separate invocation.

.PARAMETER Quiet
    Suppress console output (useful when only the JSON export is wanted).

.NOTES
    Author        : Richard Armstrong-Finnerty
    Prerequisite  : dbatools module
    License       : MIT

.EXAMPLE
    .\Invoke-SqlEstateChecks.ps1
    Run all per-instance checks against the default local instance (ssms\win11).

.EXAMPLE
    .\Invoke-SqlEstateChecks.ps1 "SQL-PRD1\INST1,1433,SQL-PRD2\INST2,1433" -OutputJsonPath .\results.json
    Run two instances and export results for the visualizer.

.EXAMPLE
    .\Invoke-SqlEstateChecks.ps1 -Instances "SQL-PRD1" -ConfigPath .\estate-config.json -OutputJsonPath .\results.json
    Run with full estate topology (volumes, hosts, log shipping) from config.

.EXAMPLE
    $cred = Get-Credential
    .\Invoke-SqlEstateChecks.ps1 -Instances "myserver.database.windows.net" -SqlCredential $cred -OutputJsonPath .\azure.json
    Run against an Azure SQL Database using a SQL / Azure AD credential.
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)] [string]$Instances = 'ssms\win11',
    [Parameter(Position = 1)] [string]$Exclude = '',
    [Parameter()] [string]$ConfigPath = '',
    [Parameter()] [string]$OutputJsonPath = '',
    [Parameter()] [pscredential]$SqlCredential,
    [Parameter()] [switch]$Quiet
)

#region --------------------------------------------------------------- Setup

Write-Output "v2"

$Config = $null
if ($ConfigPath) {
    if (-not (Test-Path -LiteralPath $ConfigPath)) { throw "Config file not found: $ConfigPath" }
    try   { $Config = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "Failed to parse config '$ConfigPath': $($_.Exception.Message)" }
}

$thresholds = if ($Config) { $Config.thresholds } else { $null }

# Instance list: comma-separated, trimmed, de-blanked, with -Exclude removed.
$instanceList = @($Instances -split ',' |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and $_ -ne $Exclude })

if ($instanceList.Count -eq 0) { throw "No instances to check." }

# Transcript (only if a path is configured).
$transcriptLog = $null
if ($Config -and $Config.transcriptPath) {
    if (-not (Test-Path -LiteralPath $Config.transcriptPath)) {
        New-Item -ItemType Directory -Path $Config.transcriptPath -Force | Out-Null
    }
    $transcriptLog = Join-Path $Config.transcriptPath ("EstateChecks_" + (Get-Date -Format 'dd-MM-yyyy_HH_mm_ss') + ".log")
    Start-Transcript -Path $transcriptLog | Out-Null
}

$start     = Get-Date
$timestamp = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')

# Single ordered collection that every check writes into.
$script:Results = [System.Collections.Generic.List[object]]::new()

#endregion

#region --------------------------------------------------------------- Helpers

function Get-StatusColor {
    param([string]$Status)
    switch ($Status) {
        'OK'       { 'Green' }
        'Critical' { 'Red' }
        'Warning'  { 'DarkYellow' }
        'Info'     { 'Magenta' }
        default    { 'Gray' }
    }
}

function Add-Result {
    param(
        [string]   $Section,                 # Volumes | LogShipping | Hosts | Instance
        [string]   $Instance = '',
        [string]   $HostName = '',
        [string]   $Category = '',
        [string]   $Name,
        [ValidateSet('OK', 'Warning', 'Critical', 'Info')]
        [string]   $Status,
        [string[]] $Details  = @(),
        [object]   $Value    = $null,
        [string]   $Unit     = '',
        [string]   $Message  = ''
    )

    $script:Results.Add([pscustomobject]@{
        Section  = $Section
        Instance = $Instance
        Host     = $HostName
        Category = $Category
        Name     = $Name
        Status   = $Status
        Details  = @($Details)
        Value    = $Value
        Unit     = $Unit
    })

    if ($Quiet) { return }

    if ($Name)  { Write-Host $Name -ForegroundColor White }
    $color = Get-StatusColor $Status
    if ($Details.Count -gt 0) {
        $Details | ForEach-Object { Write-Host "    $_" -ForegroundColor $color }
    }
    else {
        $line = if ($Message) { $Message } elseif ($null -ne $Value) { "$Value $Unit".Trim() } else { $Status }
        Write-Host "    $line" -ForegroundColor $color
    }
}

function Add-EntityCheck {
    # The common pattern: "no entity should be in state X". Pass the offending objects;
    # any present -> failure listing their names, otherwise OK.
    param(
        [string] $Instance,
        [string] $Category,
        [string] $Name,
        $Offenders,
        [string] $Property   = 'Name',
        [ValidateSet('OK', 'Warning', 'Critical', 'Info')]
        [string] $FailStatus = 'Critical'
    )
    $names = @()
    if ($null -ne $Offenders) {
        $names = @($Offenders | Sort-Object $Property | ForEach-Object { [string]$_.$Property })
    }
    if ($names.Count -gt 0) {
        Add-Result -Section 'Instance' -Instance $Instance -Category $Category -Name $Name -Status $FailStatus -Details $names
    }
    else {
        Add-Result -Section 'Instance' -Instance $Instance -Category $Category -Name $Name -Status 'OK'
    }
}

function Get-NowFor {
    param([string]$TimeZone)
    if ($TimeZone) { [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId((Get-Date), $TimeZone) }
    else           { Get-Date }
}

function Format-Uptime {
    param([TimeSpan]$Span)
    "$($Span.Days)d $($Span.Hours)h $($Span.Minutes)m"
}

function Get-Threshold {
    param($Object, [string]$Name, $Default)
    if ($null -ne $Object -and $null -ne $Object.$Name) { $Object.$Name } else { $Default }
}

#endregion

#region --------------------------------------------------------------- T-SQL

$restoresTsql = @"
WITH [LastRestores] AS
(
    SELECT
        [d].[name] AS [DatabaseName],
        [r].[restore_date],
        RowNum = ROW_NUMBER() OVER (PARTITION BY [d].[Name] ORDER BY [r].[restore_date] DESC)
    FROM [master].[sys].[databases] [d] WITH (NOLOCK)
    LEFT OUTER JOIN [msdb].[dbo].[restorehistory] [r] ON [r].[destination_database_name] = [d].[Name]
    WHERE [d].[state_desc] != 'restoring'
    AND [d].[name] NOT LIKE '### TEST RESTORE ###%'
    AND [d].[name] NOT IN (SELECT [sd].[secondary_database] FROM [msdb].[dbo].[log_shipping_secondary_databases] [sd])
    AND [d].[name] NOT IN ('master', 'tempdb')
    AND [r].[restore_date] > DATEADD(DAY, -1, GETDATE())
)
SELECT [DatabaseName]
FROM [LastRestores] WITH (NOLOCK)
WHERE [RowNum] = 1
AND [restore_date] >= (GETDATE() - 1)
"@

$lastVmBackupQuery = @"
SELECT FORMAT(MAX([backup_start_date]), 'dd-MMM-yyyy HH:mm:ss') [LatestType7Backup]
FROM [msdb].[dbo].[backupmediafamily]
INNER JOIN [msdb].[dbo].[backupset]
    ON [msdb].[dbo].[backupmediafamily].[media_set_id] = [msdb].[dbo].[backupset].[media_set_id]
WHERE (CONVERT(datetime, [backup_start_date], 102) >= GETDATE() - 7)
AND [msdb].[dbo].[backupmediafamily].[device_type] = 7;
"@

$sqlUptime        = "SELECT SQL_Server_Start_Time = MIN([login_time]) FROM [sysprocesses] WITH (NOLOCK)"
$current_ram_gb   = "SELECT [counter_name], [cntr_value]/1024/1024.00 AS [GB RAM] FROM [sys].[dm_os_performance_counters] WHERE [counter_name] LIKE '%Total Server%';"
$totalCpuCores    = "SELECT COUNT(*) [totalCpuCores] FROM [sys].[dm_os_schedulers] WHERE [scheduler_id] < 1000;"
$unusableCpuCores = "SELECT COUNT(*) [unusableCpuCores] FROM [sys].[dm_os_schedulers] WHERE [scheduler_id] < 1000 AND [status] <> 'VISIBLE ONLINE';"
$startupProcQuery = "SELECT [name] FROM sysobjects WITH (NOLOCK) WHERE type = 'P' AND OBJECTPROPERTY(id, 'ExecIsStartUp') = 1 AND [name] <> 'sp_ssis_startup'"
$percentGrowthQuery = "SELECT DISTINCT [d].[Name] FROM [sys].[master_files] [f] JOIN [sys].[databases] [d] ON [f].[database_id] = [d].[database_id] WHERE [f].[is_percent_growth] = 1 ORDER BY [d].[name]"

$runningJobsQuery = @"
SELECT j.name AS job_name, ja.start_execution_date
FROM msdb.dbo.sysjobactivity ja
LEFT JOIN msdb.dbo.sysjobhistory jh ON ja.job_history_id = jh.instance_id
JOIN msdb.dbo.sysjobs j  ON ja.job_id = j.job_id
JOIN msdb.dbo.sysjobsteps js ON ja.job_id = js.job_id AND ISNULL(ja.last_executed_step_id,0)+1 = js.step_id
WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY session_id DESC)
AND start_execution_date IS NOT NULL
AND stop_execution_date IS NULL;
"@

$ssisThreshold = Get-Threshold $thresholds 'ssisLongRunningSeconds' 7200
$ssis_running_processes_TSQL = @"
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'SSISDB' AND state_desc = 'ONLINE')
BEGIN
    IF OBJECT_ID('tempdb..#ssis_monitor_configure') IS NOT NULL DROP TABLE #ssis_monitor_configure;
    CREATE TABLE #ssis_monitor_configure(
        [package_name] [nvarchar](100) NOT NULL,
        [environment_name] [nvarchar](100) NULL,
        [threshold_time_sec] [int] NULL,
        [monitored] [bit] NULL ) ON [primary];
    INSERT INTO #ssis_monitor_configure ([package_name],[environment_name],[threshold_time_sec],[monitored])
        SELECT DISTINCT p.name, NULL, $ssisThreshold, 0
        FROM ssisdb.internal.packages p (NOLOCK);
    SELECT COUNT(*) AS [COUNT]
    FROM ssisdb.catalog.executions(nolock) ex
    JOIN #ssis_monitor_configure(nolock) smc ON ex.package_name = smc.package_name
    WHERE ex.end_time IS NULL AND ex.status = 2
    AND (ex.environment_name = smc.environment_name OR smc.environment_name IS NULL)
    AND (DATEDIFF(SECOND, CAST(ex.start_time AS datetime2), SYSDATETIME()) > smc.threshold_time_sec);
END
ELSE
BEGIN
    SELECT 0 AS [COUNT];
END
"@

#endregion

if (-not $Quiet) {
    Write-Output "`n************************************************`n$timestamp`n************************************************`n"
    Write-Host "Instances: $($instanceList -join ', ')" -ForegroundColor Yellow
    if ($Exclude) { Write-Host "Excluded: $Exclude" -ForegroundColor Yellow }
}

#region --------------------------------------------------------------- Volume checks (config-driven)

if ($Config -and $Config.volumeHosts) {
    $volThresholds = $Config.volumeFreeSpaceThresholds
    $warnPct = Get-Threshold $volThresholds 'warning'  30
    $highPct = Get-Threshold $volThresholds 'high'     20
    $critPct = Get-Threshold $volThresholds 'critical' 10

    foreach ($volHost in @($Config.volumeHosts)) {
        $hostName = if ($volHost -is [string]) { $volHost } else { $volHost.name }
        if (-not $Quiet) { Write-Host "`n$hostName - storage % remaining" -ForegroundColor Yellow }

        try {
            $volumes = Invoke-Command -ComputerName $hostName -ScriptBlock {
                Get-Volume | Where-Object { $_.FileSystem -eq 'NTFS' -and $_.Size -gt 0 } | ForEach-Object {
                    [pscustomobject]@{
                        Label        = $_.FileSystemLabel
                        PctRemaining = [math]::Round((($_.SizeRemaining / 1GB) / ($_.Size / 1GB)) * 100, 2)
                    }
                }
            } -ErrorAction Stop

            foreach ($v in ($volumes | Sort-Object Label)) {
                $status =
                    if     ($v.PctRemaining -le $critPct) { 'Critical' }
                    elseif ($v.PctRemaining -le $highPct) { 'Warning'  }
                    elseif ($v.PctRemaining -lt $warnPct) { 'Warning'  }
                    else                                  { 'OK'       }
                Add-Result -Section 'Volumes' -HostName $hostName -Category 'Storage' `
                           -Name ([string]$v.Label) -Status $status -Value $v.PctRemaining -Unit '%'
            }
        }
        catch {
            Add-Result -Section 'Volumes' -HostName $hostName -Category 'Storage' `
                       -Name 'Volume query' -Status 'Critical' -Message "Unable to query volumes: $($_.Exception.Message)"
        }
    }
}

#endregion

if (-not $Quiet) { Write-Output "`nRefreshing Cumulative Update index" }
try { Update-DbaBuildReference -ErrorAction Stop | Out-Null } catch { }

#region --------------------------------------------------------------- Log shipping (config-driven)

if ($Config -and $Config.logShipping) {
    if (-not $Quiet) { Write-Output "`nLog Shipping" }
    foreach ($pair in @($Config.logShipping)) {
        foreach ($side in @(
            @{ Role = 'Primary';   Instance = $pair.primary   },
            @{ Role = 'Secondary'; Instance = $pair.secondary }
        )) {
            if (-not $side.Instance) { continue }
            try {
                $lsSplat = @{ SqlInstance = $side.Instance; ErrorAction = 'Stop' }
                if ($SqlCredential) { $lsSplat.SqlCredential = $SqlCredential }
                $rows = if ($side.Role -eq 'Primary') {
                    Test-DbaDbLogShipStatus @lsSplat -Primary
                } else {
                    Test-DbaDbLogShipStatus @lsSplat -Secondary
                }
                foreach ($r in ($rows | Select-Object SqlInstance, Database, Status)) {
                    $st = if ($r.Status -ne 'All OK') { 'Critical' } else { 'OK' }
                    Add-Result -Section 'LogShipping' -Instance ([string]$side.Instance) -Category $side.Role `
                               -Name ([string]$r.Database) -Status $st -Message ([string]$r.Status)
                }
            }
            catch {
                Add-Result -Section 'LogShipping' -Instance ([string]$side.Instance) -Category $side.Role `
                           -Name 'Log shipping status' -Status 'Critical' -Message $_.Exception.Message
            }
        }
    }
}

#endregion

#region --------------------------------------------------------------- Host uptime (config-driven)

if ($Config -and $Config.uptimeHosts) {
    if (-not $Quiet) { Write-Host "`nHosts uptime" -ForegroundColor White }
    foreach ($h in @($Config.uptimeHosts)) {
        $hostName = if ($h -is [string]) { $h } else { $h.name }
        $hostTz   = if ($h -is [string]) { $null } else { $h.timeZone }
        try {
            $boot = (Get-CimInstance -ComputerName $hostName -ClassName Win32_OperatingSystem -ErrorAction Stop).LastBootUpTime
            $span = New-TimeSpan -Start $boot -End (Get-NowFor $hostTz)
            $status = if ($span.TotalDays -lt 1) { 'Critical' } else { 'OK' }
            Add-Result -Section 'Hosts' -HostName $hostName -Category 'Uptime' `
                       -Name $hostName -Status $status -Value ([math]::Round($span.TotalDays, 2)) -Unit 'days' `
                       -Message (Format-Uptime $span)
        }
        catch {
            Add-Result -Section 'Hosts' -HostName $hostName -Category 'Uptime' `
                       -Name $hostName -Status 'Critical' -Message "Unable to query uptime: $($_.Exception.Message)"
        }
    }
}

#endregion

#region --------------------------------------------------------------- Per-instance checks

$targetCompat = Get-Threshold $thresholds 'compatibilityLevel' 150
$fullHours    = Get-Threshold $thresholds 'fullBackupHours' 170
$diffWeekday  = Get-Threshold $thresholds 'diffBackupHoursWeekday' 26
$diffMonday   = Get-Threshold $thresholds 'diffBackupHoursMonday'  50
$logMins      = Get-Threshold $thresholds 'logBackupMinutes' 20
$maxCuBehind  = Get-Threshold $thresholds 'maxCuBehind' '1CU'   # '1CU' = OK up to one CU behind; use Test-DbaBuild -Latest for strict

foreach ($instance in $instanceList) {

    if (-not $Quiet) { Write-Host "`n=== $instance ===" -ForegroundColor Yellow }

    # --- Connectivity ---
    # Connect once and reuse the server object for every check below. The credential
    # (if supplied) is baked into the connection, so it covers Azure SQL / Azure AD too.
    try {
        $connectSplat = @{ SqlInstance = $instance }
        if ($SqlCredential) { $connectSplat.SqlCredential = $SqlCredential }
        $server = Connect-DbaInstance @connectSplat -ErrorAction Stop
        Add-Result -Section 'Instance' -Instance $instance -Category 'Connectivity' -Name 'Connectivity check' -Status 'OK'
    }
    catch {
        Add-Result -Section 'Instance' -Instance $instance -Category 'Connectivity' -Name 'Connectivity check' -Status 'Critical' -Message $_.Exception.Message
        continue
    }

    # --- Edition / physical host (informational) ---
    try {
        $edition  = (Get-DbaInstanceProperty -SqlInstance $server -InstanceProperty Edition).Value
        $physical = (Get-DbaInstanceProperty -SqlInstance $server -InstanceProperty ComputerNamePhysicalNetBIOS).Value
        Add-Result -Section 'Instance' -Instance $instance -HostName ([string]$physical) -Category 'Info' -Name 'Edition' -Status 'Info' -Message ([string]$edition)
    } catch { }

    # --- Instance uptime (local server time) ---
    try {
        $started = (Invoke-DbaQuery -SqlInstance $server -Query $sqlUptime).SQL_Server_Start_Time
        $span = New-TimeSpan -Start $started -End (Get-Date)
        $st = if ($span.TotalDays -lt 1) { 'Warning' } else { 'OK' }
        Add-Result -Section 'Instance' -Instance $instance -Category 'Info' -Name 'Instance uptime' -Status $st -Value ([math]::Round($span.TotalDays,2)) -Unit 'days' -Message (Format-Uptime $span)
    } catch { }

    # --- Build / CU level ---
    try {
        $build  = Test-DbaBuild -SqlInstance $server -MaxBehind $maxCuBehind
        $cuText = "$($build.NameLevel) $($build.SPLevel) $($build.CULevel) (build $($build.BuildLevel))".Trim()
        if ($build.Compliant) {
            Add-Result -Section 'Instance' -Instance $instance -Category 'Build' `
                -Name "SQL build within $maxCuBehind of latest" -Status 'OK' -Message $cuText
        } else {
            $supEnd = if ($build.SupportedUntil) { $build.SupportedUntil.ToString('yyyy-MM-dd') } else { 'n/a' }
            Add-Result -Section 'Instance' -Instance $instance -Category 'Build' `
                -Name "SQL build within $maxCuBehind of latest" -Status 'Warning' `
                -Message "$cuText | target $($build.BuildTarget); supported until $supEnd"
        }
    } catch {
        Add-Result -Section 'Instance' -Instance $instance -Category 'Build' -Name 'SQL build check' `
            -Status 'Critical' -Message $_.Exception.Message
    }

    # --- RAM / CPU (informational) ---
    try {
        $ram = [math]::Round((Invoke-DbaQuery -SqlInstance $server -Query $current_ram_gb).'GB RAM', 2)
        Add-Result -Section 'Instance' -Instance $instance -Category 'Info' -Name 'Current used RAM' -Status 'Info' -Value $ram -Unit 'GB'
    } catch { }
    try {
        $cores = (Invoke-DbaQuery -SqlInstance $server -Query $totalCpuCores).totalCpuCores
        Add-Result -Section 'Instance' -Instance $instance -Category 'Info' -Name 'CPU cores available' -Status 'Info' -Value $cores -Unit 'cores'
        $unusable = (Invoke-DbaQuery -SqlInstance $server -Query $unusableCpuCores).unusableCpuCores
        if ($unusable -ne 0) {
            Add-Result -Section 'Instance' -Instance $instance -Category 'Info' -Name 'CPU unusable cores' -Status 'Critical' -Value $unusable -Unit 'cores'
        }
    } catch { }

    # --- Security configuration ---
    try {
        Add-EntityCheck -Instance $instance -Category 'Security' -Name 'No Windows BUILTIN groups should be Logins' `
            -Offenders (Get-DbaLogin -SqlInstance $server | Where-Object { $_.Name -like 'BUILTIN*' })
    } catch { }

    try {
        $cfg = Get-DbaSpConfigure -SqlInstance $server
        $cfgChecks = @(
            @{ Cfg = 'CrossDBOwnershipChaining';       Name = 'Cross DB Ownership Chaining should be disabled' },
            @{ Cfg = 'OleAutomationProceduresEnabled'; Name = 'OLE Automation Procedures should be disabled' },
            @{ Cfg = 'RemoteDacConnectionsEnabled';    Name = 'Remote DAC should be disabled' },
            @{ Cfg = 'AdHocDistributedQueriesEnabled'; Name = 'Ad Hoc Distributed Queries should be disabled' },
            @{ Cfg = 'XPCmdShellEnabled';              Name = 'XP Command Shell should be disabled' }
        )
        foreach ($c in $cfgChecks) {
            $enabled = $cfg | Where-Object { $_.Name -eq $c.Cfg -and $_.RunningValue -eq 1 }
            if ($enabled) {
                Add-Result -Section 'Instance' -Instance $instance -Category 'Security' -Name $c.Name -Status 'Critical' -Message 'Enabled'
            } else {
                Add-Result -Section 'Instance' -Instance $instance -Category 'Security' -Name $c.Name -Status 'OK'
            }
        }
    } catch { }

    try {
        Add-EntityCheck -Instance $instance -Category 'Security' -Name 'No startup procedures allowed' `
            -Offenders (Invoke-DbaQuery -SqlInstance $server -Query $startupProcQuery) -Property 'name'
    } catch { }

    # --- [sa] login disabled (generic: the real sa is login ID 1) ---
    try {
        $saLogin = Get-DbaLogin -SqlInstance $server | Where-Object { $_.ID -eq 1 }
        $saName  = if ($saLogin) { $saLogin.Name } else { 'sa' }
        if ($saLogin -and -not $saLogin.IsDisabled) {
            Add-Result -Section 'Instance' -Instance $instance -Category 'Security' -Name "[$saName] login should be disabled" -Status 'Critical' -Message 'Enabled'
        } else {
            Add-Result -Section 'Instance' -Instance $instance -Category 'Security' -Name "[$saName] login should be disabled" -Status 'OK'
        }
    } catch { }

    # --- Agent jobs ---
    try {
        $sa = (Get-DbaLogin -SqlInstance $server | Where-Object { $_.ID -eq 1 }).Name
        $jobs = Get-DbaAgentJob -SqlInstance $server
        Add-EntityCheck -Instance $instance -Category 'Agent' -Name 'Every job should be owned by [sa]' `
            -Offenders ($jobs | Where-Object { $_.OwnerLoginName -ne $sa })
        Add-EntityCheck -Instance $instance -Category 'Agent' -Name 'Every job should have a schedule' `
            -Offenders ($jobs | Where-Object { $_.HasSchedule -eq $false })
    } catch { }

    try {
        $failed = New-Object System.Collections.Generic.List[object]
        foreach ($fj in (Find-DbaAgentJob -SqlInstance $server -IsFailed | Where-Object { $_.HasSchedule -and $_.Enabled })) {
            foreach ($sched in $fj.JobSchedules) {
                if ($sched.IsEnabled) { $failed.Add([pscustomobject]@{ Name = $fj.Name }); break }
            }
        }
        Add-EntityCheck -Instance $instance -Category 'Agent' -Name 'There should not be any failed Agent jobs' -Offenders $failed
    } catch { }

    # --- Long-running jobs (vs prescribed multiple of average duration) ---
    try {
        $longRunning = New-Object System.Collections.Generic.List[object]
        $runningJobs = Invoke-DbaQuery -SqlInstance $server -Query $runningJobsQuery
        foreach ($job in $runningJobs) {
            $hist = Get-DbaAgentJobHistory -SqlInstance $server -Job $job.job_name -OutcomeType Succeeded |
                    Where-Object { $_.SqlInstance -and $_.StepName -eq '(Job Outcome)' }
            if (-not $hist) { continue }
            $avgMins = [math]::Round((($hist | Measure-Object -Property { $_.Duration.TotalMinutes } -Average).Average), 2)
            $breach = $false
            if     ($avgMins -le 0.5) { if ((Get-Date).AddMinutes(-5)  -gt $job.start_execution_date) { $breach = $true } }
            elseif ($avgMins -le 5)   { if ((Get-Date).AddMinutes(-15) -gt $job.start_execution_date) { $breach = $true } }
            else                      { if ((Get-Date).AddMinutes($avgMins * -1.5) -gt $job.start_execution_date) { $breach = $true } }
            if ($breach) { $longRunning.Add([pscustomobject]@{ Name = $job.job_name }) }
        }
        Add-EntityCheck -Instance $instance -Category 'Agent' -Name 'No job should run longer than its prescribed multiple of average duration' -Offenders $longRunning
    } catch { }

    # --- Database state / configuration ---
    try {
        $databases = Invoke-DbaQuery -SqlInstance $server -Query "SELECT * FROM [sys].[databases] WHERE [Name] NOT LIKE '### TEST RESTORE ###%'"

        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'No database other than [msdb] should be Trustworthy' `
            -Offenders ($databases | Where-Object { $_.Name -ne 'msdb' -and $_.is_trustworthy_on -eq 1 })

        Add-EntityCheck -Instance $instance -Category 'Database' -Name "Every database should use compatibility level $targetCompat" `
            -Offenders ($databases | Where-Object { $_.compatibility_level -ne $targetCompat })

        $stateChecks = @(
            @{ State = 'SUSPECT';          Name = 'No database should be in the "Suspect" state' },
            @{ State = 'EMERGENCY';        Name = 'No database should be in the "Emergency Mode" state' },
            @{ State = 'RECOVERY_PENDING'; Name = 'No database should be in the "Recovery Pending" state' },
            @{ State = 'RECOVERING';       Name = 'No database should be in the "Recovering" state' },
            @{ State = 'RESTORING';        Name = 'No database should be in the "Restoring" state' }
        )
        foreach ($s in $stateChecks) {
            Add-EntityCheck -Instance $instance -Category 'Database' -Name $s.Name `
                -Offenders ($databases | Where-Object { $_.state_desc -eq $s.State })
        }

        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'No database should be in the "Standby" state' `
            -Offenders ($databases | Where-Object { $_.is_in_standby -eq 1 })

        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'All databases should be owned by [sa]' `
            -Offenders ($databases | Where-Object { $_.owner_sid -ne 1 })

        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'All databases should use Checksum page verification' `
            -Offenders ($databases | Where-Object { $_.page_verify_option_desc -ne 'CHECKSUM' })

        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'No database should use AutoShrink' `
            -Offenders ($databases | Where-Object { $_.is_auto_shrink_on -eq 1 })

        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'No database should be Offline' `
            -Offenders ($databases | Where-Object { $_.state_desc -eq 'OFFLINE' })

        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'No database file should use Percent growth' `
            -Offenders (Invoke-DbaQuery -SqlInstance $server -Query $percentGrowthQuery) -Property 'Name'

        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'No database should be READ-ONLY' `
            -Offenders (Get-DbaDatabase -SqlInstance $server -Access ReadOnly | Where-Object { $_.Name -notlike '### TEST RESTORE ###*' })
    } catch {
        Add-Result -Section 'Instance' -Instance $instance -Category 'Database' -Name 'Database checks' -Status 'Critical' -Message $_.Exception.Message
    }

    # --- All databases Normal ---
    try {
        Add-EntityCheck -Instance $instance -Category 'Database' -Name 'All databases should be in the "Normal" state' `
            -Offenders (Get-DbaDatabase -SqlInstance $server -Status Normal | Where-Object { $_.Status -ne 'Normal' })
    } catch { }

    # --- DBCC CHECKDB freshness ---
    try {
        $normal = (Get-DbaDatabase -SqlInstance $server -Status Normal).Name
        Add-EntityCheck -Instance $instance -Category 'Integrity' -Name 'Latest DBCC CHECKDB should be successful and within 7 days' `
            -Offenders (Get-DbaLastGoodCheckDb -SqlInstance $server -ExcludeDatabase tempdb -Database $normal | Where-Object { $null -eq $_.LastGoodCheckDb -and $_.Database -notlike '### TEST RESTORE ###*' }) -Property 'Database'
    } catch { }

    # --- Backups ---
    try {
        $fullCut = (Get-Date).AddHours(-$fullHours)
        Add-EntityCheck -Instance $instance -Category 'Backup' -Name "Full backup for every database within $fullHours hours" `
            -Offenders (Get-DbaDatabase -SqlInstance $server -ExcludeDatabase tempdb -OnlyAccessible | Where-Object { $_.Name -notlike '### TEST RESTORE ###*' -and $fullCut -gt $_.LastFullBackup })
    } catch { }

    try {
        $today = (Get-Date).DayOfWeek
        $diffOffenders = Get-DbaDatabase -SqlInstance $server -OnlyAccessible -WarningAction SilentlyContinue | Where-Object {
            $_.Name -notlike '### TEST RESTORE ###*' -and (
                ($today -eq 'Monday' -and (Get-Date).AddHours(-$diffMonday) -gt $_.LastDiffBackup) -or
                ($today -notin @('Sunday', 'Monday') -and (Get-Date).AddHours(-$diffWeekday) -gt $_.LastDiffBackup)
            )
        }
        Add-EntityCheck -Instance $instance -Category 'Backup' -Name 'Diff backup within window (Sun/Mon adjusted)' -Offenders $diffOffenders
    } catch { }

    try {
        $logCut = (Get-Date).AddMinutes(-$logMins)
        Add-EntityCheck -Instance $instance -Category 'Backup' -Name "Log backup within $logMins minutes for Full/BulkLogged databases" `
            -Offenders (Get-DbaDatabase -SqlInstance $server -RecoveryModel Full, BulkLogged -OnlyAccessible -WarningAction SilentlyContinue | Where-Object { $logCut -gt $_.LastLogBackup -and $_.Name -notlike '### TEST RESTORE ###*' })
    } catch { }

    try {
        $latestVmBackup = (Invoke-DbaQuery -SqlInstance $server -Query $lastVmBackupQuery).LatestType7Backup
        if ($latestVmBackup -and $latestVmBackup -ne [DBNull]::Value) {
            Add-Result -Section 'Instance' -Instance $instance -Category 'Backup' -Name 'No Type 7 (VM) backups in past 8 days' -Status 'Critical' -Message "Type 7 backup found. Latest: $latestVmBackup"
        } else {
            Add-Result -Section 'Instance' -Instance $instance -Category 'Backup' -Name 'No Type 7 (VM) backups in past 8 days' -Status 'OK'
        }
    } catch { }

    # --- Restores in past 24h (informational) ---
    try {
        $restores = @(Invoke-DbaQuery -SqlInstance $server -Query $restoresTsql)
        if ($restores.Count -gt 0) {
            Add-Result -Section 'Instance' -Instance $instance -Category 'Restore' -Name 'Database restores during past 24 hours' -Status 'Info' -Details @($restores.DatabaseName)
        } else {
            Add-Result -Section 'Instance' -Instance $instance -Category 'Restore' -Name 'Database restores during past 24 hours' -Status 'OK'
        }
    } catch { }

    # --- SSIS long-running ---
    try {
        $ssis = (Invoke-DbaQuery -SqlInstance $server -Query $ssis_running_processes_TSQL).COUNT
        if ($ssis -gt 0) {
            Add-Result -Section 'Instance' -Instance $instance -Category 'SSIS' -Name 'No SSIS long-running processes' -Status 'Critical' -Message "$ssis long-running SSIS process(es)"
        } else {
            Add-Result -Section 'Instance' -Instance $instance -Category 'SSIS' -Name 'No SSIS long-running processes' -Status 'OK'
        }
    } catch { }
}

#endregion

#region --------------------------------------------------------------- Summary & export

$end      = Get-Date
$duration = New-TimeSpan -Start $start -End $end

$summary = [pscustomobject]@{
    ok       = @($script:Results | Where-Object Status -eq 'OK').Count
    warning  = @($script:Results | Where-Object Status -eq 'Warning').Count
    critical = @($script:Results | Where-Object Status -eq 'Critical').Count
    info     = @($script:Results | Where-Object Status -eq 'Info').Count
    total    = $script:Results.Count
}

if (-not $Quiet) {
    Write-Host "`nSummary" -ForegroundColor White
    Write-Host ("  OK: {0}  Warning: {1}  Critical: {2}  Info: {3}" -f $summary.ok, $summary.warning, $summary.critical, $summary.info) -ForegroundColor Yellow
    Write-Output "`nDuration: $($duration.Minutes) min $($duration.Seconds) sec`n"
}

if ($OutputJsonPath) {
    $payload = [pscustomobject]@{
        schemaVersion   = '1.0'
        generatedAt     = (Get-Date).ToString('o')
        instances       = $instanceList
        excluded        = $Exclude
        durationSeconds = [math]::Round($duration.TotalSeconds, 1)
        summary         = $summary
        results         = $script:Results
    }
    $payload | ConvertTo-Json -Depth 8 | Out-File -LiteralPath $OutputJsonPath -Encoding UTF8
    if (-not $Quiet) { Write-Host "JSON written to $OutputJsonPath" -ForegroundColor Cyan }
}

# --- Optional email ---
if ($Config -and $Config.email -and $Config.email.enabled -and $transcriptLog) {
    try {
        $body = Get-Content -LiteralPath $transcriptLog -Raw
        Send-MailMessage -From $Config.email.from -To $Config.email.to -Subject $Config.email.subject -Body $body -SmtpServer $Config.email.smtpServer
    } catch { }
}

if ($transcriptLog) { Stop-Transcript | Out-Null }

#endregion
