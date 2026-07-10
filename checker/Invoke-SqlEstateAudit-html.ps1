<#
.SYNOPSIS
    Read-only SQL Server estate auditor. Surveys an estate of instances via dbatools,
    grades findings by severity, optionally captures a Perfmon counter sample per
    instance, and produces CSV and self-contained HTML outputs with charts
    and a prioritised remediation call-to-action.

.DESCRIPTION
    Invoke-SqlEstateAudit is a read-only, dbatools-first estate health checker.
    It never changes server state (no SET, no ALTER, no configuration changes) -
    every check uses a Get-*/Test-*/Measure-* style dbatools or native cmdlet.

    Findings are graded High / Medium / Low / Informational and rolled into:
      - a flat CSV (one row per finding, easy to pivot/filter/diff run-to-run)
      - a self-contained HTML report (no external CSS/JS - opens anywhere, even
        on a locked-down machine with no internet) with embedded charts and a
        filterable findings table

    Instance sourcing is flexible: a direct list, a text/CSV file, a Central
    Management Server, and/or live network discovery via dbatools'
    Find-DbaInstance - any combination can be combined and de-duplicated.

    Thresholds and check enablement are parameter-driven by default (so the
    script is fully self-contained and portable on day one), but can
    optionally be centralised in a small SQL Server configuration database
    (schema: dbo.Check, dbo.BackupThreshold, dbo.CheckParameter,
    dbo.CheckExclusion) via -ConfigSqlInstance/-ConfigDatabase. When supplied,
    values from the config database override the parameter defaults; if the
    config database is unreachable the script logs a warning and silently
    falls back to parameter defaults rather than failing the run.

    Perfmon capture (-RunPerfmon) is opt-in and has three modes:
      Quick    - a single point-in-time counter read per instance
      Trend    - a short sampling window (default 90s @ 2s intervals)
      Extended - a longer sampling window (default 8 min @ 5s intervals)
    Capture uses native Get-Counter against the resolved computer name for
    each instance (reliable across every supported Windows Server version and
    leaves no artefacts behind). If -PreferDbatoolsPerfmon is also supplied,
    the script will first attempt to use dbatools' Get-DbaPfDataCollectorSet*
    family (genuine Windows Data Collector Sets) and only falls back to
    Get-Counter if that path is unavailable or errors - this is opt-in
    because dbatools' bundled Perfmon templates vary by module version and
    creating/removing a Data Collector Set is a slightly heavier footprint
    than an ad-hoc counter read.

.NOTES
    File Name : Invoke-SqlEstateAudit.ps1
    Author    : Richard ArmstrongFinnerty (github.com/rafinnerty)
    Requires  : dbatools module (PowerShell 5.1 or 7+)
    Philosophy: read-only, dependency-free, self-contained output, defensive
                per-check error handling - a single check failing logs a
                collection issue rather than ending the run.

.PARAMETER SqlInstance
    One or more SQL Server instances to audit, supplied directly.

.PARAMETER InstancesFile
    Path to a plain text or CSV file of instance names (one per line, or a
    'SqlInstance' column if CSV). Combined with -SqlInstance if both given.

.PARAMETER CmsInstance
    A Central Management Server instance. Registered servers under it are
    added to the audit list via Get-DbaRegServer.

.PARAMETER CmsGroup
    Optional CMS server group name to restrict Get-DbaRegServer to.

.PARAMETER DiscoverInstances
    Switch. Runs dbatools' Find-DbaInstance to discover SQL Server instances
    on the network and adds any found to the audit list. Slower and noisier
    than a supplied list - intended as a supplement/cross-check, not a
    replacement, for a known instance list.

.PARAMETER DiscoveryDomain
    Optional Active Directory domain to scope Find-DbaInstance's domain scan.
    If omitted, Find-DbaInstance uses the current computer's domain.

.PARAMETER DiscoveryIpRange
    Optional IP range (e.g. '10.10.1.1-10.10.1.254') to scope Find-DbaInstance's
    IP scan. If omitted, IP range scanning is not used.

.PARAMETER ExcludeInstance
    One or more instance names to exclude from the final, combined list,
    regardless of which source(s) they came from.

.PARAMETER SqlCredential
    Optional SQL Server credential (PSCredential). If omitted, Windows
    Authentication is used (the default and recommended posture for a
    read-only audit).

.PARAMETER ConfigSqlInstance
    Optional instance hosting the sqlChecker configuration database. When
    supplied together with -ConfigDatabase, check enablement/severity/
    exclusions/thresholds are loaded from there and override parameter
    defaults for any value present.

.PARAMETER ConfigDatabase
    Name of the configuration database on -ConfigSqlInstance. Default: sqlChecker.

.PARAMETER FullBackupMaxDays
    Default parameter threshold: a full backup older than this many days is
    flagged. Overridden by dbo.BackupThreshold (BackupType='Full') if a
    config database is supplied. Default: 7.

.PARAMETER LogBackupMaxMinutes
    Default parameter threshold: for FULL/BULK_LOGGED recovery databases, a
    log backup older than this many minutes is flagged. Overridden by
    dbo.BackupThreshold (BackupType='Log') if a config database is supplied.
    Default: 15.

.PARAMETER CheckDbMaxDays
    Default parameter threshold: a DBCC CHECKDB older than this many days (or
    never run) is flagged. Default: 14.

.PARAMETER DiskFreePercentWarn
    Default parameter threshold: volume free space below this %% is Medium.
    Default: 15.

.PARAMETER DiskFreePercentCrit
    Default parameter threshold: volume free space below this %% is High.
    Default: 5.

.PARAMETER MaxCuBehind
    Default parameter threshold passed to Test-DbaBuild -MaxBehind. Default: '1CU'.

.PARAMETER RecentRestartHours
    Default parameter threshold: an instance whose uptime is below this many
    hours is flagged Informational (possible unplanned restart / patch not
    yet reviewed). Default: 24.

.PARAMETER CostThresholdRecommended
    The 'sensible' cost threshold for parallelism value the script recommends
    when an instance is still at the SQL Server default of 5. Default: 50.

.PARAMETER QueryStoreStoragePercentWarn
    Default parameter threshold: a Query Store whose current storage exceeds
    this %% of MAX_STORAGE_SIZE_MB is flagged Medium (it will silently flip to
    READ_ONLY and stop collecting when it fills). Default: 90.

.PARAMETER RunPerfmon
    Switch. Enables performance counter capture per instance.

.PARAMETER PerfmonMode
    One of Quick / Trend / Extended. See .DESCRIPTION. Default: Quick.

.PARAMETER PerfmonDurationSeconds
    Overrides the default capture window (Trend=90s, Extended=480s) for the
    selected -PerfmonMode.

.PARAMETER PerfmonIntervalSeconds
    Overrides the default sample interval (Trend=2s, Extended=5s) for the
    selected -PerfmonMode.

.PARAMETER PreferDbatoolsPerfmon
    Switch. Attempt dbatools' Get-DbaPfDataCollectorSet* Data Collector Set
    workflow before falling back to native Get-Counter. See .DESCRIPTION.

.PARAMETER OutputPath
    Folder to write CSV/HTML outputs to. Default: a timestamped
    subfolder of the current directory.

.PARAMETER CompanyName
    Display name used in report headers. Default: Super Widgets LLC'.

.PARAMETER ReportTitle
    Display title used in report headers. Default: 'SQL Server Estate Audit'.

.PARAMETER OpenWhenDone
    Switch. Opens the HTML report in the default browser when the run completes.

.EXAMPLE
    .\Invoke-SqlEstateAudit.ps1 -InstancesFile .\instances.txt -OutputPath "C:\Audit\Super Widgets LLC" -OpenWhenDone

.EXAMPLE
    .\Invoke-SqlEstateAudit.ps1 -CmsInstance CMS01 -RunPerfmon -PerfmonMode Trend

.EXAMPLE
    .\Invoke-SqlEstateAudit.ps1 -SqlInstance SQL01,SQL02\PROD -DiscoverInstances -DiscoveryDomain corp.zedra.com

.EXAMPLE
    .\Invoke-SqlEstateAudit.ps1 -InstancesFile .\instances.txt -ConfigSqlInstance SQL01 -ConfigDatabase sqlChecker -RunPerfmon -PerfmonMode Extended
#>

[CmdletBinding()]
param(
    [Parameter()]
    [string[]]$SqlInstance,

    [Parameter()]
    [string]$InstancesFile,

    [Parameter()]
    [string]$CmsInstance,

    [Parameter()]
    [string]$CmsGroup,

    [Parameter()]
    [switch]$DiscoverInstances,

    [Parameter()]
    [string]$DiscoveryDomain,

    [Parameter()]
    [string]$DiscoveryIpRange,

    [Parameter()]
    [string[]]$ExcludeInstance = @(),

    [Parameter()]
    [System.Management.Automation.PSCredential]$SqlCredential,

    [Parameter()]
    [string]$ConfigSqlInstance,

    [Parameter()]
    [string]$ConfigDatabase = 'sqlChecker',

    [Parameter()] [int]$FullBackupMaxDays = 7,
    [Parameter()] [int]$LogBackupMaxMinutes = 15,
    [Parameter()] [int]$DiffBackupMaxHours = 26,
    [Parameter()] [int]$CheckDbMaxDays = 14,
    [Parameter()] [int]$DiskFreePercentWarn = 15,
    [Parameter()] [int]$DiskFreePercentCrit = 5,
    [Parameter()] [string]$MaxCuBehind = '1CU',
    [Parameter()] [int]$RecentRestartHours = 24,
    [Parameter()] [int]$CostThresholdRecommended = 50,
    [Parameter()] [int]$QueryStoreStoragePercentWarn = 90,
    [Parameter()] [int]$SsisLongRunningMinutes = 120,

    [Parameter()]
    [switch]$RunPerfmon,

    [Parameter()]
    [ValidateSet('Quick', 'Trend', 'Extended')]
    [string]$PerfmonMode = 'Quick',

    [Parameter()]
    [int]$PerfmonDurationSeconds,

    [Parameter()]
    [int]$PerfmonIntervalSeconds,

    [Parameter()]
    [switch]$PreferDbatoolsPerfmon,

    [Parameter()]
    [string]$OutputPath,

    [Parameter()]
    [string]$CompanyName = 'Super Widgets LLC',

    [Parameter()]
    [string]$ReportTitle = 'SQL Server Estate Audit',

    [Parameter()]
    [switch]$OpenWhenDone
)

#region ------------------------------- Bootstrap -------------------------------

$ErrorActionPreference = 'Stop'
$scriptStart = Get-Date

if (-not $OutputPath) {
    $OutputPath = Join-Path -Path (Get-Location) -ChildPath ("SqlEstateAudit_" + (Get-Date -Format 'yyyyMMdd_HHmmss'))
}
if (-not (Test-Path -Path $OutputPath)) {
    New-Item -Path $OutputPath -ItemType Directory -Force | Out-Null
}
$transcriptPath = Join-Path $OutputPath 'AuditTranscript.log'
Start-Transcript -Path $transcriptPath -Force | Out-Null

function Write-AuditLog {
    param(
        [Parameter(Mandatory)] [string]$Message,
        [ValidateSet('Info', 'Warn', 'Error', 'Section')] [string]$Level = 'Info'
    )
    $ts = (Get-Date -Format 'HH:mm:ss')
    switch ($Level) {
        'Section' { Write-Host "`n=== $Message ===" -ForegroundColor Cyan }
        'Warn'    { Write-Host "[$ts] WARN  $Message" -ForegroundColor Yellow }
        'Error'   { Write-Host "[$ts] ERROR $Message" -ForegroundColor Red }
        default   { Write-Host "[$ts] $Message" -ForegroundColor Gray }
    }
}

Write-AuditLog -Level Section -Message "SQL Server Estate Audit starting"
Write-AuditLog "Output folder: $OutputPath"

if (-not (Get-Module -ListAvailable -Name dbatools)) {
    Write-AuditLog -Level Error -Message "dbatools module not found. Install with: Install-Module dbatools -Scope CurrentUser"
    Stop-Transcript | Out-Null
    throw "dbatools module is required and was not found."
}
Import-Module dbatools -ErrorAction Stop

#endregion

#region ------------------------------- Instance resolution -------------------------------

function Resolve-AuditInstanceList {
    [CmdletBinding()]
    param(
        [string[]]$SqlInstance,
        [string]$InstancesFile,
        [string]$CmsInstance,
        [string]$CmsGroup,
        [switch]$DiscoverInstances,
        [string]$DiscoveryDomain,
        [string]$DiscoveryIpRange,
        [string[]]$ExcludeInstance
    )

    $collected = New-Object System.Collections.Generic.List[string]
    $sourceLog = @{}

    if ($SqlInstance) {
        foreach ($i in $SqlInstance) { $collected.Add($i); $sourceLog[$i] = 'Direct parameter' }
    }

    if ($InstancesFile) {
        if (-not (Test-Path $InstancesFile)) {
            Write-AuditLog -Level Warn -Message "InstancesFile '$InstancesFile' not found - skipping this source."
        }
        else {
            $raw = if ($InstancesFile -like '*.csv') {
                (Import-Csv -Path $InstancesFile) | ForEach-Object {
                    if ($_.PSObject.Properties.Name -contains 'SqlInstance') { $_.SqlInstance } else { $_.PSObject.Properties.Value | Select-Object -First 1 }
                }
            }
            else {
                Get-Content -Path $InstancesFile | Where-Object { $_ -and $_.Trim() -ne '' -and -not $_.Trim().StartsWith('#') }
            }
            foreach ($i in $raw) {
                $iTrim = $i.Trim()
                if ($iTrim) { $collected.Add($iTrim); if (-not $sourceLog.ContainsKey($iTrim)) { $sourceLog[$iTrim] = 'InstancesFile' } }
            }
        }
    }

    if ($CmsInstance) {
        try {
            Write-AuditLog "Querying CMS '$CmsInstance' for registered servers..."
            $regParams = @{ SqlInstance = $CmsInstance }
            if ($CmsGroup) { $regParams['Group'] = $CmsGroup }
            $regServers = Get-DbaRegServer @regParams -ErrorAction Stop
            foreach ($rs in $regServers) {
                $name = $rs.ServerName
                $collected.Add($name)
                if (-not $sourceLog.ContainsKey($name)) { $sourceLog[$name] = "CMS:$CmsInstance" }
            }
            Write-AuditLog "CMS returned $($regServers.Count) registered server(s)."
        }
        catch {
            Write-AuditLog -Level Warn -Message "Could not query CMS '$CmsInstance': $($_.Exception.Message)"
        }
    }

    if ($DiscoverInstances) {
        try {
            Write-AuditLog "Running Find-DbaInstance network discovery - this can take several minutes..."

            # Only request the 'Domain' (AD/SPN) discovery type when the machine is
            # actually domain-joined. On a workgroup box, Domain/SPN enumeration fails
            # noisily (leaking runspace-level "System error." lines into the transcript)
            # even though it recovers - so we scope it out and rely on SQL Browser
            # (DataSourceEnumeration) plus Local discovery, which need no domain.
            $isDomainJoined = $false
            try { $isDomainJoined = [bool](Get-CimInstance -ClassName Win32_ComputerSystem -ErrorAction Stop).PartOfDomain } catch { $isDomainJoined = $false }

            if ($DiscoveryIpRange) {
                # IPRange scans the supplied addresses directly - no domain needed.
                $discovered = Find-DbaInstance -DiscoveryType IPRange -IpAddress $DiscoveryIpRange -ErrorAction Stop
            }
            elseif ($DiscoveryDomain) {
                # An explicit domain was supplied, so Domain discovery is intended regardless.
                $discovered = Find-DbaInstance -DiscoveryType Domain, DataSourceEnumeration -DomainController $DiscoveryDomain -ErrorAction Stop
            }
            else {
                if ($isDomainJoined) {
                    $discovered = Find-DbaInstance -DiscoveryType Domain, DataSourceEnumeration -ErrorAction Stop
                }
                else {
                    Write-AuditLog "Machine is not domain-joined - scoping discovery to SQL Browser enumeration (skipping AD/SPN scan)."
                    $discovered = Find-DbaInstance -DiscoveryType DataSourceEnumeration -ErrorAction Stop
                }
            }
            foreach ($d in $discovered) {
                $name = $d.SqlInstance
                $collected.Add($name)
                if (-not $sourceLog.ContainsKey($name)) { $sourceLog[$name] = 'Find-DbaInstance discovery' }
            }
            Write-AuditLog "Discovery found $($discovered.Count) instance(s)."
        }
        catch {
            Write-AuditLog -Level Warn -Message "Find-DbaInstance discovery failed or is unsupported in this environment: $($_.Exception.Message)"
        }
    }

    $final = $collected |
        Select-Object -Unique |
        Where-Object { $ExcludeInstance -notcontains $_ }

    if (-not $final -or $final.Count -eq 0) {
        throw "No SQL Server instances were resolved. Supply -SqlInstance, -InstancesFile, -CmsInstance, and/or -DiscoverInstances."
    }

    Write-AuditLog "Resolved $($final.Count) unique instance(s) to audit:"
    foreach ($f in ($final | Sort-Object)) { Write-AuditLog "  - $f  [$($sourceLog[$f])]" }

    return $final
}

#endregion

#region ------------------------------- Configuration / thresholds -------------------------------

# Recommended remediation text, keyed by CheckCode. Used in the HTML
# "call to action" section so every finding maps to a concrete next step.
$Script:Recommendations = @{
    'BUILD_UNSUPPORTED'     = 'Patch to a supported build immediately - this is an unsupported, unpatched surface with no vendor fixes available.'
    'BUILD_CU_BEHIND'       = 'Schedule patching to bring the instance within the target CU tolerance at the next maintenance window.'
    'BACKUP_FULL_STALE'     = 'Investigate why the backup job is not completing and restore full backup cadence for this database.'
    'BACKUP_LOG_STALE'      = 'Check the log backup job/agent schedule - a stale log chain risks both RPO and unbounded log growth.'
    'CHECKDB_STALE'         = 'Run DBCC CHECKDB (or schedule via Ola Hallengren/Maintenance Solution) and restore a regular integrity-check cadence.'
    'CHECKDB_MISSING'       = 'No successful CHECKDB has ever been recorded for this database - run one at the next opportunity and schedule going forward.'
    'SUSPECT_PAGES'         = 'Investigate suspect pages immediately - this indicates on-disk corruption. Validate backups and plan a restore/repair strategy.'
    'DB_STATE_BAD'          = 'Investigate this database state as a priority - SUSPECT/RECOVERY_PENDING/OFFLINE databases are effectively down for users.'
    'DB_STANDBY'            = 'Confirm this STANDBY database is an intentional log-shipping secondary, not an accidental state.'
    'DB_RESTORING'          = 'Confirm this RESTORING database is mid-plan (e.g. log shipping) rather than a stalled/failed restore.'
    'DB_READONLY'           = 'Confirm READ_ONLY is intentional for this database; if not, investigate how/when it was set.'
    'DB_TRUSTWORTHY'        = 'Disable TRUSTWORTHY unless there is a documented, specific reason (e.g. certain CLR/cross-db ownership chaining scenarios).'
    'DB_COMPAT_LOW'         = 'Plan a compatibility level upgrade once application regression testing has been completed.'
    'DB_AUTOSHRINK'         = 'Disable AUTO_SHRINK - it causes fragmentation and CPU/IO churn with no lasting space benefit.'
    'DB_AUTOCLOSE'          = 'Disable AUTO_CLOSE - it causes unnecessary open/close overhead and cache flushing on every connection.'
    'DB_PAGEVERIFY'         = 'Set PAGE_VERIFY to CHECKSUM - earlier options (TORN_PAGE_DETECTION/NONE) give materially weaker corruption detection.'
    'DB_OWNER_NOT_SA'       = 'Change database owner to sa (or an equivalent service account) to avoid ownership-chain issues if the named owner is disabled/removed.'
    'DB_PERCENT_GROWTH'     = 'Switch data/log file autogrowth from percentage to a fixed MB amount to avoid unpredictable, increasingly large growth events.'
    'DB_ORPHANED_USER'      = 'Reconcile orphaned database users against server logins (sp_change_users_login / ALTER USER ... WITH LOGIN) or remove if stale.'
    'SEC_XP_CMDSHELL'       = 'Disable xp_cmdshell unless there is a specific, documented business requirement - it is a significant lateral-movement risk.'
    'SEC_OLE_AUTOMATION'    = 'Disable Ole Automation Procedures unless required - deprecated and rarely needed on modern SQL Server.'
    'SEC_ADHOC_QUERIES'     = 'Disable Ad Hoc Distributed Queries unless required, to reduce the linked-server/attack surface.'
    'SEC_CROSS_DB_CHAIN'    = 'Disable Cross DB Ownership Chaining at the instance level; use it per-database only where explicitly justified.'
    'SEC_REMOTE_DAC'        = 'Review whether Remote DAC is required; if not, disable it to reduce the emergency-access attack surface.'
    'SEC_SA_ENABLED'        = 'Disable the built-in sa login (or at minimum rename it and enforce a strong, rotated, vaulted password).'
    'SEC_BUILTIN_LOGIN'     = 'Remove BUILTIN\\Administrators / broad Windows group logins and replace with named, least-privilege logins/roles.'
    'SEC_LINKED_SRV_NOAUTH' = 'Review this linked server''s security context - "be made using the login''s current security context" or self-mapping without an explicit remote login is often unintended.'
    'AGENT_JOB_FAILED'      = 'Investigate the failed job history and remediate the underlying cause; re-run manually if the missed run is time-sensitive.'
    'AGENT_JOB_NO_SCHEDULE' = 'Confirm this job is intentionally schedule-less (e.g. alert-driven) or attach a schedule.'
    'AGENT_JOB_LONG_RUNNING'= 'Investigate whether this job is still running as expected or is hung/blocked beyond its normal duration envelope.'
    'AGENT_JOB_NO_OWNER'    = 'Change job ownership to sa (or a dedicated service account) so the job keeps running if the current owner login is disabled.'
    'AGENT_JOB_NO_NOTIFY'   = 'Configure failure notification (operator email) so job failures are not silently missed.'
    'PERF_MAXDOP'           = 'Set MAXDOP per current Microsoft guidance for this instance''s NUMA/core topology (Test-DbaMaxDop recommendation shown).'
    'PERF_COST_THRESHOLD'   = 'Raise Cost Threshold for Parallelism from the SQL Server default of 5 - most OLTP workloads benefit from 40-75.'
    'PERF_MAX_MEMORY'       = 'Set Max Server Memory explicitly - leaving it unbounded risks OS memory pressure and can starve the OS/other instances.'
    'DISK_SPACE_LOW'        = 'Free up space or extend the volume - approaching capacity risks autogrowth failures and transaction failures.'
    'DISK_SPACE_CRIT'       = 'Free up space or extend this volume urgently - below the critical threshold, autogrowth and transactions can start failing with little warning.'
    'QDS_NOT_ENABLED'       = 'Enable Query Store (OPERATION_MODE = READ_WRITE) - without it there is no plan/regression history when a performance incident hits.'
    'QDS_STATE_MISMATCH'    = 'Query Store is not running in its configured state - typically it has filled MAX_STORAGE_SIZE_MB and flipped to READ_ONLY, or hit an error. Increase max size and/or purge old data (sp_query_store_flush_db / ALTER DATABASE ... SET QUERY_STORE CLEAR), then set READ_WRITE.'
    'QDS_READ_ONLY'         = 'Query Store is deliberately READ_ONLY - it retains history but collects nothing new. Confirm this is intentional.'
    'QDS_STORAGE_NEAR_FULL' = 'Query Store storage is close to MAX_STORAGE_SIZE_MB - when it fills it silently flips to READ_ONLY and stops collecting. Increase the max size or tighten retention (STALE_QUERY_THRESHOLD_DAYS / size-based cleanup).'
    'QDS_CAPTURE_ALL'       = 'QUERY_CAPTURE_MODE = ALL captures every ad-hoc statement and can bloat Query Store on busy systems - AUTO is the recommended setting on SQL 2019+.'
    'HADR_LOGSHIP_LAG'      = 'Investigate log shipping restore lag/failures - this directly threatens the DR RPO for this database.'
    'HADR_AG_UNHEALTHY'     = 'Investigate Availability Group synchronization health - a replica not synchronized threatens both DR and readable-secondary workload.'
    'UPTIME_RECENT_RESTART' = 'Confirm this restart was planned (patching/failover); if unplanned, review the SQL Server/Windows event logs for the cause.'
    'SSIS_LONG_RUNNING'     = 'Investigate this long-running SSIS process for a stall/blocking condition.'
    'TDE_NOT_ENABLED'       = 'Consider Transparent Data Encryption for databases holding sensitive/regulated data, per data-protection policy.'
    'TEMPDB_FILE_COUNT'     = 'Align tempdb data file count with (v)CPU count (up to 8) and ensure files are equally sized, per Microsoft guidance.'
    'PERFMON_HIGH_CPU'      = 'Investigate top CPU consumers (Query Store / Get-DbaTopCpuTime) during the sampled window.'
    'PERFMON_LOW_PLE'       = 'Page Life Expectancy is low relative to buffer pool size - investigate memory pressure and top-memory-consuming queries.'
    'PERFMON_HIGH_DISK_LAT' = 'Disk latency is elevated - investigate storage subsystem performance and I/O-heavy queries during the sampled window.'
    'PERFMON_MEM_GRANTS_PEND'= 'Pending memory grants observed - investigate concurrent memory-intensive queries and consider Resource Governor / query tuning.'
    'CPU_CORES_UNUSABLE'    = 'CPU capacity is present but unusable by SQL Server - typically an edition core/socket licensing cap. Either license appropriately, reconfigure the VM (fewer sockets, more cores per socket), or accept and document the cap.'
    'SEC_STARTUP_PROC'      = 'Verify this startup procedure is known and intentional - auto-execute procedures are a persistence mechanism and should be documented or removed.'
    'BACKUP_VDI_DETECTED'   = 'Confirm the VDI/snapshot backup tool is intended and coordinated with native backups - snapshot fulls can reset differential bases (use COPY_ONLY where supported).'
    'BACKUP_DIFF_STALE'     = 'Diffs are part of this database''s strategy but have gone stale - check the differential backup job and schedule.'
    'DB_RESTORED_RECENTLY'  = 'Confirm this restore was expected (planned refresh/DR test); unexpected restores warrant investigation.'
}

function Get-AuditConfig {
    [CmdletBinding()]
    param(
        [hashtable]$ParameterDefaults,
        [string]$ConfigSqlInstance,
        [string]$ConfigDatabase
    )

    # Start from parameter defaults - the script is fully self-contained without a config DB.
    $config = @{
        Thresholds      = $ParameterDefaults.Clone()
        DisabledChecks  = New-Object System.Collections.Generic.HashSet[string]
        SeverityOverride= @{}
        Exclusions      = New-Object System.Collections.Generic.List[object]   # objects: CheckCode, SqlInstance, ObjectName
        Source          = 'ParametersOnly'
    }

    if (-not $ConfigSqlInstance) {
        Write-AuditLog "No -ConfigSqlInstance supplied - running fully parameter-driven (self-contained mode)."
        return $config
    }

    try {
        Write-AuditLog "Loading config overrides from $ConfigSqlInstance.$ConfigDatabase ..."
        $checkRows = Invoke-DbaQuery -SqlInstance $ConfigSqlInstance -Database $ConfigDatabase -Query "SELECT CheckCode, IsEnabled, Severity FROM dbo.[Check]" -ErrorAction Stop
        foreach ($row in $checkRows) {
            if ($row.IsEnabled -eq $false) { [void]$config.DisabledChecks.Add($row.CheckCode) }
            if ($row.Severity) { $config.SeverityOverride[$row.CheckCode] = $row.Severity }
        }

        $backupRows = Invoke-DbaQuery -SqlInstance $ConfigSqlInstance -Database $ConfigDatabase -Query "SELECT BackupType, TimeBox, Limit FROM dbo.BackupThreshold" -ErrorAction Stop
        foreach ($row in $backupRows) {
            if ($row.BackupType -eq 'Full' -and $row.TimeBox -eq 'Day')    { $config.Thresholds['FullBackupMaxDays']  = [int]$row.Limit }
            if ($row.BackupType -eq 'Log'  -and $row.TimeBox -eq 'Minute') { $config.Thresholds['LogBackupMaxMinutes'] = [int]$row.Limit }
        }

        $paramRows = Invoke-DbaQuery -SqlInstance $ConfigSqlInstance -Database $ConfigDatabase -Query "SELECT ParamName, ParamValue FROM dbo.CheckParameter" -ErrorAction Stop
        foreach ($row in $paramRows) {
            if ($config.Thresholds.ContainsKey($row.ParamName)) {
                $config.Thresholds[$row.ParamName] = $row.ParamValue
            }
        }

        $exclusionRows = Invoke-DbaQuery -SqlInstance $ConfigSqlInstance -Database $ConfigDatabase -Query "SELECT CheckCode, SqlInstance, ObjectName FROM dbo.CheckExclusion" -ErrorAction Stop
        foreach ($row in $exclusionRows) { $config.Exclusions.Add($row) }

        $config.Source = "ConfigDb:$ConfigSqlInstance.$ConfigDatabase"
        Write-AuditLog "Config DB loaded: $($config.DisabledChecks.Count) disabled check(s), $($config.Exclusions.Count) exclusion row(s)."
    }
    catch {
        Write-AuditLog -Level Warn -Message "Could not load config DB ($ConfigSqlInstance.$ConfigDatabase): $($_.Exception.Message) - falling back to parameter defaults only."
    }

    return $config
}

function Test-AuditExcluded {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] $Config,
        [Parameter(Mandatory)] [string]$CheckCode,
        [Parameter(Mandatory)] [string]$SqlInstance,
        [string]$ObjectName = ''
    )
    if ($Config.DisabledChecks.Contains($CheckCode)) { return $true }
    foreach ($ex in $Config.Exclusions) {
        $codeMatch = (-not $ex.CheckCode) -or ($ex.CheckCode -eq $CheckCode)
        $instMatch = (-not $ex.SqlInstance) -or ($SqlInstance -like $ex.SqlInstance)
        $objMatch  = (-not $ex.ObjectName) -or ($ObjectName -like $ex.ObjectName)
        if ($codeMatch -and $instMatch -and $objMatch) { return $true }
    }
    return $false
}

#endregion

#region ------------------------------- Finding collector -------------------------------

$Script:Findings = New-Object System.Collections.Generic.List[object]
$Script:CollectionIssues = New-Object System.Collections.Generic.List[object]

function Add-Finding {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] [string]$CheckCode,
        [Parameter(Mandatory)] [string]$Category,
        [Parameter(Mandatory)] [ValidateSet('High', 'Medium', 'Low', 'Informational', 'OK')] [string]$Severity,
        [Parameter(Mandatory)] [string]$CheckName,
        [string]$ObjectName = '',
        [Parameter(Mandatory)] [string]$Detail,
        [hashtable]$Config
    )

    if ($Config -and (Test-AuditExcluded -Config $Config -CheckCode $CheckCode -SqlInstance $SqlInstance -ObjectName $ObjectName)) {
        return
    }

    if ($Config -and $Config.SeverityOverride.ContainsKey($CheckCode)) {
        $Severity = $Config.SeverityOverride[$CheckCode]
    }

    $recommendation = if ($Script:Recommendations.ContainsKey($CheckCode)) { $Script:Recommendations[$CheckCode] } else { 'Review finding and remediate per standard practice.' }

    $Script:Findings.Add([PSCustomObject]@{
        SqlInstance    = $SqlInstance
        Category       = $Category
        CheckCode      = $CheckCode
        CheckName      = $CheckName
        Severity       = $Severity
        ObjectName     = $ObjectName
        Detail         = $Detail
        Recommendation = $recommendation
        CapturedAt     = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
    })
}

function Get-CompatLevelNumber {
    # dbatools/SMO may expose compatibility level as .CompatibilityLevel or .Compatibility depending on
    # version, rendered as a plain number or an enum-like string (e.g. 'Version150') - extract digits
    # defensively rather than assuming a direct [int] cast will always work.
    param($Value)
    if ($null -eq $Value) { return $null }
    $s = [string]$Value
    if ($s -match '(\d+)') { return [int]$Matches[1] }
    return $null
}

function ConvertTo-AuditDateTime {
    # dbatools date properties (Get-DbaLastBackup et al.) arrive as a mix of
    # [DbaDateTime], [datetime], [DBNull], $null, or the SQL '1900-01-01' "never"
    # sentinel depending on cmdlet and database state. Mixing those types in
    # date arithmetic throws 'Object must be of type DateTime'. Normalise
    # everything to either a real [datetime] or $null meaning 'never'.
    param($Value)
    if ($null -eq $Value -or $Value -is [System.DBNull]) { return $null }
    $dt = $null
    if ($Value -is [datetime]) { $dt = $Value }
    else {
        try { $dt = [datetime]$Value }
        catch {
            $parsed = [datetime]::MinValue
            if ([datetime]::TryParse([string]$Value, [ref]$parsed)) { $dt = $parsed } else { return $null }
        }
    }
    if ($dt -le [datetime]'1902-01-01') { return $null }  # 1900-01-01 / MinValue sentinels = never
    return $dt
}

function Add-CollectionIssue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] [string]$CheckCode,
        [Parameter(Mandatory)] [string]$ErrorMessage
    )
    $Script:CollectionIssues.Add([PSCustomObject]@{
        SqlInstance  = $SqlInstance
        CheckCode    = $CheckCode
        ErrorMessage = $ErrorMessage
        CapturedAt   = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
    })
    Write-AuditLog -Level Warn -Message "[$SqlInstance] $CheckCode collection issue: $ErrorMessage"
}

# Wraps a check scriptblock so one failing check logs a collection issue
# rather than aborting the whole instance survey.
function Invoke-AuditCheck {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] [string]$CheckCode,
        [Parameter(Mandatory)] [scriptblock]$ScriptBlock
    )
    try {
        & $ScriptBlock
    }
    catch {
        Add-CollectionIssue -SqlInstance $SqlInstance -CheckCode $CheckCode -ErrorMessage $_.Exception.Message
    }
}

#endregion

#region ------------------------------- Instance-level checks -------------------------------

function Invoke-InstanceLevelChecks {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] $ServerObject,
        [Parameter(Mandatory)] [hashtable]$Thresholds,
        [Parameter(Mandatory)] [hashtable]$Config,
        [System.Management.Automation.PSCredential]$SqlCredential
    )

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'BUILD' -ScriptBlock {
        $build = Test-DbaBuild -SqlInstance $ServerObject -MaxBehind $Thresholds['MaxCuBehind'] -ErrorAction Stop
        if ($build) {
            if (-not $build.SupportedUntil -or ($build.SupportedUntil -lt (Get-Date))) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BUILD_UNSUPPORTED' -Category 'Patching' -Severity 'High' `
                    -CheckName 'SQL Server build support status' -Detail "Build $($build.Build) is out of support (support ended $($build.SupportedUntil))."
            }
            elseif ($build.Compliant -eq $false) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BUILD_CU_BEHIND' -Category 'Patching' -Severity 'Medium' `
                    -CheckName 'SQL Server CU currency' -Detail "Build $($build.Build) is more than $($Thresholds['MaxCuBehind']) behind the latest Cumulative Update."
            }
            else {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BUILD' -Category 'Patching' -Severity 'OK' `
                    -CheckName 'SQL Server build support status' -Detail "Build $($build.Build) is supported and within CU currency tolerance."
            }
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'UPTIME' -ScriptBlock {
        $uptime = Get-DbaUptime -SqlInstance $ServerObject -ErrorAction Stop
        # Get-DbaUptime exposes SqlStartTime, not SqlServerStartTime - the latter
        # resolved to $null and rendered as "(since )" in the report.
        $startTime = if ($uptime) { ConvertTo-AuditDateTime $uptime.SqlStartTime } else { $null }
        $startText = if ($startTime) { $startTime.ToString('yyyy-MM-dd HH:mm') } else { 'start time unavailable' }
        if ($uptime -and $uptime.SqlUptime.TotalHours -lt $Thresholds['RecentRestartHours']) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'UPTIME_RECENT_RESTART' -Category 'Availability' -Severity 'Informational' `
                -CheckName 'Recent restart' -Detail ("Instance has only been up {0:N1} hours (since {1})." -f $uptime.SqlUptime.TotalHours, $startText)
        }
        elseif ($uptime) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'UPTIME' -Category 'Availability' -Severity 'OK' `
                -CheckName 'Uptime' -Detail ("Up {0:N1} hours (since {1})." -f $uptime.SqlUptime.TotalHours, $startText)
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'MAXMEM' -ScriptBlock {
        $mem = Get-DbaMaxMemory -SqlInstance $ServerObject -ErrorAction Stop
        if ($mem -and ($mem.MaxValue -ge 2147483647 -or $mem.MaxValue -eq 0)) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERF_MAX_MEMORY' -Category 'Configuration' -Severity 'Medium' `
                -CheckName 'Max Server Memory unbounded' -Detail "Max Server Memory is left at the default/unbounded value (Total physical memory: $($mem.Total) MB)."
        }
        elseif ($mem) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'MAXMEM' -Category 'Configuration' -Severity 'OK' `
                -CheckName 'Max Server Memory configured' -Detail "Max Server Memory set to $($mem.MaxValue) MB (physical: $($mem.Total) MB)."
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'MAXDOP' -ScriptBlock {
        $dop = Test-DbaMaxDop -SqlInstance $ServerObject -ErrorAction Stop | Select-Object -First 1
        if ($dop -and $dop.CurrentInstanceMaxDop -ne $dop.RecommendedMaxDop) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERF_MAXDOP' -Category 'Configuration' -Severity 'Low' `
                -CheckName 'MAXDOP configuration' -Detail "Current MAXDOP is $($dop.CurrentInstanceMaxDop); dbatools recommends $($dop.RecommendedMaxDop) for this topology."
        }
        elseif ($dop) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'MAXDOP' -Category 'Configuration' -Severity 'OK' `
                -CheckName 'MAXDOP configuration' -Detail "MAXDOP $($dop.CurrentInstanceMaxDop) matches the dbatools recommendation."
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'COSTTHRESHOLD' -ScriptBlock {
        $cfg = Get-DbaSpConfigure -SqlInstance $ServerObject -Name 'cost threshold for parallelism' -ErrorAction Stop
        if ($cfg -and [int]$cfg.RunningValue -eq 5) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERF_COST_THRESHOLD' -Category 'Configuration' -Severity 'Low' `
                -CheckName 'Cost Threshold for Parallelism at default' -Detail "Still at the SQL Server default of 5; consider raising toward $($Thresholds['CostThresholdRecommended']) after workload review."
        }
        elseif ($cfg) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'COSTTHRESHOLD' -Category 'Configuration' -Severity 'OK' `
                -CheckName 'Cost Threshold for Parallelism' -Detail "Set to $([int]$cfg.RunningValue), above the SQL Server default."
        }
    }

    # Surface-area / sp_configure hardening flags
    $surfaceAreaChecks = @(
        @{ Name = 'xp_cmdshell'; Code = 'SEC_XP_CMDSHELL'; Severity = 'High'; Label = 'xp_cmdshell enabled' }
        @{ Name = 'Ole Automation Procedures'; Code = 'SEC_OLE_AUTOMATION'; Severity = 'Medium'; Label = 'Ole Automation Procedures enabled' }
        @{ Name = 'Ad Hoc Distributed Queries'; Code = 'SEC_ADHOC_QUERIES'; Severity = 'Medium'; Label = 'Ad Hoc Distributed Queries enabled' }
        @{ Name = 'cross db ownership chaining'; Code = 'SEC_CROSS_DB_CHAIN'; Severity = 'Medium'; Label = 'Cross DB Ownership Chaining enabled' }
        @{ Name = 'remote admin connections'; Code = 'SEC_REMOTE_DAC'; Severity = 'Low'; Label = 'Remote DAC enabled' }
    )
    foreach ($sac in $surfaceAreaChecks) {
        Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode $sac.Code -ScriptBlock {
            $val = Get-DbaSpConfigure -SqlInstance $ServerObject -Name $sac.Name -ErrorAction Stop
            if ($val -and [int]$val.RunningValue -eq 1) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode $sac.Code -Category 'Security' -Severity $sac.Severity `
                    -CheckName $sac.Label -Detail "$($sac.Name) is currently enabled at the instance level."
            }
            elseif ($val) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode $sac.Code -Category 'Security' -Severity 'OK' `
                    -CheckName $sac.Label -Detail "$($sac.Name) is disabled, as expected."
            }
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'SEC_LOGINS' -ScriptBlock {
        $logins = Get-DbaLogin -SqlInstance $ServerObject -ErrorAction Stop
        $sa = $logins | Where-Object { $_.id -eq 1 }
        if ($sa -and $sa.IsDisabled -eq $false) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SEC_SA_ENABLED' -Category 'Security' -Severity 'Medium' `
                -CheckName 'sa login enabled' -ObjectName $sa.Name -Detail "The built-in sa login ('$($sa.Name)') is enabled."
        }
        elseif ($sa) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SEC_SA_ENABLED' -Category 'Security' -Severity 'OK' `
                -CheckName 'sa login disabled' -ObjectName $sa.Name -Detail "The built-in sa login is disabled, as expected."
        }
        $builtins = $logins | Where-Object { $_.Name -like 'BUILTIN\*' -and $_.IsDisabled -eq $false }
        foreach ($b in $builtins) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SEC_BUILTIN_LOGIN' -Category 'Security' -Severity 'High' `
                -CheckName 'Broad BUILTIN Windows group login' -ObjectName $b.Name -Detail "'$($b.Name)' is an enabled login granting SQL access to a broad Windows group."
        }
        if ($builtins.Count -eq 0) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SEC_BUILTIN_LOGIN' -Category 'Security' -Severity 'OK' `
                -CheckName 'No broad BUILTIN logins' -Detail "No enabled BUILTIN\* Windows group logins found."
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'SEC_LINKEDSRV' -ScriptBlock {
        $linked = Get-DbaLinkedServer -SqlInstance $ServerObject -ErrorAction Stop
        $flagged = 0
        foreach ($ls in $linked) {
            if ($ls.DataAccess -and -not $ls.Impersonate -and -not $ls.RemoteUser) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SEC_LINKED_SRV_NOAUTH' -Category 'Security' -Severity 'Low' `
                    -CheckName 'Linked server security context' -ObjectName $ls.Name -Detail "Linked server '$($ls.Name)' has no explicit remote login mapping configured - verify the security context is intentional."
                $flagged++
            }
        }
        if ($linked.Count -gt 0 -and $flagged -eq 0) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SEC_LINKEDSRV' -Category 'Security' -Severity 'OK' `
                -CheckName 'Linked server security contexts' -Detail "$($linked.Count) linked server(s) reviewed; all have an explicit security context."
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'TEMPDB_FILES' -ScriptBlock {
        $cpuCount = (Get-DbaComputerSystem -ComputerName $ServerObject.ComputerName -ErrorAction SilentlyContinue).NumberLogicalProcessors
        $tempdbFiles = Get-DbaDbFile -SqlInstance $ServerObject -Database tempdb -ErrorAction Stop | Where-Object { $_.TypeDescription -eq 'ROWS' }
        if ($cpuCount -and $tempdbFiles) {
            $recommended = [Math]::Min($cpuCount, 8)
            $sizes = $tempdbFiles | Select-Object -ExpandProperty Size -Unique
            $issue = $false
            if ($tempdbFiles.Count -lt $recommended) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'TEMPDB_FILE_COUNT' -Category 'Configuration' -Severity 'Low' `
                    -CheckName 'tempdb data file count' -Detail "$($tempdbFiles.Count) tempdb data file(s) present against $cpuCount logical CPUs (recommended up to $recommended)."
                $issue = $true
            }
            if ($sizes.Count -gt 1) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'TEMPDB_FILE_COUNT' -Category 'Configuration' -Severity 'Low' `
                    -CheckName 'tempdb data files unequal size' -Detail "tempdb data files are not equally sized, which can cause uneven allocation (proportional fill)."
                $issue = $true
            }
            if (-not $issue) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'TEMPDB_FILE_COUNT' -Category 'Configuration' -Severity 'OK' `
                    -CheckName 'tempdb data files' -Detail "$($tempdbFiles.Count) equally-sized file(s) against $cpuCount logical CPUs."
            }
        }
    }
    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'CPU_SCHEDULERS' -ScriptBlock {
        # Schedulers that are not VISIBLE ONLINE mean CPU cores SQL Server cannot use -
        # classically Standard Edition's socket/core cap on a box with more cores than
        # the edition licence allows. Silent capacity loss unless someone looks.
        $sched = Invoke-DbaQuery -SqlInstance $ServerObject -Query @"
SELECT COUNT(*) AS [total],
       SUM(CASE WHEN [status] <> 'VISIBLE ONLINE' THEN 1 ELSE 0 END) AS [unusable]
FROM sys.dm_os_schedulers WHERE [scheduler_id] < 1000;
"@ -ErrorAction Stop
        if ($sched -and [int]$sched.unusable -gt 0) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'CPU_CORES_UNUSABLE' -Category 'Configuration' -Severity 'High' `
                -CheckName 'CPU cores unusable by SQL Server' -Detail "$($sched.unusable) of $($sched.total) scheduler(s) are not VISIBLE ONLINE - typically an edition licensing cap (e.g. Standard Edition core limits) leaving CPU capacity unused."
        }
        elseif ($sched) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'CPU_SCHEDULERS' -Category 'Configuration' -Severity 'OK' `
                -CheckName 'CPU cores all usable' -Detail "All $($sched.total) scheduler(s) are VISIBLE ONLINE."
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'SEC_STARTUP_PROCS' -ScriptBlock {
        # Procedures marked to auto-execute at instance startup are a persistence
        # mechanism worth eyeballing. sp_ssis_startup is the common legitimate one.
        $procs = Invoke-DbaQuery -SqlInstance $ServerObject -Query @"
SELECT [name] FROM sys.procedures WITH (NOLOCK)
WHERE OBJECTPROPERTY([object_id], 'ExecIsStartUp') = 1 AND [name] <> 'sp_ssis_startup';
"@ -ErrorAction Stop
        if ($procs) {
            foreach ($p in $procs) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SEC_STARTUP_PROC' -Category 'Security' -Severity 'Medium' `
                    -CheckName 'Startup procedure present' -ObjectName $p.name -Detail "Procedure '$($p.name)' is marked to auto-execute at instance startup - verify it is known and intentional."
            }
        }
        else {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SEC_STARTUP_PROCS' -Category 'Security' -Severity 'OK' `
                -CheckName 'No startup procedures' -Detail "No procedures (other than sp_ssis_startup) are marked to auto-execute at startup."
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'BACKUP_VDI' -ScriptBlock {
        # device_type 7 = virtual device (VDI): third-party/VM-snapshot backup tools.
        # Worth surfacing because snapshot tools can take FULL backups that silently
        # reset the differential base and confuse the native backup chain/RPO story.
        $vdi = Invoke-DbaQuery -SqlInstance $ServerObject -Query @"
SELECT MAX([bs].[backup_start_date]) AS [LatestVdiBackup], COUNT(*) AS [VdiCount]
FROM msdb.dbo.backupset [bs] WITH (NOLOCK)
JOIN msdb.dbo.backupmediafamily [mf] WITH (NOLOCK) ON [mf].[media_set_id] = [bs].[media_set_id]
WHERE [mf].[device_type] = 7 AND [bs].[backup_start_date] >= DATEADD(DAY, -8, GETDATE());
"@ -ErrorAction Stop
        if ($vdi -and [int]$vdi.VdiCount -gt 0) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BACKUP_VDI_DETECTED' -Category 'Backups' -Severity 'Low' `
                -CheckName 'Virtual device (VDI) backups detected' -Detail "$($vdi.VdiCount) VDI (device_type 7) backup(s) in the last 8 days; latest $($vdi.LatestVdiBackup). Typically a VM/snapshot backup tool - confirm it is intended and not disrupting the native backup chain (e.g. resetting differential bases)."
        }
        else {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BACKUP_VDI' -Category 'Backups' -Severity 'OK' `
                -CheckName 'No virtual device (VDI) backups' -Detail "No device_type 7 backups recorded in the last 8 days."
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'DB_RESTORES_RECENT' -ScriptBlock {
        # Awareness rather than a fault: restores in the last 24h are worth knowing
        # about on any estate (unexpected restores can indicate incidents or drift).
        $restores = Invoke-DbaQuery -SqlInstance $ServerObject -Query @"
SELECT [rh].[destination_database_name] AS [DatabaseName], MAX([rh].[restore_date]) AS [LastRestore]
FROM msdb.dbo.restorehistory [rh] WITH (NOLOCK)
WHERE [rh].[restore_date] >= DATEADD(DAY, -1, GETDATE())
GROUP BY [rh].[destination_database_name];
"@ -ErrorAction Stop
        foreach ($r in $restores) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_RESTORED_RECENTLY' -Category 'Availability' -Severity 'Informational' `
                -CheckName 'Database restored in last 24 hours' -ObjectName $r.DatabaseName -Detail "Most recent restore: $($r.LastRestore). Confirm it was expected."
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'SSIS_LONG_RUNNING' -ScriptBlock {
        # Only meaningful where the SSIS catalog exists and is online.
        $ssis = Invoke-DbaQuery -SqlInstance $ServerObject -Query @"
IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = 'SSISDB' AND [state_desc] = 'ONLINE')
    SELECT [ex].[package_name], [ex].[start_time],
           DATEDIFF(MINUTE, CAST([ex].[start_time] AS datetime2), SYSDATETIME()) AS [RunningMinutes]
    FROM SSISDB.catalog.executions [ex] WITH (NOLOCK)
    WHERE [ex].[end_time] IS NULL AND [ex].[status] = 2
      AND DATEDIFF(MINUTE, CAST([ex].[start_time] AS datetime2), SYSDATETIME()) > $($Thresholds['SsisLongRunningMinutes']);
"@ -ErrorAction Stop
        foreach ($s in $ssis) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SSIS_LONG_RUNNING' -Category 'Agent' -Severity 'Medium' `
                -CheckName 'SSIS execution running unusually long' -ObjectName $s.package_name -Detail ("Package has been running {0:N0} minutes (since {1}); threshold {2} minutes." -f $s.RunningMinutes, $s.start_time, $Thresholds['SsisLongRunningMinutes'])
        }
    }
}

#endregion

#region ------------------------------- Database-level checks -------------------------------

function Invoke-DatabaseLevelChecks {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] $ServerObject,
        [Parameter(Mandatory)] [hashtable]$Thresholds,
        [Parameter(Mandatory)] [hashtable]$Config
    )

    $databases = Get-DbaDatabase -SqlInstance $ServerObject -ErrorAction Stop
    $onlineDatabases = $databases | Where-Object { $_.Status -eq 'Normal' }

    # --- Backups ---
    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'BACKUPS' -ScriptBlock {
        $backupInfo = Get-DbaLastBackup -SqlInstance $ServerObject -ErrorAction Stop
        $now = Get-Date
        foreach ($b in $backupInfo) {
            if ($b.Database -eq 'tempdb') { continue }
            # Per-database isolation: one malformed row (e.g. a DBNull date on an
            # OFFLINE/RESTORING database) must log its own collection issue, not
            # abort the loop and silently leave the rest of the estate unevaluated.
            try {
                $lastFull = ConvertTo-AuditDateTime $b.LastFullBackup
                $lastLog  = ConvertTo-AuditDateTime $b.LastLogBackup
                $lastDiff = ConvertTo-AuditDateTime $b.LastDiffBackup
                $dbIssue = $false
                if (-not $lastFull -or ($now - $lastFull).TotalDays -gt $Thresholds['FullBackupMaxDays']) {
                    $age = if ($lastFull) { "{0:N1} days ago" -f ($now - $lastFull).TotalDays } else { 'never recorded' }
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BACKUP_FULL_STALE' -Category 'Backups' -Severity 'High' `
                        -CheckName 'Full backup currency' -ObjectName $b.Database -Detail "Last full backup: $age (threshold: $($Thresholds['FullBackupMaxDays']) days)."
                    $dbIssue = $true
                }
                if ($b.RecoveryModel -in @('Full', 'BulkLogged')) {
                    if (-not $lastLog -or ($now - $lastLog).TotalMinutes -gt $Thresholds['LogBackupMaxMinutes']) {
                        $age = if ($lastLog) { "{0:N0} minutes ago" -f ($now - $lastLog).TotalMinutes } else { 'never recorded' }
                        Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BACKUP_LOG_STALE' -Category 'Backups' -Severity 'High' `
                            -CheckName 'Log backup currency' -ObjectName $b.Database -Detail "Recovery model $($b.RecoveryModel); last log backup: $age (threshold: $($Thresholds['LogBackupMaxMinutes']) min)."
                        $dbIssue = $true
                    }
                }
                # Diff currency: only meaningful where diffs are evidently part of the
                # strategy (at least one diff on record) - avoids noise on estates that
                # run full+log only. Threshold configurable via -DiffBackupMaxHours.
                # A diff is only stale if no full backup has superseded it either.
                if ($lastDiff -and ($now - $lastDiff).TotalHours -gt $Thresholds['DiffBackupMaxHours'] -and
                    (-not $lastFull -or ($now - $lastFull).TotalHours -gt $Thresholds['DiffBackupMaxHours'])) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BACKUP_DIFF_STALE' -Category 'Backups' -Severity 'Medium' `
                        -CheckName 'Differential backup currency' -ObjectName $b.Database -Detail ("Diffs are in use for this database but the last one was {0:N1} hours ago (threshold: {1}h), with no newer full backup." -f ($now - $lastDiff).TotalHours, $Thresholds['DiffBackupMaxHours'])
                    $dbIssue = $true
                }
                if (-not $dbIssue) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'BACKUPS' -Category 'Backups' -Severity 'OK' `
                        -CheckName 'Backup currency' -ObjectName $b.Database -Detail "Full and (where applicable) log backups are within threshold."
                }
            }
            catch {
                Add-CollectionIssue -SqlInstance $SqlInstance -CheckCode "BACKUPS ($($b.Database))" -ErrorMessage $_.Exception.Message
            }
        }
    }

    # --- CHECKDB currency ---
    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'CHECKDB' -ScriptBlock {
        $names = $onlineDatabases | Where-Object { $_.Name -ne 'tempdb' } | Select-Object -ExpandProperty Name
        if ($names) {
            $checkResults = Get-DbaLastGoodCheckDb -SqlInstance $ServerObject -Database $names -ErrorAction Stop
            foreach ($c in $checkResults) {
                if (-not $c.LastGoodCheckDb) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'CHECKDB_MISSING' -Category 'Integrity' -Severity 'High' `
                        -CheckName 'DBCC CHECKDB history' -ObjectName $c.Database -Detail "No successful CHECKDB has ever been recorded."
                }
                elseif (((Get-Date) - $c.LastGoodCheckDb).TotalDays -gt $Thresholds['CheckDbMaxDays']) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'CHECKDB_STALE' -Category 'Integrity' -Severity 'Medium' `
                        -CheckName 'DBCC CHECKDB currency' -ObjectName $c.Database -Detail ("Last good CHECKDB: {0:N0} days ago (threshold: {1} days)." -f ((Get-Date) - $c.LastGoodCheckDb).TotalDays, $Thresholds['CheckDbMaxDays'])
                }
                else {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'CHECKDB' -Category 'Integrity' -Severity 'OK' `
                        -CheckName 'DBCC CHECKDB currency' -ObjectName $c.Database -Detail ("Last good CHECKDB: {0:N0} days ago." -f ((Get-Date) - $c.LastGoodCheckDb).TotalDays)
                }
            }
        }
    }

    # --- Suspect pages ---
    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'SUSPECTPAGES' -ScriptBlock {
        $suspect = Get-DbaSuspectPage -SqlInstance $ServerObject -ErrorAction Stop
        foreach ($sp in $suspect) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SUSPECT_PAGES' -Category 'Integrity' -Severity 'High' `
                -CheckName 'Suspect pages recorded' -ObjectName $sp.Database -Detail "Suspect page recorded in msdb (file_id $($sp.FileId), page_id $($sp.PageId), event $($sp.EventType))."
        }
        if (-not $suspect -or $suspect.Count -eq 0) {
            Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'SUSPECTPAGES' -Category 'Integrity' -Severity 'OK' `
                -CheckName 'No suspect pages' -Detail "No suspect pages recorded in msdb for this instance."
        }
    }

    # Native compat baseline: use model database's compat level as the instance's "current" native level.
    # dbatools/SMO may expose this as .CompatibilityLevel (SMO's native name) or .Compatibility depending on
    # version, and the value can render as a plain number or an enum-like string (e.g. 'Version150') - so
    # Get-CompatLevelNumber (defined earlier, top-level) extracts digits defensively.
    $modelDb = $databases | Where-Object { $_.Name -eq 'model' } | Select-Object -First 1
    $modelCompatRaw = if ($modelDb.PSObject.Properties.Name -contains 'CompatibilityLevel') { $modelDb.CompatibilityLevel } else { $modelDb.Compatibility }
    $modelCompat = Get-CompatLevelNumber $modelCompatRaw

    foreach ($db in $databases) {
        $dbName = $db.Name
        if ($dbName -in @('tempdb')) { continue }

        Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'DBSTATE' -ScriptBlock {
            $issue = $false
            if ($db.Status -match 'Suspect|RecoveryPending|Offline|EmergencyMode') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_STATE_BAD' -Category 'Availability' -Severity 'High' `
                    -CheckName 'Database state' -ObjectName $dbName -Detail "Database status is '$($db.Status)'."
                $issue = $true
            }
            # Belt and braces: SMO surfaces standby either via the IsStandby flag or
            # a 'Standby' token in the Status enum, depending on version/path taken.
            if ($db.IsStandby -or $db.Status -match 'Standby') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_STANDBY' -Category 'Availability' -Severity 'Informational' `
                    -CheckName 'Database in STANDBY' -ObjectName $dbName -Detail "Database is in STANDBY mode - confirm this is an intentional log-shipping secondary."
                $issue = $true
            }
            if ($db.Status -match 'Restoring') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_RESTORING' -Category 'Availability' -Severity 'Medium' `
                    -CheckName 'Database in RESTORING' -ObjectName $dbName -Detail "Database is in RESTORING state - confirm this is expected."
                $issue = $true
            }
            if ($db.IsReadOnly -and $dbName -ne 'model' -and $dbName -notlike '### TEST RESTORE ###*') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_READONLY' -Category 'Configuration' -Severity 'Informational' `
                    -CheckName 'Database READ_ONLY' -ObjectName $dbName -Detail "Database is READ_ONLY - confirm this is intentional."
                $issue = $true
            }
            if (-not $issue) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DBSTATE' -Category 'Availability' -Severity 'OK' `
                    -CheckName 'Database state' -ObjectName $dbName -Detail "Database is ONLINE/Normal with no state concerns."
            }
        }

        Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'DBFLAGS' -ScriptBlock {
            $issue = $false
            if ($db.Trustworthy -and $dbName -ne 'msdb') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_TRUSTWORTHY' -Category 'Security' -Severity 'High' `
                    -CheckName 'TRUSTWORTHY enabled' -ObjectName $dbName -Detail "TRUSTWORTHY is ON for a database other than msdb."
                $issue = $true
            }
            $dbCompatRaw = if ($db.PSObject.Properties.Name -contains 'CompatibilityLevel') { $db.CompatibilityLevel } else { $db.Compatibility }
            $dbCompat = Get-CompatLevelNumber $dbCompatRaw
            if ($modelCompat -and $dbCompat -and ($dbCompat -lt ($modelCompat - 10))) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_COMPAT_LOW' -Category 'Configuration' -Severity 'Low' `
                    -CheckName 'Compatibility level behind instance native' -ObjectName $dbName -Detail "Compatibility level $dbCompat vs instance native $modelCompat (model db)."
                $issue = $true
            }
            if ($db.AutoShrink) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_AUTOSHRINK' -Category 'Configuration' -Severity 'Medium' `
                    -CheckName 'AUTO_SHRINK enabled' -ObjectName $dbName -Detail "AUTO_SHRINK is ON."
                $issue = $true
            }
            if ($db.AutoClose) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_AUTOCLOSE' -Category 'Configuration' -Severity 'Medium' `
                    -CheckName 'AUTO_CLOSE enabled' -ObjectName $dbName -Detail "AUTO_CLOSE is ON."
                $issue = $true
            }
            if ($db.PageVerify -ne 'Checksum') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_PAGEVERIFY' -Category 'Integrity' -Severity 'Medium' `
                    -CheckName 'Page verify not CHECKSUM' -ObjectName $dbName -Detail "PAGE_VERIFY is set to '$($db.PageVerify)' instead of CHECKSUM."
                $issue = $true
            }
            if ($db.Owner -and $db.Owner -ne 'sa') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_OWNER_NOT_SA' -Category 'Configuration' -Severity 'Low' `
                    -CheckName 'Database owner not sa' -ObjectName $dbName -Detail "Database owner is '$($db.Owner)' rather than sa."
                $issue = $true
            }
            if (-not $issue) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DBFLAGS' -Category 'Configuration' -Severity 'OK' `
                    -CheckName 'Database configuration best practices' -ObjectName $dbName -Detail "Trustworthy/compat/autoshrink/autoclose/pageverify/owner all within policy."
            }
        }

        Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'DBFILEGROWTH' -ScriptBlock {
            $files = Get-DbaDbFile -SqlInstance $ServerObject -Database $dbName -ErrorAction Stop
            # @() wrap: a single matching file otherwise comes back as a scalar
            # whose .Count resolves to nothing, rendering " file(s)" in the report.
            $pctGrowth = @($files | Where-Object { $_.GrowthType -eq 'Percent' })
            if ($pctGrowth.Count -gt 0) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_PERCENT_GROWTH' -Category 'Configuration' -Severity 'Low' `
                    -CheckName 'File autogrowth set to percent' -ObjectName $dbName -Detail "$($pctGrowth.Count) file(s) use percentage autogrowth rather than a fixed MB amount."
            }
            else {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DBFILEGROWTH' -Category 'Configuration' -Severity 'OK' `
                    -CheckName 'File autogrowth' -ObjectName $dbName -Detail "All data/log files use fixed-MB autogrowth."
            }
        }

        Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'QUERYSTORE' -ScriptBlock {
            # Query Store shipped in SQL Server 2016 (v13); skip older instances
            # entirely rather than emitting noise. System databases cannot host it,
            # and it is unreadable on inaccessible/non-Normal databases.
            if ($ServerObject.VersionMajor -lt 13) { return }
            if ($dbName -in @('master', 'model', 'msdb')) { return }
            if ($db.Status -ne 'Normal' -or -not $db.IsAccessible) { return }

            $qs = $db.QueryStoreOptions
            if ($null -eq $qs) { return }   # older SMO / edge cases: nothing to assess

            $desired = [string]$qs.DesiredState   # Off | ReadOnly | ReadWrite
            $actual  = [string]$qs.ActualState    # Off | ReadOnly | ReadWrite | Error
            $maxMB   = [double]$qs.MaxStorageSizeInMB
            $curMB   = [double]$qs.CurrentStorageSizeInMB
            $usedPct = if ($maxMB -gt 0) { [Math]::Round(($curMB / $maxMB) * 100, 1) } else { $null }
            $capture = if ($qs.PSObject.Properties['QueryCaptureMode']) { [string]$qs.QueryCaptureMode } else { '' }

            $stateText = "Configured: $desired, actual: $actual"
            if ($null -ne $usedPct) { $stateText += ("; storage {0:N0} of {1:N0} MB ({2:N1}% used)" -f $curMB, $maxMB, $usedPct) }
            if ($capture) { $stateText += "; capture mode: $capture" }

            if ($actual -eq 'Off') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'QDS_NOT_ENABLED' -Category 'Configuration' -Severity 'Low' `
                    -CheckName 'Query Store not enabled' -ObjectName $dbName -Detail "Query Store is OFF - no plan history or regression evidence will be available for this database. $stateText."
            }
            elseif ($actual -eq 'Error') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'QDS_STATE_MISMATCH' -Category 'Configuration' -Severity 'High' `
                    -CheckName 'Query Store in ERROR state' -ObjectName $dbName -Detail "Query Store actual state is ERROR - it is not collecting. Check the SQL Server error log and consider sp_query_store_consistency_check. $stateText."
            }
            elseif ($desired -ne $actual) {
                # The silent failure mode: configured READ_WRITE but actually
                # READ_ONLY, almost always because MAX_STORAGE_SIZE_MB filled.
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'QDS_STATE_MISMATCH' -Category 'Configuration' -Severity 'Medium' `
                    -CheckName 'Query Store state mismatch' -ObjectName $dbName -Detail "Query Store is configured $desired but actually $actual - it has stopped collecting, typically because storage filled. $stateText."
            }
            elseif ($actual -eq 'ReadOnly') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'QDS_READ_ONLY' -Category 'Configuration' -Severity 'Informational' `
                    -CheckName 'Query Store READ_ONLY by design' -ObjectName $dbName -Detail "Query Store is deliberately configured READ_ONLY - history is retained but nothing new is collected. $stateText."
            }
            elseif ($null -ne $usedPct -and $usedPct -ge [double]$Thresholds['QueryStoreStoragePercentWarn']) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'QDS_STORAGE_NEAR_FULL' -Category 'Configuration' -Severity 'Medium' `
                    -CheckName 'Query Store storage nearly full' -ObjectName $dbName -Detail "Query Store is READ_WRITE but storage is $usedPct% used (threshold: $($Thresholds['QueryStoreStoragePercentWarn'])%) - it will flip to READ_ONLY and stop collecting when full. $stateText."
            }
            else {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'QUERYSTORE' -Category 'Configuration' -Severity 'OK' `
                    -CheckName 'Query Store' -ObjectName $dbName -Detail "Query Store is READ_WRITE and healthy. $stateText."
            }

            # Independent of state: ALL capture mode on a busy system bloats QDS.
            # AUTO has been the recommended default since SQL 2019.
            if ($capture -eq 'All' -and $actual -ne 'Off') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'QDS_CAPTURE_ALL' -Category 'Configuration' -Severity 'Low' `
                    -CheckName 'Query Store capture mode ALL' -ObjectName $dbName -Detail "QUERY_CAPTURE_MODE is ALL - every ad-hoc statement is captured, which can bloat Query Store; AUTO is recommended."
            }
        }

        Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'ORPHANUSERS' -ScriptBlock {
            $orphans = Get-DbaDbOrphanUser -SqlInstance $ServerObject -Database $dbName -ErrorAction Stop
            foreach ($o in $orphans) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DB_ORPHANED_USER' -Category 'Security' -Severity 'Low' `
                    -CheckName 'Orphaned database user' -ObjectName "$dbName.$($o.User)" -Detail "User '$($o.User)' has no matching server login (SID mismatch or missing login)."
            }
            if (-not $orphans -or $orphans.Count -eq 0) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'ORPHANUSERS' -Category 'Security' -Severity 'OK' `
                    -CheckName 'No orphaned users' -ObjectName $dbName -Detail "No orphaned database users found."
            }
        }
    }

    # --- TDE awareness (estate/instance level, not per-db, to avoid noise) ---
    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'TDE' -ScriptBlock {
        $userDbs = $onlineDatabases | Where-Object { $_.Name -notin @('master', 'model', 'msdb', 'tempdb') }
        if ($userDbs) {
            $encStatus = Get-DbaDbEncryption -SqlInstance $ServerObject -ErrorAction Stop
            $encryptedCount = ($encStatus | Where-Object { $_.EncryptionEnabled }).Count
            if ($encryptedCount -eq 0) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'TDE_NOT_ENABLED' -Category 'Security' -Severity 'Informational' `
                    -CheckName 'No databases using TDE' -Detail "None of $($userDbs.Count) user database(s) have Transparent Data Encryption enabled."
            }
            else {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'TDE' -Category 'Security' -Severity 'OK' `
                    -CheckName 'TDE in use' -Detail "$encryptedCount of $($userDbs.Count) user database(s) have TDE enabled."
            }
        }
    }
}

#endregion

#region ------------------------------- Agent job checks -------------------------------

function Invoke-AgentJobChecks {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] $ServerObject,
        [Parameter(Mandatory)] [hashtable]$Config
    )

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'AGENTJOBS' -ScriptBlock {
        $jobs = Get-DbaAgentJob -SqlInstance $ServerObject -ErrorAction Stop
        foreach ($job in $jobs) {
            if (-not $job.Enabled) { continue }

            $issue = $false
            if ($job.LastRunOutcome -eq 'Failed') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'AGENT_JOB_FAILED' -Category 'Agent' -Severity 'High' `
                    -CheckName 'Agent job last run failed' -ObjectName $job.Name -Detail "Last run outcome: Failed (last run: $($job.LastRunDate))."
                $issue = $true
            }
            if (-not $job.HasSchedule) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'AGENT_JOB_NO_SCHEDULE' -Category 'Agent' -Severity 'Low' `
                    -CheckName 'Agent job has no schedule' -ObjectName $job.Name -Detail "Job is enabled but has no attached schedule - confirm this is alert/manually triggered by design."
                $issue = $true
            }
            if ($job.OwnerLoginName -and $job.OwnerLoginName -ne 'sa') {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'AGENT_JOB_NO_OWNER' -Category 'Agent' -Severity 'Low' `
                    -CheckName 'Agent job not owned by sa' -ObjectName $job.Name -Detail "Job owner is '$($job.OwnerLoginName)' rather than sa."
                $issue = $true
            }
            $emailLevel = [string]$job.EmailLevel
            if ($emailLevel -in @('Never', '')) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'AGENT_JOB_NO_NOTIFY' -Category 'Agent' -Severity 'Low' `
                    -CheckName 'Agent job has no failure notification' -ObjectName $job.Name -Detail "No operator email notification configured on failure."
                $issue = $true
            }
            if (-not $issue) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'AGENTJOBS' -Category 'Agent' -Severity 'OK' `
                    -CheckName 'Agent job healthy' -ObjectName $job.Name -Detail "Last run succeeded; scheduled, sa-owned, and notifies on failure."
            }
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'AGENTJOBS_RUNNING' -ScriptBlock {
        $running = Get-DbaRunningJob -SqlInstance $ServerObject -ErrorAction SilentlyContinue
        foreach ($r in $running) {
            $elapsedSeconds = $null
            if ($r.PSObject.Properties.Name -contains 'ElapsedTime' -and $r.ElapsedTime) {
                $elapsedSeconds = $r.ElapsedTime.TotalSeconds
            }
            if (-not $elapsedSeconds) { continue }

            $history = Get-DbaAgentJobHistory -SqlInstance $ServerObject -Job $r.JobName -ErrorAction SilentlyContinue |
                Where-Object { $_.Status -eq 'Succeeded' -and $_.StepId -eq 0 } |
                Select-Object -First 10
            $avgSeconds = 0
            if ($history) {
                $durations = $history | ForEach-Object {
                    if ($_.PSObject.Properties.Name -contains 'Duration' -and $_.Duration) { $_.Duration.TotalSeconds } else { $null }
                } | Where-Object { $_ -ne $null }
                if ($durations) { $avgSeconds = ($durations | Measure-Object -Average).Average }
            }

            $thresholdSeconds = if ($avgSeconds -gt 30) { $avgSeconds * 1.5 } else { 900 }  # 15 min floor for very short/unknown-history jobs
            if ($elapsedSeconds -gt $thresholdSeconds) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'AGENT_JOB_LONG_RUNNING' -Category 'Agent' -Severity 'Medium' `
                    -CheckName 'Agent job running longer than usual' -ObjectName $r.JobName `
                    -Detail ("Currently running {0:N0} min vs a historical average of {1:N0} min." -f ($elapsedSeconds / 60), ($avgSeconds / 60))
            }
        }
    }
}

#endregion

#region ------------------------------- HA/DR checks -------------------------------

function Invoke-HadrChecks {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] $ServerObject,
        [Parameter(Mandatory)] [hashtable]$Config
    )

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'LOGSHIPPING' -ScriptBlock {
        $lsStatus = Test-DbaDbLogShipStatus -SqlInstance $ServerObject -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
        # Silent when log shipping isn't configured at all - only reports when there's something to say.
        foreach ($ls in $lsStatus) {
            $outOfDateProp = $ls.PSObject.Properties | Where-Object { $_.Name -like '*OutOfDate*' -and $_.Value -eq $true }
            if ($outOfDateProp) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'HADR_LOGSHIP_LAG' -Category 'HA/DR' -Severity 'High' `
                    -CheckName 'Log shipping restore/backup out of date' -ObjectName $ls.Database -Detail "Log shipping status flagged out-of-date on: $($outOfDateProp.Name -join ', ')."
            }
            else {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'LOGSHIPPING' -Category 'HA/DR' -Severity 'OK' `
                    -CheckName 'Log shipping healthy' -ObjectName $ls.Database -Detail "Log shipping backup/restore is current."
            }
        }
    }

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'AVAILABILITYGROUPS' -ScriptBlock {
        $agDbs = Get-DbaAgDatabase -SqlInstance $ServerObject -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
        # Silent when no AG is configured - only reports when there's something to say.
        foreach ($agdb in $agDbs) {
            $issue = $false
            if ($agdb.SynchronizationState -and $agdb.SynchronizationState -notin @('Synchronized', 'Synchronizing')) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'HADR_AG_UNHEALTHY' -Category 'HA/DR' -Severity 'High' `
                    -CheckName 'AG database not synchronized' -ObjectName "$($agdb.AvailabilityGroup).$($agdb.Name)" -Detail "SynchronizationState: $($agdb.SynchronizationState)."
                $issue = $true
            }
            if ($agdb.PSObject.Properties.Name -contains 'IsSuspended' -and $agdb.IsSuspended) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'HADR_AG_UNHEALTHY' -Category 'HA/DR' -Severity 'High' `
                    -CheckName 'AG database suspended' -ObjectName "$($agdb.AvailabilityGroup).$($agdb.Name)" -Detail "Database movement is suspended within the Availability Group."
                $issue = $true
            }
            if (-not $issue) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'AVAILABILITYGROUPS' -Category 'HA/DR' -Severity 'OK' `
                    -CheckName 'AG database healthy' -ObjectName "$($agdb.AvailabilityGroup).$($agdb.Name)" -Detail "SynchronizationState: $($agdb.SynchronizationState), not suspended."
            }
        }
    }
}

#endregion

#region ------------------------------- Storage checks -------------------------------

function Invoke-StorageChecks {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] $ServerObject,
        [Parameter(Mandatory)] [hashtable]$Thresholds,
        [Parameter(Mandatory)] [hashtable]$Config
    )

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'DISKSPACE' -ScriptBlock {
        $computerName = (Resolve-DbaNetworkName -ComputerName $ServerObject.ComputerName -ErrorAction Stop).ComputerName
        $disks = Get-DbaDiskSpace -ComputerName $computerName -ErrorAction Stop
        foreach ($d in $disks) {
            if ($null -eq $d.PercentFree) { continue }
            # Get-DbaDiskSpace returns Capacity as a dbatools [Size] object. Dividing
            # that by 1GB yields another [Size] whose ToString() renders as e.g.
            # "475 B", producing garbage like "475 B GB" in the report. Use the
            # .Gigabyte property where present, falling back to a plain byte division.
            $capGB = if ($d.Capacity.PSObject.Properties['Gigabyte']) { [double]$d.Capacity.Gigabyte } else { [double]$d.Capacity / 1GB }
            $Script:DiskSpaceRaw.Add([PSCustomObject]@{
                SqlInstance  = $SqlInstance
                ComputerName = $computerName
                Volume       = $d.Name
                CapacityGB   = [Math]::Round($capGB, 1)
                PercentFree  = [Math]::Round($d.PercentFree, 1)
            })
            if ($d.PercentFree -le $Thresholds['DiskFreePercentCrit']) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DISK_SPACE_CRIT' -Category 'Storage' -Severity 'High' `
                    -CheckName 'Disk space critically low' -ObjectName $d.Name -Detail ("{0:N1}% free of {1:N0} GB." -f $d.PercentFree, $capGB)
            }
            elseif ($d.PercentFree -le $Thresholds['DiskFreePercentWarn']) {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DISK_SPACE_LOW' -Category 'Storage' -Severity 'Medium' `
                    -CheckName 'Disk space low' -ObjectName $d.Name -Detail ("{0:N1}% free of {1:N0} GB." -f $d.PercentFree, $capGB)
            }
            else {
                Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'DISKSPACE' -Category 'Storage' -Severity 'OK' `
                    -CheckName 'Disk space healthy' -ObjectName $d.Name -Detail ("{0:N1}% free of {1:N0} GB." -f $d.PercentFree, $capGB)
            }
        }
    }
}

#endregion

#region ------------------------------- Perfmon capture -------------------------------

$Script:PerfmonSamples = New-Object System.Collections.Generic.List[object]
$Script:DiskSpaceRaw = New-Object System.Collections.Generic.List[object]

function Get-PerfmonModeSettings {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$Mode,
        [int]$DurationSecondsOverride,
        [int]$IntervalSecondsOverride
    )
    switch ($Mode) {
        'Quick'    { $duration = 1;   $interval = 1 }
        'Trend'    { $duration = 90;  $interval = 2 }
        'Extended' { $duration = 480; $interval = 5 }
    }
    if ($DurationSecondsOverride) { $duration = $DurationSecondsOverride }
    if ($IntervalSecondsOverride) { $interval = $IntervalSecondsOverride }
    $samples = if ($Mode -eq 'Quick') { 1 } else { [Math]::Max(2, [Math]::Floor($duration / $interval)) }
    return @{ Duration = $duration; Interval = $interval; MaxSamples = $samples }
}

function Get-SqlPerfCounterPaths {
    [CmdletBinding()]
    param([Parameter(Mandatory)] $ServerObject)

    $isDefault = [string]::IsNullOrEmpty($ServerObject.InstanceName)
    $objPrefix = if ($isDefault) { 'SQLServer' } else { 'MSSQL$' + $ServerObject.InstanceName }

    return @(
        '\Processor(_Total)\% Processor Time'
        '\System\Processor Queue Length'
        '\Memory\Available MBytes'
        '\PhysicalDisk(_Total)\Avg. Disk sec/Read'
        '\PhysicalDisk(_Total)\Avg. Disk sec/Write'
        '\PhysicalDisk(_Total)\Avg. Disk Queue Length'
        "\${objPrefix}:Buffer Manager\Page life expectancy"
        "\${objPrefix}:Buffer Manager\Buffer cache hit ratio"
        "\${objPrefix}:SQL Statistics\Batch Requests/sec"
        "\${objPrefix}:SQL Statistics\SQL Compilations/sec"
        "\${objPrefix}:SQL Statistics\SQL Re-Compilations/sec"
        "\${objPrefix}:General Statistics\User Connections"
        "\${objPrefix}:General Statistics\Processes blocked"
        "\${objPrefix}:Locks(_Total)\Lock Waits/sec"
        "\${objPrefix}:Memory Manager\Memory Grants Pending"
        "\${objPrefix}:Memory Manager\Total Server Memory (KB)"
    )
}

# Best-effort path using dbatools' genuine Windows Data Collector Set wrappers.
# Only attempted when -PreferDbatoolsPerfmon is supplied; any failure here
# falls through to the native Get-Counter path with no user-visible error.
function Try-DbatoolsPerfmonCapture {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$ComputerName,
        [Parameter(Mandatory)] [string[]]$CounterPaths,
        [Parameter(Mandatory)] [hashtable]$ModeSettings
    )
    $setName = "SqlEstateAudit_$([Guid]::NewGuid().ToString('N').Substring(0,8))"
    try {
        if (-not (Get-Command Get-DbaPfDataCollectorSetTemplate -ErrorAction SilentlyContinue)) { return $null }

        New-DbaPfDataCollectorSet -ComputerName $ComputerName -Name $setName -ErrorAction Stop | Out-Null
        # Custom counter sets vary by dbatools version - if this cmdlet shape doesn't
        # match, we bail out cleanly to the native fallback rather than guessing.
        Add-DbaPfDataCollectorCounter -ComputerName $ComputerName -CollectorSet $setName -Counter $CounterPaths -ErrorAction Stop | Out-Null
        Start-DbaPfDataCollectorSet -ComputerName $ComputerName -CollectorSet $setName -ErrorAction Stop | Out-Null
        Start-Sleep -Seconds $ModeSettings.Duration
        Stop-DbaPfDataCollectorSet -ComputerName $ComputerName -CollectorSet $setName -ErrorAction Stop | Out-Null
        $samples = Get-DbaPfDataCollectorCounterSample -ComputerName $ComputerName -CollectorSet $setName -ErrorAction Stop
        Remove-DbaPfDataCollectorSet -ComputerName $ComputerName -CollectorSet $setName -ErrorAction SilentlyContinue | Out-Null
        return $samples
    }
    catch {
        try { Remove-DbaPfDataCollectorSet -ComputerName $ComputerName -CollectorSet $setName -ErrorAction SilentlyContinue | Out-Null } catch { }
        return $null
    }
}

function Invoke-PerfmonCapture {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] $ServerObject,
        [Parameter(Mandatory)] [string]$Mode,
        [int]$DurationSecondsOverride,
        [int]$IntervalSecondsOverride,
        [switch]$PreferDbatoolsPerfmon,
        [Parameter(Mandatory)] [hashtable]$Config
    )

    Invoke-AuditCheck -SqlInstance $SqlInstance -CheckCode 'PERFMON' -ScriptBlock {
        $modeSettings = Get-PerfmonModeSettings -Mode $Mode -DurationSecondsOverride $DurationSecondsOverride -IntervalSecondsOverride $IntervalSecondsOverride
        $computerName = (Resolve-DbaNetworkName -ComputerName $ServerObject.ComputerName -ErrorAction Stop).ComputerName
        $counterPaths = Get-SqlPerfCounterPaths -ServerObject $ServerObject

        Write-AuditLog "[$SqlInstance] Capturing Perfmon (${Mode}: $($modeSettings.Duration)s / $($modeSettings.MaxSamples) sample(s)) on $computerName ..."

        $rawSamples = $null
        $capturedVia = 'Get-Counter'

        if ($PreferDbatoolsPerfmon) {
            $rawSamples = Try-DbatoolsPerfmonCapture -ComputerName $computerName -CounterPaths $counterPaths -ModeSettings $modeSettings
            if ($rawSamples) { $capturedVia = 'dbatools Data Collector Set' }
        }

        $flatSamples = New-Object System.Collections.Generic.List[object]

        if ($rawSamples) {
            foreach ($s in $rawSamples) {
                $flatSamples.Add([PSCustomObject]@{ Path = $s.Path; Timestamp = $s.Timestamp; Value = $s.CookedValue })
            }
        }
        else {
            $getCounterParams = @{
                ComputerName = $computerName
                Counter      = $counterPaths
                MaxSamples   = $modeSettings.MaxSamples
                ErrorAction  = 'Stop'
            }
            if ($modeSettings.MaxSamples -gt 1) { $getCounterParams['SampleInterval'] = $modeSettings.Interval }
            $counterResults = Get-Counter @getCounterParams
            foreach ($result in $counterResults) {
                foreach ($cs in $result.CounterSamples) {
                    $flatSamples.Add([PSCustomObject]@{ Path = $cs.Path; Timestamp = $cs.Timestamp; Value = $cs.CookedValue })
                }
            }
        }

        # Persist every sample for the CSV/trend chart, tagged with instance + capture method.
        foreach ($fs in $flatSamples) {
            $Script:PerfmonSamples.Add([PSCustomObject]@{
                SqlInstance = $SqlInstance
                ComputerName= $computerName
                CapturedVia = $capturedVia
                Counter     = ($fs.Path -split '\\')[-1]
                FullPath    = $fs.Path
                Timestamp   = $fs.Timestamp
                Value       = $fs.Value
            })
        }

        # Threshold evaluation against the captured average per counter.
        $byCounter = $flatSamples | Group-Object Path
        foreach ($grp in $byCounter) {
            $avg = ($grp.Group | Measure-Object -Property Value -Average).Average
            $counterShortName = ($grp.Name -split '\\')[-1]

            if ($grp.Name -like '*% Processor Time*') {
                if ($avg -ge 95) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERFMON_HIGH_CPU' -Category 'Performance' -Severity 'High' `
                        -CheckName 'High CPU during capture window' -Detail ("Average CPU {0:N1}% over the {1}s capture ({2})." -f $avg, $modeSettings.Duration, $Mode)
                }
                elseif ($avg -ge 85) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERFMON_HIGH_CPU' -Category 'Performance' -Severity 'Medium' `
                        -CheckName 'Elevated CPU during capture window' -Detail ("Average CPU {0:N1}% over the {1}s capture ({2})." -f $avg, $modeSettings.Duration, $Mode)
                }
            }
            elseif ($grp.Name -like '*Page life expectancy*') {
                # Rough, widely-used rule of thumb (not NUMA/buffer-pool-size-aware) - flagged as
                # a signal to investigate further, not a definitive verdict.
                if ($avg -lt 300) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERFMON_LOW_PLE' -Category 'Performance' -Severity 'Medium' `
                        -CheckName 'Low Page Life Expectancy' -Detail ("Average PLE {0:N0}s over the capture window - investigate memory pressure." -f $avg)
                }
            }
            elseif ($grp.Name -like '*Avg. Disk sec/Read*' -or $grp.Name -like '*Avg. Disk sec/Write*') {
                $ms = $avg * 1000
                if ($ms -ge 50) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERFMON_HIGH_DISK_LAT' -Category 'Performance' -Severity 'High' `
                        -CheckName 'High disk latency' -ObjectName $counterShortName -Detail ("Average {0:N1} ms over the capture window." -f $ms)
                }
                elseif ($ms -ge 20) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERFMON_HIGH_DISK_LAT' -Category 'Performance' -Severity 'Medium' `
                        -CheckName 'Elevated disk latency' -ObjectName $counterShortName -Detail ("Average {0:N1} ms over the capture window." -f $ms)
                }
            }
            elseif ($grp.Name -like '*Memory Grants Pending*') {
                if ($avg -gt 0) {
                    Add-Finding -Config $Config -SqlInstance $SqlInstance -CheckCode 'PERFMON_MEM_GRANTS_PEND' -Category 'Performance' -Severity 'Medium' `
                        -CheckName 'Memory grants pending observed' -Detail ("Average {0:N1} pending memory grant(s) during the capture window." -f $avg)
                }
            }
        }
    }
}

#endregion

#region ------------------------------- Chart generation (GDI+, no external dependencies) -------------------------------

# Charts are rendered server-side as PNGs using .NET's System.Drawing (GDI+) -
# no Chart.js/D3/internet dependency, so the HTML report stays fully
# self-contained (images are embedded as base64) and the same PNG files are
# reused in the HTML report. Requires Windows (GDI+); if System.Drawing
# is unavailable the chart functions return $null and the reports simply
# omit that image rather than failing the whole run.

$Script:ChartColors = @{
    High          = [System.Drawing.Color]::FromArgb(220, 38, 38)
    Medium        = [System.Drawing.Color]::FromArgb(245, 158, 11)
    Low           = [System.Drawing.Color]::FromArgb(59, 130, 246)
    Informational = [System.Drawing.Color]::FromArgb(107, 114, 128)
    OK            = [System.Drawing.Color]::FromArgb(16, 185, 129)
    Axis          = [System.Drawing.Color]::FromArgb(55, 65, 81)
    Grid          = [System.Drawing.Color]::FromArgb(229, 231, 235)
}

function Initialize-ChartEngine {
    try {
        Add-Type -AssemblyName System.Drawing -ErrorAction Stop
        return $true
    }
    catch {
        Write-AuditLog -Level Warn -Message "System.Drawing unavailable - charts will be skipped ($($_.Exception.Message))."
        return $false
    }
}

function New-DonutChartImage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [array]$Segments,   # PSCustomObject with Label; Value; Color
        [string]$Title = '',
        [int]$Width = 520,
        [int]$Height = 360,
        [Parameter(Mandatory)] [string]$OutFile
    )
    if (-not $Script:ChartEngineOk) { return $null }
    $total = ($Segments | Measure-Object -Property Value -Sum).Sum
    if (-not $total -or $total -le 0) { return $null }

    $bmp = New-Object System.Drawing.Bitmap $Width, $Height
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $g.Clear([System.Drawing.Color]::White)

    $titleFont  = New-Object System.Drawing.Font 'Segoe UI', 13, ([System.Drawing.FontStyle]::Bold)
    $labelFont  = New-Object System.Drawing.Font 'Segoe UI', 10
    $axisBrush  = New-Object System.Drawing.SolidBrush $Script:ChartColors.Axis

    if ($Title) { $g.DrawString($Title, $titleFont, $axisBrush, 10, 8) }

    $diameter = [Math]::Min($Height - 70, 260)
    $ox = 20
    $oy = 45
    $rect = New-Object System.Drawing.Rectangle $ox, $oy, $diameter, $diameter
    $startAngle = -90.0
    foreach ($seg in $Segments) {
        if ($seg.Value -le 0) { continue }
        $sweep = ($seg.Value / $total) * 360.0
        $brush = New-Object System.Drawing.SolidBrush $seg.Color
        $g.FillPie($brush, $rect, $startAngle, $sweep)
        $brush.Dispose()
        $startAngle += $sweep
    }
    # Donut hole
    $holeDiameter = [Math]::Floor($diameter * 0.55)
    $holeOffset = [Math]::Floor(($diameter - $holeDiameter) / 2)
    $whiteBrush = New-Object System.Drawing.SolidBrush ([System.Drawing.Color]::White)
    $g.FillEllipse($whiteBrush, ($ox + $holeOffset), ($oy + $holeOffset), $holeDiameter, $holeDiameter)
    $whiteBrush.Dispose()

    $centerFont = New-Object System.Drawing.Font 'Segoe UI', 16, ([System.Drawing.FontStyle]::Bold)
    $centerText = "$total"
    $centerSize = $g.MeasureString($centerText, $centerFont)
    $g.DrawString($centerText, $centerFont, $axisBrush, ($ox + $diameter/2 - $centerSize.Width/2), ($oy + $diameter/2 - $centerSize.Height/2 - 8))
    $subFont = New-Object System.Drawing.Font 'Segoe UI', 8
    $subText = 'findings'
    $subSize = $g.MeasureString($subText, $subFont)
    $g.DrawString($subText, $subFont, $axisBrush, ($ox + $diameter/2 - $subSize.Width/2), ($oy + $diameter/2 + $centerSize.Height/2 - 10))

    # Legend
    $legendX = $ox + $diameter + 30
    $legendY = $oy + 5
    foreach ($seg in $Segments) {
        $swatchBrush = New-Object System.Drawing.SolidBrush $seg.Color
        $g.FillRectangle($swatchBrush, $legendX, $legendY, 14, 14)
        $swatchBrush.Dispose()
        $pct = if ($total -gt 0) { ($seg.Value / $total) * 100 } else { 0 }
        $g.DrawString(("{0}: {1} ({2:N0}%)" -f $seg.Label, $seg.Value, $pct), $labelFont, $axisBrush, ($legendX + 20), ($legendY - 2))
        $legendY += 26
    }

    $bmp.Save($OutFile, [System.Drawing.Imaging.ImageFormat]::Png)
    $g.Dispose(); $bmp.Dispose()
    return $OutFile
}

function New-HBarChartImage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [array]$Bars,   # PSCustomObject with Label; Value; Color
        [string]$Title = '',
        [string]$ValueSuffix = '',
        [int]$Width = 640,
        [Parameter(Mandatory)] [string]$OutFile
    )
    if (-not $Script:ChartEngineOk) { return $null }
    if (-not $Bars -or $Bars.Count -eq 0) { return $null }

    $rowHeight = 30
    $topMargin = 45
    $bottomMargin = 20
    $Height = $topMargin + $bottomMargin + ($rowHeight * $Bars.Count)
    $labelWidth = 180
    $chartLeft = $labelWidth + 20
    $chartWidth = $Width - $chartLeft - 70

    $max = ($Bars | Measure-Object -Property Value -Maximum).Maximum
    if (-not $max -or $max -le 0) { $max = 1 }

    $bmp = New-Object System.Drawing.Bitmap $Width, $Height
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $g.Clear([System.Drawing.Color]::White)

    $titleFont = New-Object System.Drawing.Font 'Segoe UI', 13, ([System.Drawing.FontStyle]::Bold)
    $labelFont = New-Object System.Drawing.Font 'Segoe UI', 9
    $valueFont = New-Object System.Drawing.Font 'Segoe UI', 9, ([System.Drawing.FontStyle]::Bold)
    $axisBrush = New-Object System.Drawing.SolidBrush $Script:ChartColors.Axis

    if ($Title) { $g.DrawString($Title, $titleFont, $axisBrush, 10, 8) }

    $y = $topMargin
    foreach ($bar in $Bars) {
        $barLen = [Math]::Max(2, [Math]::Floor(($bar.Value / $max) * $chartWidth))
        $barBrush = New-Object System.Drawing.SolidBrush $bar.Color

        $labelSize = $g.MeasureString($bar.Label, $labelFont)
        $labelY = $y + ($rowHeight - $labelSize.Height) / 2
        $g.DrawString($bar.Label, $labelFont, $axisBrush, ($labelWidth - $labelSize.Width - 8), $labelY)

        $g.FillRectangle($barBrush, $chartLeft, ($y + 5), $barLen, ($rowHeight - 12))
        $barBrush.Dispose()

        $valText = "$($bar.Value)$ValueSuffix"
        $g.DrawString($valText, $valueFont, $axisBrush, ($chartLeft + $barLen + 8), ($y + 5))

        $y += $rowHeight
    }

    $bmp.Save($OutFile, [System.Drawing.Imaging.ImageFormat]::Png)
    $g.Dispose(); $bmp.Dispose()
    return $OutFile
}

function New-LineChartImage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [hashtable]$Series,   # SeriesName -> array of @{ X (datetime); Y (double) }
        [string]$Title = '',
        [string]$YAxisLabel = '',
        [int]$Width = 760,
        [int]$Height = 340,
        [Parameter(Mandatory)] [string]$OutFile
    )
    if (-not $Script:ChartEngineOk) { return $null }
    if (-not $Series -or $Series.Keys.Count -eq 0) { return $null }

    $leftMargin = 60
    $rightMargin = 170
    $topMargin = 45
    $bottomMargin = 40
    $plotWidth = $Width - $leftMargin - $rightMargin
    $plotHeight = $Height - $topMargin - $bottomMargin

    $allPoints = $Series.Values | ForEach-Object { $_ }
    $allY = $allPoints | ForEach-Object { $_.Y }
    $allX = $allPoints | ForEach-Object { $_.X }
    if (-not $allY -or $allY.Count -eq 0) { return $null }
    $minY = ($allY | Measure-Object -Minimum).Minimum
    $maxY = ($allY | Measure-Object -Maximum).Maximum
    if ($minY -eq $maxY) { $maxY = $minY + 1 }
    $minX = ($allX | Measure-Object -Minimum).Minimum
    $maxX = ($allX | Measure-Object -Maximum).Maximum
    $xSpan = ($maxX - $minX).TotalSeconds
    if ($xSpan -le 0) { $xSpan = 1 }

    $palette = @(
        [System.Drawing.Color]::FromArgb(37, 99, 235), [System.Drawing.Color]::FromArgb(220, 38, 38),
        [System.Drawing.Color]::FromArgb(16, 185, 129), [System.Drawing.Color]::FromArgb(245, 158, 11),
        [System.Drawing.Color]::FromArgb(139, 92, 246), [System.Drawing.Color]::FromArgb(6, 182, 212)
    )

    $bmp = New-Object System.Drawing.Bitmap $Width, $Height
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $g.Clear([System.Drawing.Color]::White)

    $titleFont = New-Object System.Drawing.Font 'Segoe UI', 13, ([System.Drawing.FontStyle]::Bold)
    $labelFont = New-Object System.Drawing.Font 'Segoe UI', 8
    $axisBrush = New-Object System.Drawing.SolidBrush $Script:ChartColors.Axis
    $gridPen   = New-Object System.Drawing.Pen $Script:ChartColors.Grid, 1

    if ($Title) { $g.DrawString($Title, $titleFont, $axisBrush, 10, 8) }

    # Gridlines + Y axis labels (5 bands)
    for ($i = 0; $i -le 4; $i++) {
        $yy = $topMargin + ($plotHeight * $i / 4)
        $g.DrawLine($gridPen, $leftMargin, $yy, ($leftMargin + $plotWidth), $yy)
        $val = $maxY - (($maxY - $minY) * $i / 4)
        $g.DrawString(("{0:N0}" -f $val), $labelFont, $axisBrush, 5, ($yy - 6))
    }

    $colorIndex = 0
    $legendY = $topMargin
    foreach ($seriesName in $Series.Keys) {
        $color = $palette[$colorIndex % $palette.Count]
        $pen = New-Object System.Drawing.Pen $color, 2
        $points = $Series[$seriesName] | Sort-Object X
        $prevPt = $null
        foreach ($pt in $points) {
            $px = $leftMargin + ((($pt.X - $minX).TotalSeconds / $xSpan) * $plotWidth)
            $py = $topMargin + $plotHeight - ((($pt.Y - $minY) / ($maxY - $minY)) * $plotHeight)
            if ($prevPt) { $g.DrawLine($pen, $prevPt.PX, $prevPt.PY, $px, $py) }
            $prevPt = [PSCustomObject]@{ PX = $px; PY = $py }
        }
        $pen.Dispose()

        $swatchBrush = New-Object System.Drawing.SolidBrush $color
        $g.FillRectangle($swatchBrush, ($Width - $rightMargin + 10), $legendY, 12, 12)
        $swatchBrush.Dispose()
        $g.DrawString($seriesName, $labelFont, $axisBrush, ($Width - $rightMargin + 26), ($legendY - 2))
        $legendY += 18
        $colorIndex++
    }

    if ($YAxisLabel) {
        $g.DrawString($YAxisLabel, $labelFont, $axisBrush, $leftMargin, ($Height - 16))
    }

    $bmp.Save($OutFile, [System.Drawing.Imaging.ImageFormat]::Png)
    $g.Dispose(); $bmp.Dispose()
    return $OutFile
}

#endregion

#region ------------------------------- CSV export -------------------------------

function Export-AuditCsv {
    [CmdletBinding()]
    param([Parameter(Mandatory)] [string]$OutputPath)

    $findingsPath = Join-Path $OutputPath 'Findings.csv'
    $Script:Findings | Sort-Object SqlInstance, Category, Severity | Export-Csv -Path $findingsPath -NoTypeInformation -Encoding UTF8
    Write-AuditLog "Findings CSV written: $findingsPath ($($Script:Findings.Count) rows)"

    if ($Script:CollectionIssues.Count -gt 0) {
        $issuesPath = Join-Path $OutputPath 'CollectionIssues.csv'
        $Script:CollectionIssues | Export-Csv -Path $issuesPath -NoTypeInformation -Encoding UTF8
        Write-AuditLog "Collection issues CSV written: $issuesPath ($($Script:CollectionIssues.Count) rows)"
    }

    if ($Script:PerfmonSamples.Count -gt 0) {
        $perfPath = Join-Path $OutputPath 'PerfmonSamples.csv'
        $Script:PerfmonSamples | Export-Csv -Path $perfPath -NoTypeInformation -Encoding UTF8
        Write-AuditLog "Perfmon samples CSV written: $perfPath ($($Script:PerfmonSamples.Count) rows)"
    }
}

#endregion

#region ------------------------------- HTML report -------------------------------

function Get-SeverityWeight {
    param([string]$Severity)
    switch ($Severity) {
        'High'          { return 100 }
        'Medium'        { return 10 }
        'Low'           { return 1 }
        'Informational' { return 0.1 }
        default         { return 0 }
    }
}

function Get-SeverityClass {
    param([string]$Severity)
    switch ($Severity) {
        'High'          { return 'sev-high' }
        'Medium'        { return 'sev-medium' }
        'Low'           { return 'sev-low' }
        'Informational' { return 'sev-info' }
        default         { return 'sev-ok' }
    }
}

function ConvertTo-HtmlEncoded {
    param([string]$Text)
    if ($null -eq $Text) { return '' }
    return [System.Net.WebUtility]::HtmlEncode($Text)
}

function Convert-ImageFileToBase64Img {
    param([string]$Path, [string]$AltText = 'chart')
    if (-not $Path -or -not (Test-Path $Path)) { return '' }
    $bytes = [System.IO.File]::ReadAllBytes($Path)
    $b64 = [Convert]::ToBase64String($bytes)
    return "<img src=`"data:image/png;base64,$b64`" alt=`"$AltText`" class=`"chart-img`" />"
}

$Script:HtmlCss = @'
:root{
  --high:#dc2626; --medium:#f59e0b; --low:#3b82f6; --info:#6b7280; --ok:#10b981;
  --navy:#0f172a; --panel:#f8fafc; --border:#e2e8f0; --text:#1e293b; --muted:#64748b;
}
*{box-sizing:border-box;}
body{font-family:'Segoe UI',Calibri,Arial,sans-serif;margin:0;background:#f1f5f9;color:var(--text);}
header.report-header{background:linear-gradient(135deg,var(--navy),#1e3a5f);color:#fff;padding:36px 48px;}
header.report-header h1{margin:0 0 6px 0;font-size:28px;}
header.report-header .sub{opacity:.85;font-size:14px;}
.container{max-width:1180px;margin:0 auto;padding:28px 24px 60px 24px;}
.card{background:#fff;border:1px solid var(--border);border-radius:10px;padding:22px 24px;margin-bottom:24px;box-shadow:0 1px 2px rgba(0,0,0,.04);}
.card h2{margin-top:0;font-size:19px;border-bottom:2px solid var(--panel);padding-bottom:10px;}
.card h3{font-size:15px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em;}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:20px;}
.grid-4{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:10px;}
.stat-box{background:var(--panel);border-radius:8px;padding:16px;text-align:center;border:1px solid var(--border);}
.stat-box .num{font-size:32px;font-weight:700;}
.stat-box .lbl{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.03em;}
.stat-box.high .num{color:var(--high);} .stat-box.medium .num{color:var(--medium);}
.stat-box.low .num{color:var(--low);} .stat-box.ok .num{color:var(--ok);}
.chart-img{max-width:100%;height:auto;display:block;margin:0 auto;}
table{border-collapse:collapse;width:100%;font-size:13.5px;}
th,td{padding:9px 10px;text-align:left;border-bottom:1px solid var(--border);vertical-align:top;}
th{background:var(--panel);font-size:11.5px;text-transform:uppercase;letter-spacing:.03em;color:var(--muted);position:sticky;top:0;}
tr:hover{background:#fafbfc;}
.badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600;color:#fff;white-space:nowrap;}
.sev-high{background:var(--high);} .sev-medium{background:var(--medium);} .sev-low{background:var(--low);}
.sev-info{background:var(--info);} .sev-ok{background:var(--ok);}
.filterbar{margin-bottom:16px;}
.filterbar button{border:1px solid var(--border);background:#fff;padding:8px 18px;border-radius:20px;cursor:pointer;font-size:13px;margin-right:8px;font-weight:600;color:var(--muted);}
.filterbar button.active{background:var(--navy);color:#fff;border-color:var(--navy);}
.cta-table td.rank{font-weight:700;font-size:16px;color:var(--navy);text-align:center;width:30px;}
.cta-table td.rec{color:#065f46;background:#ecfdf5;border-radius:6px;}
footer{text-align:center;color:var(--muted);font-size:12px;padding:24px;}
.tag{display:inline-block;background:var(--panel);border:1px solid var(--border);border-radius:5px;padding:2px 8px;font-size:11.5px;color:var(--muted);margin-right:4px;}
.muted{color:var(--muted);font-size:13px;}
.notice{background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:14px 18px;font-size:13.5px;color:#92400e;}
'@

$Script:HtmlJs = @'
function auditFilter(state){
  var rows = document.querySelectorAll('#findingsTable tbody tr');
  rows.forEach(function(r){
    var sev = r.getAttribute('data-sev');
    var show = (state === 'all') ||
               (state === 'attention' && sev !== 'OK') ||
               (state === 'ok' && sev === 'OK');
    r.style.display = show ? '' : 'none';
  });
  document.querySelectorAll('.filterbar button').forEach(function(b){ b.classList.remove('active'); });
  document.getElementById('btn-' + state).classList.add('active');
}
'@

function New-AuditHtmlReport {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$OutputPath,
        [Parameter(Mandatory)] [string]$CompanyName,
        [Parameter(Mandatory)] [string]$ReportTitle,
        [Parameter(Mandatory)] [string[]]$InstanceList,
        [Parameter(Mandatory)] [hashtable]$Config,
        [bool]$PerfmonRan,
        [string]$PerfmonMode,
        [Parameter(Mandatory)] [datetime]$ScriptStart
    )

    $chartsDir = Join-Path $OutputPath '_charts'
    New-Item -Path $chartsDir -ItemType Directory -Force | Out-Null

    $actionable = $Script:Findings | Where-Object { $_.Severity -ne 'OK' }
    $bySeverity = $actionable | Group-Object Severity
    $sevCounts = @{ High = 0; Medium = 0; Low = 0; Informational = 0 }
    foreach ($g in $bySeverity) { $sevCounts[$g.Name] = $g.Count }

    # --- Chart 1: severity donut ---
    $donutSegments = @(
        [PSCustomObject]@{ Label = 'High';          Value = $sevCounts['High'];          Color = $Script:ChartColors.High }
        [PSCustomObject]@{ Label = 'Medium';        Value = $sevCounts['Medium'];        Color = $Script:ChartColors.Medium }
        [PSCustomObject]@{ Label = 'Low';           Value = $sevCounts['Low'];           Color = $Script:ChartColors.Low }
        [PSCustomObject]@{ Label = 'Informational'; Value = $sevCounts['Informational']; Color = $Script:ChartColors.Informational }
    )
    $donutFile = Join-Path $chartsDir 'severity_donut.png'
    New-DonutChartImage -Segments $donutSegments -Title 'Findings by Severity' -OutFile $donutFile | Out-Null

    # --- Chart 2: findings by category ---
    $byCategory = $actionable | Group-Object Category | Sort-Object Count -Descending
    $catBars = $byCategory | ForEach-Object { [PSCustomObject]@{ Label = $_.Name; Value = $_.Count; Color = $Script:ChartColors.Low } }
    $categoryFile = Join-Path $chartsDir 'by_category.png'
    New-HBarChartImage -Bars $catBars -Title 'Findings by Category' -OutFile $categoryFile | Out-Null

    # --- Chart 3: top instances by finding count ---
    $byInstance = $actionable | Group-Object SqlInstance | Sort-Object Count -Descending | Select-Object -First 10
    $instBars = $byInstance | ForEach-Object { [PSCustomObject]@{ Label = $_.Name; Value = $_.Count; Color = $Script:ChartColors.Medium } }
    $instanceFile = Join-Path $chartsDir 'by_instance.png'
    New-HBarChartImage -Bars $instBars -Title 'Top 10 Instances by Finding Count' -OutFile $instanceFile | Out-Null

    # --- Chart 4: disk free % by volume (if captured) ---
    $diskFile = $null
    if ($Script:DiskSpaceRaw.Count -gt 0) {
        $diskBars = $Script:DiskSpaceRaw | Sort-Object PercentFree | ForEach-Object {
            $color = if ($_.PercentFree -le $Config.Thresholds['DiskFreePercentCrit']) { $Script:ChartColors.High }
                     elseif ($_.PercentFree -le $Config.Thresholds['DiskFreePercentWarn']) { $Script:ChartColors.Medium }
                     else { $Script:ChartColors.OK }
            [PSCustomObject]@{ Label = "$($_.SqlInstance) ($($_.Volume))"; Value = $_.PercentFree; Color = $color }
        } | Select-Object -First 20
        $diskFile = Join-Path $chartsDir 'disk_space.png'
        New-HBarChartImage -Bars $diskBars -Title 'Disk Free Space by Volume (%)' -ValueSuffix '%' -OutFile $diskFile | Out-Null
    }

    # --- Chart 5: perfmon trend (if captured with >1 sample and a meaningful counter) ---
    $perfFile = $null
    if ($PerfmonRan -and $Script:PerfmonSamples.Count -gt 0) {
        $bpsSamples = $Script:PerfmonSamples | Where-Object { $_.Counter -like 'Batch Requests*' }
        if ($bpsSamples) {
            $series = @{}
            foreach ($grp in ($bpsSamples | Group-Object SqlInstance)) {
                $series[$grp.Name] = $grp.Group | ForEach-Object { [PSCustomObject]@{ X = $_.Timestamp; Y = [double]$_.Value } }
            }
            $anyMultiSample = ($bpsSamples | Group-Object SqlInstance | Where-Object { $_.Count -gt 1 })
            if ($anyMultiSample) {
                $perfFile = Join-Path $chartsDir 'perfmon_trend.png'
                New-LineChartImage -Series $series -Title 'Batch Requests/sec during capture window' -YAxisLabel 'Batch Requests/sec' -OutFile $perfFile | Out-Null
            }
        }
    }

    # --- Call to action: top issues ranked by severity-weighted, estate-wide impact ---
    $ctaGroups = $actionable | Group-Object CheckCode | ForEach-Object {
        $first = $_.Group | Select-Object -First 1
        $affected = ($_.Group | Select-Object -ExpandProperty SqlInstance -Unique).Count
        [PSCustomObject]@{
            CheckCode      = $_.Name
            Category       = $first.Category
            CheckName      = $first.CheckName
            Severity       = ($_.Group | Sort-Object { Get-SeverityWeight $_.Severity } -Descending | Select-Object -First 1).Severity
            Count          = $_.Count
            AffectedCount  = $affected
            Recommendation = $first.Recommendation
        }
    } | Sort-Object { (Get-SeverityWeight $_.Severity) * $_.AffectedCount } -Descending | Select-Object -First 12

    $ctaRowsHtml = New-Object System.Collections.Generic.List[string]
    $rank = 1
    foreach ($c in $ctaGroups) {
        $sevClass = Get-SeverityClass $c.Severity
        $ctaRowsHtml.Add(@"
<tr>
  <td class="rank">$rank</td>
  <td><span class="badge $sevClass">$($c.Severity)</span></td>
  <td>$(ConvertTo-HtmlEncoded $c.Category)</td>
  <td><strong>$(ConvertTo-HtmlEncoded $c.CheckName)</strong></td>
  <td>$($c.Count) finding$(if ($c.Count -ne 1) { 's' }) across $($c.AffectedCount) instance$(if ($c.AffectedCount -ne 1) { 's' })</td>
  <td class="rec">$(ConvertTo-HtmlEncoded $c.Recommendation)</td>
</tr>
"@)
        $rank++
    }

    # --- Per-instance summary table ---
    $instanceRowsHtml = New-Object System.Collections.Generic.List[string]
    foreach ($inst in ($InstanceList | Sort-Object)) {
        $instFindings = $actionable | Where-Object { $_.SqlInstance -eq $inst }
        $h = ($instFindings | Where-Object Severity -eq 'High').Count
        $m = ($instFindings | Where-Object Severity -eq 'Medium').Count
        $l = ($instFindings | Where-Object Severity -eq 'Low').Count
        $i = ($instFindings | Where-Object Severity -eq 'Informational').Count
        $instanceRowsHtml.Add(@"
<tr>
  <td><strong>$(ConvertTo-HtmlEncoded $inst)</strong></td>
  <td><span class="badge sev-high">$h</span></td>
  <td><span class="badge sev-medium">$m</span></td>
  <td><span class="badge sev-low">$l</span></td>
  <td><span class="badge sev-info">$i</span></td>
</tr>
"@)
    }

    # --- Full findings table ---
    $findingRowsHtml = New-Object System.Collections.Generic.List[string]
    foreach ($f in ($Script:Findings | Sort-Object SqlInstance, Category, @{Expression={Get-SeverityWeight $_.Severity}; Descending=$true})) {
        $sevClass = Get-SeverityClass $f.Severity
        $dataSev = if ($f.Severity -eq 'OK') { 'OK' } else { 'ATTN' }
        $findingRowsHtml.Add(@"
<tr data-sev="$dataSev">
  <td>$(ConvertTo-HtmlEncoded $f.SqlInstance)</td>
  <td>$(ConvertTo-HtmlEncoded $f.Category)</td>
  <td><span class="badge $sevClass">$($f.Severity)</span></td>
  <td>$(ConvertTo-HtmlEncoded $f.CheckName)</td>
  <td>$(ConvertTo-HtmlEncoded $f.ObjectName)</td>
  <td>$(ConvertTo-HtmlEncoded $f.Detail)</td>
</tr>
"@)
    }

    # --- Collection issues (transparency) ---
    $issuesHtml = ''
    if ($Script:CollectionIssues.Count -gt 0) {
        $issueRows = ($Script:CollectionIssues | ForEach-Object {
            "<tr><td>$(ConvertTo-HtmlEncoded $_.SqlInstance)</td><td>$(ConvertTo-HtmlEncoded $_.CheckCode)</td><td>$(ConvertTo-HtmlEncoded $_.ErrorMessage)</td></tr>"
        }) -join "`n"
        $issuesHtml = @"
<div class="card">
  <h2>Collection Issues</h2>
  <p class="muted">$($Script:CollectionIssues.Count) check(s) could not complete on one or more instances (e.g. permissions, connectivity, or a dbatools property drift). These are logged, not silently ignored - the rest of the audit continued unaffected.</p>
  <table><thead><tr><th>Instance</th><th>Check</th><th>Error</th></tr></thead><tbody>$issueRows</tbody></table>
</div>
"@
    }

    $perfmonNoticeHtml = ''
    if ($PerfmonRan) {
        $perfmonNoticeHtml = @"
<div class="notice">Perfmon capture ran in <strong>$PerfmonMode</strong> mode across $($InstanceList.Count) instance(s). Counter thresholds flagged above are estate-audit-time signals, not a substitute for ongoing monitoring.</div>
"@
    }

    $perfSectionHtml = ''
    if ($perfFile) {
        $perfImgTag = Convert-ImageFileToBase64Img -Path $perfFile -AltText 'Perfmon trend'
        $perfSectionHtml = '<div class="card"><h2>Performance Capture</h2>' + $perfImgTag + '</div>'
    }

    $diskSectionInner = if ($diskFile) { Convert-ImageFileToBase64Img -Path $diskFile -AltText 'Disk free space by volume' } else { '<p class="muted">No disk space data captured.</p>' }

    $configSourceLabel = if ($Config.Source -eq 'ParametersOnly') { 'Script parameters (self-contained mode)' } else { $Config.Source }

    $totalActionable = $actionable.Count
    $totalOk = ($Script:Findings | Where-Object Severity -eq 'OK').Count
    $duration = (Get-Date) - $ScriptStart

    $html = @"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<title>$(ConvertTo-HtmlEncoded $ReportTitle) - $(ConvertTo-HtmlEncoded $CompanyName)</title>
<style>$($Script:HtmlCss)</style>
</head>
<body>
<header class="report-header">
  <h1>$(ConvertTo-HtmlEncoded $ReportTitle)</h1>
  <div class="sub">$(ConvertTo-HtmlEncoded $CompanyName) &middot; Generated $((Get-Date).ToString('dd MMMM yyyy, HH:mm')) &middot; $($InstanceList.Count) instance$(if ($InstanceList.Count -ne 1) { 's' }) audited &middot; Runtime: $([Math]::Round($duration.TotalMinutes,1)) min</div>
</header>
<div class="container">

  $perfmonNoticeHtml

  <div class="card">
    <h2>Executive Summary</h2>
    <div class="grid-4">
      <div class="stat-box high"><div class="num">$($sevCounts['High'])</div><div class="lbl">High</div></div>
      <div class="stat-box medium"><div class="num">$($sevCounts['Medium'])</div><div class="lbl">Medium</div></div>
      <div class="stat-box low"><div class="num">$($sevCounts['Low'])</div><div class="lbl">Low</div></div>
      <div class="stat-box ok"><div class="num">$totalOk</div><div class="lbl">Passing Checks</div></div>
    </div>
    <div class="grid-2">
      <div>$(Convert-ImageFileToBase64Img -Path $donutFile -AltText 'Findings by severity')</div>
      <div>$(Convert-ImageFileToBase64Img -Path $categoryFile -AltText 'Findings by category')</div>
    </div>
  </div>

  <div class="card">
    <h2>Call to Action &mdash; Prioritised Remediation Roadmap</h2>
    <p class="muted">Ranked by severity and estate-wide reach (how many instances are affected), not just raw count. Start at the top.</p>
    <table class="cta-table">
      <thead><tr><th>#</th><th>Severity</th><th>Category</th><th>Finding</th><th>Scope</th><th>Recommended Action</th></tr></thead>
      <tbody>
        $($ctaRowsHtml -join "`n")
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Estate View</h2>
    <div class="grid-2">
      <div>$(Convert-ImageFileToBase64Img -Path $instanceFile -AltText 'Top instances by finding count')</div>
      <div>$diskSectionInner</div>
    </div>
  </div>

  $perfSectionHtml

  <div class="card">
    <h2>Per-Instance Summary</h2>
    <table>
      <thead><tr><th>Instance</th><th>High</th><th>Medium</th><th>Low</th><th>Informational</th></tr></thead>
      <tbody>
        $($instanceRowsHtml -join "`n")
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Full Findings</h2>
    <div class="filterbar">
      <button id="btn-all" class="active" onclick="auditFilter('all')">All</button>
      <button id="btn-attention" onclick="auditFilter('attention')">Needs Attention</button>
      <button id="btn-ok" onclick="auditFilter('ok')">Healthy</button>
    </div>
    <table id="findingsTable">
      <thead><tr><th>Instance</th><th>Category</th><th>Severity</th><th>Check</th><th>Object</th><th>Detail</th></tr></thead>
      <tbody>
        $($findingRowsHtml -join "`n")
      </tbody>
    </table>
  </div>

  $issuesHtml

  <div class="card">
    <h3>Methodology</h3>
    <p class="muted">Read-only survey via the dbatools PowerShell module. Configuration source: <span class="tag">$configSourceLabel</span>. Backup thresholds: full &le; $($Config.Thresholds['FullBackupMaxDays']) day(s), log &le; $($Config.Thresholds['LogBackupMaxMinutes']) minute(s). CHECKDB &le; $($Config.Thresholds['CheckDbMaxDays']) day(s). Disk space flagged when free space falls below: $($Config.Thresholds['DiskFreePercentWarn'])% (warn) / $($Config.Thresholds['DiskFreePercentCrit'])% (critical). Query Store storage warned at &ge; $($Config.Thresholds['QueryStoreStoragePercentWarn'])% used.</p>
  </div>

</div>
<footer>Generated by Invoke-SqlEstateAudit.ps1 &middot; $(ConvertTo-HtmlEncoded $CompanyName)</footer>
<script>$($Script:HtmlJs)</script>
</body>
</html>
"@

    $htmlPath = Join-Path $OutputPath 'SqlEstateAudit.html'
    $html | Out-File -FilePath $htmlPath -Encoding UTF8
    Write-AuditLog "HTML report written: $htmlPath"

    return [PSCustomObject]@{
        HtmlPath      = $htmlPath
        DonutFile     = $donutFile
        CategoryFile  = $categoryFile
        InstanceFile  = $instanceFile
        DiskFile      = $diskFile
        PerfFile      = $perfFile
        CtaGroups     = $ctaGroups
        SevCounts     = $sevCounts
    }
}

#endregion

#region ------------------------------- Main orchestration -------------------------------

$Script:ChartEngineOk = Initialize-ChartEngine

try {
    $instanceList = Resolve-AuditInstanceList -SqlInstance $SqlInstance -InstancesFile $InstancesFile `
        -CmsInstance $CmsInstance -CmsGroup $CmsGroup -DiscoverInstances:$DiscoverInstances `
        -DiscoveryDomain $DiscoveryDomain -DiscoveryIpRange $DiscoveryIpRange -ExcludeInstance $ExcludeInstance

    $parameterDefaults = @{
        FullBackupMaxDays        = $FullBackupMaxDays
        LogBackupMaxMinutes      = $LogBackupMaxMinutes
        DiffBackupMaxHours       = $DiffBackupMaxHours
        CheckDbMaxDays           = $CheckDbMaxDays
        DiskFreePercentWarn      = $DiskFreePercentWarn
        DiskFreePercentCrit      = $DiskFreePercentCrit
        MaxCuBehind              = $MaxCuBehind
        RecentRestartHours       = $RecentRestartHours
        CostThresholdRecommended = $CostThresholdRecommended
        QueryStoreStoragePercentWarn = $QueryStoreStoragePercentWarn
        SsisLongRunningMinutes   = $SsisLongRunningMinutes
    }
    $config = Get-AuditConfig -ParameterDefaults $parameterDefaults -ConfigSqlInstance $ConfigSqlInstance -ConfigDatabase $ConfigDatabase
    $thresholds = $config.Thresholds

    Write-AuditLog -Level Section -Message "Surveying $($instanceList.Count) instance(s)"

    $connectedCount = 0
    foreach ($inst in $instanceList) {
        Write-AuditLog "--- $inst ---"
        try {
            $connParams = @{ SqlInstance = $inst; ErrorAction = 'Stop' }
            if ($SqlCredential) { $connParams['SqlCredential'] = $SqlCredential }
            $server = Connect-DbaInstance @connParams
        }
        catch {
            Add-CollectionIssue -SqlInstance $inst -CheckCode 'CONNECT' -ErrorMessage "Could not connect: $($_.Exception.Message)"
            continue
        }
        $connectedCount++

        Invoke-InstanceLevelChecks -SqlInstance $inst -ServerObject $server -Thresholds $thresholds -Config $config -SqlCredential $SqlCredential
        Invoke-DatabaseLevelChecks -SqlInstance $inst -ServerObject $server -Thresholds $thresholds -Config $config
        Invoke-AgentJobChecks -SqlInstance $inst -ServerObject $server -Config $config
        Invoke-HadrChecks -SqlInstance $inst -ServerObject $server -Config $config
        Invoke-StorageChecks -SqlInstance $inst -ServerObject $server -Thresholds $thresholds -Config $config

        if ($RunPerfmon) {
            Invoke-PerfmonCapture -SqlInstance $inst -ServerObject $server -Mode $PerfmonMode `
                -DurationSecondsOverride $PerfmonDurationSeconds -IntervalSecondsOverride $PerfmonIntervalSeconds `
                -PreferDbatoolsPerfmon:$PreferDbatoolsPerfmon -Config $config
        }

        try { $server.ConnectionContext.Disconnect() } catch { }
    }

    if ($connectedCount -eq 0) {
        Write-AuditLog -Level Error -Message "Could not connect to any instance in the list - nothing to report on."
        Write-AuditLog -Level Section -Message "Writing outputs"
        Export-AuditCsv -OutputPath $OutputPath   # CollectionIssues.csv still captures why
        Write-AuditLog "No instances were audited, so no HTML report was generated. See CollectionIssues.csv in: $OutputPath"
        return
    }

    Write-AuditLog -Level Section -Message "Writing outputs"
    Export-AuditCsv -OutputPath $OutputPath

    $htmlResult = New-AuditHtmlReport -OutputPath $OutputPath -CompanyName $CompanyName -ReportTitle $ReportTitle `
        -InstanceList $instanceList -Config $config -PerfmonRan:$RunPerfmon -PerfmonMode $PerfmonMode -ScriptStart $scriptStart

    $duration = (Get-Date) - $scriptStart
    $actionableCount = ($Script:Findings | Where-Object { $_.Severity -ne 'OK' }).Count
    Write-AuditLog -Level Section -Message "Audit complete"
    Write-AuditLog "Instances audited: $connectedCount / $($instanceList.Count)"
    Write-AuditLog "Findings requiring attention: $actionableCount   |   Collection issues: $($Script:CollectionIssues.Count)"
    Write-AuditLog "Runtime: $([Math]::Round($duration.TotalMinutes, 1)) minute(s)"
    Write-AuditLog "Output folder: $OutputPath"

    if ($OpenWhenDone -and $htmlResult.HtmlPath -and (Test-Path $htmlResult.HtmlPath)) {
        Invoke-Item -Path $htmlResult.HtmlPath
    }
}
catch {
    Write-AuditLog -Level Error -Message "Audit run failed: $($_.Exception.Message)"
    throw
}
finally {
    Stop-Transcript | Out-Null
}

#endregion
