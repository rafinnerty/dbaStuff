#requires -Version 5.1
<#
.SYNOPSIS
    Shrink rowstore tables, heaps, and indexes in a single database. 
    Moves objects onto a target filegroup - including LOB/BLOB (TEXTIMAGE / LOB_DATA) data.
    Preserves constraints (PK / UNIQUE / FK / CHECK / DEFAULT).

.DESCRIPTION
    For each user table the script generates (and optionally executes) the correct DDL:

      * Clustered tables (no LOB)  : CREATE ... CLUSTERED INDEX ... WITH (DROP_EXISTING = ON)
                                     ON [TargetFG]. Moves IN_ROW_DATA + ROW_OVERFLOW_DATA.
                                     PK/UNIQUE constraints are preserved because the
                                     constraint-backing index is rebuilt in place, not dropped.

      * Clustered tables (LOB)     : Uses the partition-scheme trick (Jitbit, "Update from 2021").
                                     Recreate the clustered index onto a partition scheme whose
                                     partitions ALL map to [TargetFG] - this rewrites every
                                     allocation unit, including LOB_DATA, onto the target FG -
                                     then recreate it again onto the plain filegroup to
                                     de-partition. Function/scheme are dropped afterwards.

      * Heaps (no LOB)             : CREATE a temporary CLUSTERED INDEX on [TargetFG] (moves the
                                     data), then DROP it WITH (MOVE TO [TargetFG]) to return a
                                     heap that now lives on the target FG.

      * Heaps (LOB)                : Same temp-clustered-index approach, but the temp index is
                                     built onto the partition scheme so LOB_DATA is relocated too,
                                     then dropped WITH (MOVE TO [TargetFG]).

      * Nonclustered indexes       : CREATE ... NONCLUSTERED INDEX ... WITH (DROP_EXISTING = ON)
                                     ON [TargetFG]. Filtered indexes, included columns, uniqueness
                                     and key-column sort direction are all reproduced.

    Every CREATE INDEX uses SORT_IN_TEMPDB = ON. Compression is configurable (keep as-is, or
    force PAGE / ROW / NONE). ONLINE is optional (Enterprise / Developer only).

    By DESIGN the default is "script only" - nothing touches the database unless you pass
    -Execute. Always review the generated T-SQL, take a backup, and run in a maintenance
    window with adequate free space (roughly the size of your largest object) and log headroom.

.PARAMETER SqlInstance
    Target SQL Server instance, e.g. 'SQL01' or 'SQL01\PROD,1433'.

.PARAMETER Database
    The database whose objects will be moved.

.PARAMETER TargetFileGroup
    Destination filegroup. Must already exist, contain at least one file, and not be read-only.

.PARAMETER Compression
    AsIs (default) keeps each object's current compression; Page / Row / None force that setting.

.PARAMETER Online
    Emit WITH (ONLINE = ON) on index operations (Enterprise / Developer edition only).

.PARAMETER MaxDop
    Optional MAXDOP hint applied to index builds.

.PARAMETER Schema
    Optional schema include-filter (one or more schema names). Default: all schemas.

.PARAMETER ExcludeTable
    Optional list of 'schema.table' names to skip.

.PARAMETER OutputScriptPath
    Writes the generated T-SQL to this .sql file (always written when supplied).

.PARAMETER Execute
    Actually run the generated DDL against the database. Omit for a dry run / script generation.

.PARAMETER StatementTimeout
    Per-statement timeout in seconds when executing. 0 = unlimited (default).

.EXAMPLE
    # Dry run - generate a reviewable script, change nothing:
    .\shrinkDbObjects.ps1 -SqlInstance SQL01 -Database Sales `
        -TargetFileGroup DATA_FG2 -OutputScriptPath .\move_sales.sql

.EXAMPLE
    # Execute, forcing PAGE compression, online:
    .\shrinkDbObjects.ps1 -SqlInstance SQL01 -Database Sales `
        -TargetFileGroup DATA_FG2 -Compression Page -Online -Execute

.NOTES
    Columnstore indexes (clustered and nonclustered) are relocated only when -IncludeColumnstore
    is supplied; otherwise they are skipped with a warning. Rowstore -Compression never applies to
    columnstore (its compression is COLUMNSTORE / COLUMNSTORE_ARCHIVE and is preserved as-is); an
    ORDERED clustered columnstore's ORDER clause is not reconstructed, so verify ordering if used.

    Skipped (with a warning) by design: partitioned tables/indexes (moving everything to ONE
    filegroup conflicts with an existing partition scheme), XML / spatial / full-text / in-memory /
    FILESTREAM structures, and nonclustered indexes whose LOB *included* columns would need their
    own partition-trick pass. These need bespoke handling.

    LOB-move technique credit: Alex Yumashev / Jitbit,
    "Moving SQL table text/image to a new filegroup" (Update from 2021).
#>
[CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
param(
    [Parameter(Mandatory)][string]   $SqlInstance,
    [Parameter(Mandatory)][string]   $Database,
    [Parameter(Mandatory)][string]   $TargetFileGroup,
    [ValidateSet('AsIs', 'Page', 'Row', 'None')][string] $Compression = 'AsIs',
    [switch]   $Online,
    [int]      $MaxDop,
    [string[]] $Schema,
    [string[]] $ExcludeTable,
    [string]   $OutputScriptPath,
    [switch]   $Execute,
    [int]      $StatementTimeout = 0,
    # When the target filegroup does not exist (or exists but has no file) it is created.
    # These control the data file that gets added.
    [string]   $NewFilePath,            # full physical .ndf path; default derives from the DB's data dir
    [int]      $NewFileSizeMB = 1024,
    [int]      $NewFileGrowthMB = 256,
    [switch]   $ReportOnly,             # classify + size every object; emit no DDL
    [string]   $ReportCsvPath,          # optional: export the per-object report to CSV
    [switch]   $IncludeColumnstore,
    [switch]   $LogToScreen             # also relocate clustered/nonclustered columnstore indexes
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# --------------------------------------------------------------------------------------------
# Query plumbing - prefer dbatools, then SqlServer module, then raw SqlClient.
# --------------------------------------------------------------------------------------------
$script:QueryEngine = $null
if (Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue) { $script:QueryEngine = 'dbatools' }
elseif (Get-Command Invoke-Sqlcmd -ErrorAction SilentlyContinue) { $script:QueryEngine = 'sqlserver' }
else { $script:QueryEngine = 'sqlclient' }

function Invoke-Query {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Sql,
        [switch] $NonQuery
    )
    switch ($script:QueryEngine) {
        'dbatools' {
            $p = @{ SqlInstance = $SqlInstance; Database = $Database; Query = $Sql
                    EnableException = $true }
            if ($StatementTimeout -gt 0) { $p['QueryTimeout'] = $StatementTimeout }
            if ($NonQuery) { Invoke-DbaQuery @p -As SingleValue | Out-Null }
            else { Invoke-DbaQuery @p }
        }
        'sqlserver' {
            $p = @{ ServerInstance = $SqlInstance; Database = $Database; Query = $Sql
                    QueryTimeout = ($(if ($StatementTimeout -gt 0) { $StatementTimeout } else { 65535 }))
                    ErrorAction = 'Stop' }
            if ($NonQuery) { Invoke-Sqlcmd @p | Out-Null } else { Invoke-Sqlcmd @p }
        }
        'sqlclient' {
            $cs = "Server=$SqlInstance;Database=$Database;Integrated Security=SSPI;TrustServerCertificate=True"
            $cn = [System.Data.SqlClient.SqlConnection]::new($cs)
            try {
                $cn.Open()
                $cmd = $cn.CreateCommand()
                $cmd.CommandText = $Sql
                $cmd.CommandTimeout = $StatementTimeout
                if ($NonQuery) { $cmd.ExecuteNonQuery() | Out-Null }
                else {
                    $da = [System.Data.SqlClient.SqlDataAdapter]::new($cmd)
                    $dt = [System.Data.DataTable]::new()
                    $da.Fill($dt) | Out-Null
                    , $dt
                }
            }
            finally { $cn.Dispose() }
        }
    }
}

# --------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------
function Quote-Name { param([string]$Name) "[" + ($Name -replace ']', ']]') + "]" }

# A type-valid (but value-irrelevant) boundary literal. Both partitions map to the SAME
# filegroup, so the actual boundary value never matters - it only has to be legal for the type.
function Get-BoundaryLiteral {
    param([string]$TypeName)
    switch -Regex ($TypeName.ToLowerInvariant()) {
        '^(tinyint|smallint|int|bigint|bit|decimal|numeric|money|smallmoney|float|real)$' { '0' ; break }
        '^(date|datetime|datetime2|smalldatetime|datetimeoffset)$' { "'2000-01-01'" ; break }
        '^time$' { "'00:00:00'" ; break }
        '^uniqueidentifier$' { "'00000000-0000-0000-0000-000000000000'" ; break }
        '^(binary|varbinary)$' { '0x00' ; break }
        '^(char|varchar)$' { "''" ; break }
        '^(nchar|nvarchar)$' { "N''" ; break }
        default { $null }
    }
}

# Resolve the compression clause string for an object given the -Compression mode.
function Resolve-Compression {
    param([string]$CurrentDesc)
    switch ($Compression) {
        'AsIs' { if ([string]::IsNullOrWhiteSpace($CurrentDesc)) { 'NONE' } else { $CurrentDesc.ToUpperInvariant() } }
        'Page' { 'PAGE' }
        'Row'  { 'ROW' }
        'None' { 'NONE' }
    }
}

# Build the "WITH (...)" option list for a rowstore CREATE INDEX.
# SuppressOnline forces ONLINE off (rowstore index ops can't run ONLINE on a table that has a
# columnstore index).
function Get-WithOptions {
    param([switch]$DropExisting, [string]$Compression, [switch]$NoSort, [switch]$SuppressOnline)
    $opts = @()
    if ($DropExisting) { $opts += 'DROP_EXISTING = ON' }
    if (-not $NoSort)  { $opts += 'SORT_IN_TEMPDB = ON' }
    if ($Online -and -not $SuppressOnline) { $opts += 'ONLINE = ON' }
    if ($MaxDop)       { $opts += "MAXDOP = $MaxDop" }
    if ($Compression)  { $opts += "DATA_COMPRESSION = $Compression" }
    'WITH (' + ($opts -join ', ') + ')'
}

# Build the "WITH (...)" option list for a columnstore index. No SORT_IN_TEMPDB, and the only
# valid DATA_COMPRESSION values are COLUMNSTORE / COLUMNSTORE_ARCHIVE (preserved from current).
function Get-ColumnstoreWithOptions {
    param([string]$CurrentCompression)
    $opts = @('DROP_EXISTING = ON')
    if ($Online) { $opts += 'ONLINE = ON' }
    if ($MaxDop) { $opts += "MAXDOP = $MaxDop" }
    if ([string]$CurrentCompression -eq 'COLUMNSTORE_ARCHIVE') { $opts += 'DATA_COMPRESSION = COLUMNSTORE_ARCHIVE' }
    'WITH (' + ($opts -join ', ') + ')'
}

# Normalise a query result (DataTable vs PSObject collection vs single object) to an array of rows.
function ConvertTo-Rows {
    param($Result)
    if ($null -eq $Result) { return @() }
    if ($Result -is [System.Data.DataTable]) { return @($Result.Rows) }
    return @($Result)
}

# --------------------------------------------------------------------------------------------
# Pre-flight: ensure the target filegroup exists and has a data file (create if missing).
# Any creation DDL is collected in $prepBatches and runs first (in the script and on -Execute).
# --------------------------------------------------------------------------------------------
$tgtFG       = Quote-Name $TargetFileGroup
$prepBatches = [System.Collections.Generic.List[string]]::new()

Write-Verbose "Checking target filegroup [$TargetFileGroup] in [$Database] on [$SqlInstance]..."
$fgCheckSql = @"
SELECT fg.name AS FgName, fg.is_read_only AS IsReadOnly,
       (SELECT COUNT(*) FROM sys.database_files df WHERE df.data_space_id = fg.data_space_id) AS FileCount
FROM sys.filegroups fg
WHERE fg.name = N'$($TargetFileGroup -replace "'","''")';
"@
$fgRows  = @(ConvertTo-Rows (Invoke-Query -Sql $fgCheckSql))
$needFG  = ($fgRows.Count -eq 0)
$needFile = $false

if ($needFG) {
    Write-Verbose "Filegroup [$TargetFileGroup] not found - it will be created."
    $prepBatches.Add("ALTER DATABASE $(Quote-Name $Database) ADD FILEGROUP $tgtFG;")
    $needFile = $true
}
else {
    $fgRow = $fgRows[0]
    if ([bool]$fgRow.IsReadOnly) { throw "Target filegroup [$TargetFileGroup] is read-only." }
    if ([int]$fgRow.FileCount -lt 1) {
        Write-Verbose "Filegroup [$TargetFileGroup] exists but has no file - a file will be added."
        $needFile = $true
    }
}

if ($needFile) {
    # Resolve the physical path for the new data file.
    if ([string]::IsNullOrWhiteSpace($NewFilePath)) {
        $dirRows = @(ConvertTo-Rows (Invoke-Query -Sql "SELECT TOP (1) physical_name AS PhysicalName FROM sys.database_files WHERE type = 0 ORDER BY file_id;"))
        if ($dirRows.Count -eq 0) { throw "Could not determine a default data-file directory; pass -NewFilePath explicitly." }
        $dataDir   = Split-Path -Path ([string]$dirRows[0].PhysicalName) -Parent
        $logical   = "$($TargetFileGroup)_data"
        $physical  = Join-Path $dataDir "$logical.ndf"
    }
    else {
        $physical  = $NewFilePath
        $logical   = [System.IO.Path]::GetFileNameWithoutExtension($NewFilePath)
    }
    $physEsc = $physical -replace "'", "''"
    $logEsc  = $logical  -replace "'", "''"
    $addFileSql = @"
ALTER DATABASE $(Quote-Name $Database) ADD FILE
(
    NAME = N'$logEsc',
    FILENAME = N'$physEsc',
    SIZE = $($NewFileSizeMB)MB,
    FILEGROWTH = $($NewFileGrowthMB)MB
) TO FILEGROUP $tgtFG;
"@
    $prepBatches.Add($addFileSql)
    Write-Verbose "Will add file '$logical' ($physical, ${NewFileSizeMB}MB, grow ${NewFileGrowthMB}MB) to [$TargetFileGroup]."
}

# --------------------------------------------------------------------------------------------
# Gather metadata
# --------------------------------------------------------------------------------------------
# Tables: clustered-index info, LOB presence, partition state, current heap compression.
$tableSql = @"
SELECT
    t.object_id                                           AS ObjectId,
    s.name                                                AS SchemaName,
    t.name                                                AS TableName,
    ISNULL(ci.index_id, 0)                                AS ClusteredIndexId,   -- 0 = heap
    ISNULL(ci.type, 0)                                    AS ClusteredIndexType, -- 1 = rowstore CI, 5 = clustered columnstore
    ci.name                                               AS ClusteredIndexName,
    ISNULL(ci.is_unique, 0)                               AS ClusteredIsUnique,
    ds.type                                               AS DataSpaceType,       -- 'FG' or 'PS'
    CASE WHEN EXISTS (
        -- Known LOB types by name (catches empty tables with no allocated LOB unit yet).
        -- geography/geometry are CLR types stored off-row as LOB_DATA.
        SELECT 1 FROM sys.columns c
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        WHERE c.object_id = t.object_id
          AND ( ty.name IN (N'text', N'ntext', N'image', N'xml', N'geography', N'geometry')
             OR (c.max_length = -1 AND ty.name IN (N'varchar', N'nvarchar', N'varbinary')) )
    ) OR EXISTS (
        -- Anything actually allocated off-row: a LOB_DATA unit on the base rowstore
        -- structure (heap = index_id 0, clustered = 1). Type-agnostic, so it also catches
        -- arbitrary CLR UDTs. index_id IN (0,1) excludes nonclustered columnstore LOB.
        SELECT 1 FROM sys.partitions p
        JOIN sys.allocation_units au ON au.container_id = p.partition_id AND au.type = 2
        WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)
    ) THEN 1 ELSE 0 END                                   AS HasLob,
    ISNULL((SELECT TOP (1) p.data_compression_desc
            FROM sys.partitions p
            WHERE p.object_id = t.object_id AND p.index_id IN (0,1)), 'NONE') AS CurrentCompression,
    t.is_memory_optimized                                 AS IsMemoryOptimized,
    CASE WHEN EXISTS (SELECT 1 FROM sys.columns c
                      WHERE c.object_id = t.object_id AND c.is_filestream = 1)
         THEN 1 ELSE 0 END                                AS HasFilestream
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
LEFT JOIN sys.indexes ci ON ci.object_id = t.object_id AND ci.index_id = 1
LEFT JOIN sys.data_spaces ds ON ds.data_space_id = ISNULL(ci.data_space_id,
            (SELECT TOP(1) h.data_space_id FROM sys.indexes h
             WHERE h.object_id = t.object_id AND h.index_id = 0))
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name;
"@

# All rowstore indexes we will (or might) move - clustered + nonclustered only.
$indexSql = @"
SELECT
    i.object_id                       AS ObjectId,
    i.index_id                        AS IndexId,
    i.name                            AS IndexName,
    i.type                            AS IndexType,          -- 1 = clustered, 2 = nonclustered, 6 = NC columnstore
    i.is_unique                       AS IsUnique,
    i.is_primary_key                  AS IsPrimaryKey,
    i.is_unique_constraint            AS IsUniqueConstraint,
    i.has_filter                      AS HasFilter,
    i.filter_definition               AS FilterDefinition,
    ds.type                           AS DataSpaceType,      -- 'FG' or 'PS'
    ISNULL((SELECT TOP (1) p.data_compression_desc
            FROM sys.partitions p
            WHERE p.object_id = i.object_id AND p.index_id = i.index_id), 'NONE') AS CurrentCompression
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id AND t.is_ms_shipped = 0
JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
WHERE i.type IN (1, 2, 6)   -- 6 = nonclustered columnstore (processed only with -IncludeColumnstore)
ORDER BY i.object_id, i.index_id;
"@

# Index key/include columns (ordered) for full index reproduction under DROP_EXISTING.
$indexColSql = @"
SELECT
    ic.object_id        AS ObjectId,
    ic.index_id         AS IndexId,
    ic.key_ordinal      AS KeyOrdinal,
    ic.is_included_column AS IsIncluded,
    ic.is_descending_key  AS IsDescending,
    ic.column_id        AS ColumnId,
    c.name              AS ColumnName
FROM sys.index_columns ic
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE ic.index_id > 0   -- index_id 1 = clustered, 2..N = each nonclustered (NOT just 2)
ORDER BY ic.object_id, ic.index_id, ic.is_included_column, ic.key_ordinal;
"@

# Columns - used to choose a partition-/index-legal column for heap temp indexes.
$colSql = @"
SELECT
    c.object_id     AS ObjectId,
    c.column_id     AS ColumnId,
    c.name          AS ColumnName,
    ty.name         AS TypeName,
    c.max_length    AS MaxLength,
    c.is_computed   AS IsComputed,
    c.is_nullable   AS IsNullable
FROM sys.columns c
JOIN sys.types ty ON ty.user_type_id = c.user_type_id
JOIN sys.tables t ON t.object_id = c.object_id AND t.is_ms_shipped = 0
ORDER BY c.object_id, c.column_id;
"@

$tables    = @(ConvertTo-Rows (Invoke-Query -Sql $tableSql))
$indexes   = @(ConvertTo-Rows (Invoke-Query -Sql $indexSql))
$indexCols = @(ConvertTo-Rows (Invoke-Query -Sql $indexColSql))
$columns   = @(ConvertTo-Rows (Invoke-Query -Sql $colSql))

# Per-table size by allocation-unit type (MB). LOB_DATA (type 2) joins on partition_id;
# IN_ROW (1) and ROW_OVERFLOW (3) join on hobt_id.
$sizeSql = @"
SELECT
    p.object_id AS ObjectId,
    CAST(SUM(CASE WHEN au.type IN (1,3) THEN au.total_pages ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18,2)) AS DataMB,
    CAST(SUM(CASE WHEN au.type = 2     THEN au.total_pages ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18,2)) AS LobMB,
    CAST(SUM(au.total_pages) * 8.0 / 1024 AS DECIMAL(18,2)) AS TotalMB
FROM sys.partitions p
JOIN sys.allocation_units au
     ON (au.type IN (1,3) AND au.container_id = p.hobt_id)
     OR (au.type = 2      AND au.container_id = p.partition_id)
JOIN sys.tables t ON t.object_id = p.object_id AND t.is_ms_shipped = 0
GROUP BY p.object_id;
"@
$sizes = @(ConvertTo-Rows (Invoke-Query -Sql $sizeSql))
$sizeByObject = @{}
foreach ($r in $sizes) { $sizeByObject[[int]$r.ObjectId] = $r }

# Index types this script does NOT relocate (XML=3, spatial=4, columnstore=5/6, in-mem hash=7),
# plus full-text indexes. Detected here so they're reported rather than silently left behind.
# Type 5 (clustered columnstore) is always handled in the main loop. Type 6 (nonclustered
# columnstore) is moved in the NC loop when -IncludeColumnstore, so only flag it otherwise.
$csExclude = if ($IncludeColumnstore) { '0, 1, 2, 5, 6, 7' } else { '0, 1, 2, 5, 7' }
$specialSql = @"
SELECT s.name AS SchemaName, t.name AS TableName, t.object_id AS ObjectId,
       i.name AS IndexName, i.type_desc AS TypeDesc
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id AND t.is_ms_shipped = 0
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE i.type NOT IN ($csExclude)
UNION ALL
SELECT s.name, t.name, t.object_id, N'(full-text index)', N'FULLTEXT'
FROM sys.fulltext_indexes fi
JOIN sys.tables t ON t.object_id = fi.object_id AND t.is_ms_shipped = 0
JOIN sys.schemas s ON s.schema_id = t.schema_id;
"@
$specialIdx = @(ConvertTo-Rows (Invoke-Query -Sql $specialSql))

function Get-Size {
    param([int]$ObjectId, [string]$Which)
    if ($sizeByObject.ContainsKey($ObjectId)) { return [decimal]$sizeByObject[$ObjectId].$Which }
    return [decimal]0
}

# Per-object report rows (populated during classification, used by -ReportOnly).
$report = [System.Collections.Generic.List[object]]::new()
function Add-Report {
    param([string]$Schema, [string]$Table, [string]$Structure, [bool]$Lob, [string]$Path, [int]$ObjectId)
    $ncCount = 0
    if ($idxByObject.ContainsKey($ObjectId)) {
        $ncCount = @($idxByObject[$ObjectId] | Where-Object { [int]$_.IndexType -eq 2 }).Count
    }
    $report.Add([pscustomobject]@{
        Schema    = $Schema
        Table     = $Table
        Structure = $Structure
        LOB       = $(if ($Lob) { 'Y' } else { '' })
        NCIdx     = $ncCount
        DataMB    = Get-Size -ObjectId $ObjectId -Which 'DataMB'
        LobMB     = Get-Size -ObjectId $ObjectId -Which 'LobMB'
        TotalMB   = Get-Size -ObjectId $ObjectId -Which 'TotalMB'
        Path      = $Path
    })
}

# Index columns by object_id\index_id
$colsByIndex = @{}
foreach ($r in $indexCols) {
    $key = "$($r.ObjectId)`:$($r.IndexId)"
    if (-not $colsByIndex.ContainsKey($key)) { $colsByIndex[$key] = [System.Collections.Generic.List[object]]::new() }
    $colsByIndex[$key].Add($r)
}
# Columns by object_id
$colsByObject = @{}
foreach ($r in $columns) {
    if (-not $colsByObject.ContainsKey([int]$r.ObjectId)) { $colsByObject[[int]$r.ObjectId] = [System.Collections.Generic.List[object]]::new() }
    $colsByObject[[int]$r.ObjectId].Add($r)
}
# Indexes by object_id
$idxByObject = @{}
foreach ($r in $indexes) {
    if (-not $idxByObject.ContainsKey([int]$r.ObjectId)) { $idxByObject[[int]$r.ObjectId] = [System.Collections.Generic.List[object]]::new() }
    $idxByObject[[int]$r.ObjectId].Add($r)
}

$lobUnsafeTypes = @('text', 'ntext', 'image', 'xml', 'timestamp', 'rowversion',
                    'geography', 'geometry', 'hierarchyid', 'sql_variant')

function Test-PartitionLegal {
    param($Col)
    $tn = ([string]$Col.TypeName).ToLowerInvariant()
    if ($lobUnsafeTypes -contains $tn) { return $false }
    if (([string]$Col.TypeName) -in @('varchar','nvarchar','varbinary') -and [int]$Col.MaxLength -eq -1) { return $false }
    if ([bool]$Col.IsComputed) { return $false }
    return $true
}

function Get-KeyColumnList {
    param($ObjectId, $IndexId)
    $key = "$ObjectId`:$IndexId"
    if (-not $colsByIndex.ContainsKey($key)) { return $null }
    $keyCols = $colsByIndex[$key] | Where-Object { -not [bool]$_.IsIncluded -and [int]$_.KeyOrdinal -gt 0 } |
               Sort-Object { [int]$_.KeyOrdinal }
    ($keyCols | ForEach-Object {
        (Quote-Name $_.ColumnName) + ($(if ([bool]$_.IsDescending) { ' DESC' } else { ' ASC' }))
    }) -join ', '
}

function Get-IncludeColumnList {
    param($ObjectId, $IndexId)
    $key = "$ObjectId`:$IndexId"
    if (-not $colsByIndex.ContainsKey($key)) { return $null }
    $inc = $colsByIndex[$key] | Where-Object { [bool]$_.IsIncluded } | Sort-Object { [int]$_.KeyOrdinal }, { [string]$_.ColumnName }
    if (-not $inc) { return $null }
    ($inc | ForEach-Object { Quote-Name $_.ColumnName }) -join ', '
}

function Get-FirstKeyColumnName {
    param($ObjectId, $IndexId)
    $key = "$ObjectId`:$IndexId"
    $first = $colsByIndex[$key] | Where-Object { -not [bool]$_.IsIncluded -and [int]$_.KeyOrdinal -eq 1 } | Select-Object -First 1
    if ($first) { [string]$first.ColumnName } else { $null }
}

# All participating columns of a (nonclustered) columnstore index, ordered by column_id.
# Columnstore columns have no key order, so every row is taken regardless of key_ordinal.
function Get-ColumnstoreColumnList {
    param($ObjectId, $IndexId)
    $key = "$ObjectId`:$IndexId"
    if (-not $colsByIndex.ContainsKey($key)) { return $null }
    $cols = $colsByIndex[$key] | Sort-Object { [int]$_.ColumnId }
    if (-not $cols) { return $null }
    ($cols | ForEach-Object { Quote-Name $_.ColumnName }) -join ', '
}

function Test-IncludeHasLob {
    param($ObjectId, $IndexId)
    $key = "$ObjectId`:$IndexId"
    if (-not $colsByIndex.ContainsKey($key)) { return $false }
    $incNames = $colsByIndex[$key] | Where-Object { [bool]$_.IsIncluded } | ForEach-Object { [string]$_.ColumnName }
    if (-not $incNames) { return $false }
    $objCols = $colsByObject[[int]$ObjectId]
    foreach ($n in $incNames) {
        $c = $objCols | Where-Object { [string]$_.ColumnName -eq $n } | Select-Object -First 1
        if ($c) {
            $tn = ([string]$c.TypeName).ToLowerInvariant()
            if ($tn -in @('text','ntext','image','xml')) { return $true }
            if (([string]$c.TypeName) -in @('varchar','nvarchar','varbinary') -and [int]$c.MaxLength -eq -1) { return $true }
        }
    }
    return $false
}

# --------------------------------------------------------------------------------------------
# Emit / collect batches
# --------------------------------------------------------------------------------------------
$batches   = [System.Collections.Generic.List[string]]::new()
$skips     = [System.Collections.Generic.List[string]]::new()
function Add-Batch { param([string]$Sql) $batches.Add($Sql.Trim()) }

foreach ($t in $tables) {
    $schemaName = [string]$t.SchemaName
    $tableName  = [string]$t.TableName
    $full       = "$schemaName.$tableName"
    $qFull      = (Quote-Name $schemaName) + '.' + (Quote-Name $tableName)
    $objId      = [int]$t.ObjectId

    if ($LogToScreen) {
        Write-Host "Evaluating object: $full ..." -ForegroundColor Cyan
    }

    if ($Schema -and ($Schema -notcontains $schemaName)) { continue }
    if ($ExcludeTable -and ($ExcludeTable -contains $full)) { continue }

    # ---- skip conditions ------------------------------------------------------------------
    if ([bool]$t.IsMemoryOptimized) {
        $skips.Add("$full : memory-optimized table - filegroup move not applicable.")
        Add-Report -Schema $schemaName -Table $tableName -Structure 'Mem-opt' -Lob ([int]$t.HasLob -eq 1) -Path 'SKIPPED: memory-optimized' -ObjectId $objId
        continue
    }
    if ([bool]$t.HasFilestream) {
        $skips.Add("$full : has FILESTREAM column(s) - needs FILESTREAM filegroup handling.")
        Add-Report -Schema $schemaName -Table $tableName -Structure 'Table' -Lob ([int]$t.HasLob -eq 1) -Path 'SKIPPED: FILESTREAM' -ObjectId $objId
        continue
    }
    if ([string]$t.DataSpaceType -eq 'PS') {
        $skips.Add("$full : already partitioned - moving to a single filegroup conflicts with the partition scheme.")
        Add-Report -Schema $schemaName -Table $tableName -Structure 'Partitioned' -Lob ([int]$t.HasLob -eq 1) -Path 'SKIPPED: already partitioned' -ObjectId $objId
        continue
    }
    # Clustered COLUMNSTORE index also occupies index_id 1 but is type 5 (no key columns).
    # Without -IncludeColumnstore it is skipped (it must NOT be rebuilt as a rowstore index).
    $isCCI = ([int]$t.ClusteredIndexId -eq 1 -and [int]$t.ClusteredIndexType -ne 1)
    if ($isCCI -and -not $IncludeColumnstore) {
        $skips.Add("$full : clustered columnstore index - not relocated (re-run with -IncludeColumnstore, or CREATE CLUSTERED COLUMNSTORE INDEX ... WITH (DROP_EXISTING = ON) ON [$TargetFileGroup]).")
        Add-Report -Schema $schemaName -Table $tableName -Structure 'Clustered columnstore' -Lob ([int]$t.HasLob -eq 1) -Path 'SKIPPED: clustered columnstore' -ObjectId $objId
        continue
    }

    $isHeap   = ([int]$t.ClusteredIndexId -eq 0)
    $hasLob   = ([int]$t.HasLob -eq 1)
    $resolved = Resolve-Compression ([string]$t.CurrentCompression)
    $pathLabel = ''
    $structure = ''

    # Rowstore index ops can't run ONLINE on a table that has any columnstore index
    # (clustered = this is a CCI table; nonclustered = a type-6 index exists). Columnstore
    # rebuilds themselves keep ONLINE - only the rowstore ops on such tables are forced offline.
    $tableHasCs = $isCCI -or (
        $idxByObject.ContainsKey($objId) -and
        @($idxByObject[$objId] | Where-Object { [int]$_.IndexType -eq 6 }).Count -gt 0
    )
    $onlineEff  = ($Online -and -not $tableHasCs)
    if ($Online -and $tableHasCs) {
        $skips.Add("$full : rowstore index moves run OFFLINE here (ONLINE is not allowed on a table with a columnstore index).")
    }

    Add-Batch "-- ===== $full =====  (heap=$isHeap, lob=$hasLob, cci=$isCCI, compression=$resolved)"

    # Names for the transient partition objects (object-scoped so they never collide).
    $pf = "ZZ_MoveFG_pf_$objId"
    $ps = "ZZ_MoveFG_ps_$objId"
    $qpf = Quote-Name $pf
    $qps = Quote-Name $ps

    # ---- CLUSTERED COLUMNSTORE (only reached with -IncludeColumnstore) ---------------------
    if ($isCCI) {
        $structure = 'Clustered columnstore'
        $pathLabel = 'Clustered columnstore rebuild'
        $qci = Quote-Name ([string]$t.ClusteredIndexName)
        # CCI takes no column list; its rebuild relocates the columnstore segments (LOB_DATA) too.
        # Rowstore -Compression does not apply; columnstore compression is preserved as-is.
        Add-Batch "CREATE CLUSTERED COLUMNSTORE INDEX $qci ON $qFull`n$(Get-ColumnstoreWithOptions ([string]$t.CurrentCompression))`nON $tgtFG;"
    }
    # ---- CLUSTERED TABLE ------------------------------------------------------------------
    elseif (-not $isHeap) {
        $structure = 'Clustered'
        $ciName  = [string]$t.ClusteredIndexName
        $qci     = Quote-Name $ciName
        $unique  = if ([int]$t.ClusteredIsUnique -eq 1) { 'UNIQUE ' } else { '' }
        $keyList = Get-KeyColumnList -ObjectId $objId -IndexId 1
        $with    = Get-WithOptions -DropExisting -Compression $resolved -SuppressOnline:$tableHasCs

        if (-not $hasLob) {
            $pathLabel = 'Clustered rebuild'
            Add-Batch "CREATE ${unique}CLUSTERED INDEX $qci ON $qFull ($keyList)`n$with`nON $tgtFG;"
        }
        else {
            # Partition-scheme trick: rewrite incl. LOB_DATA onto target FG, then de-partition.
            $partColName = Get-FirstKeyColumnName -ObjectId $objId -IndexId 1
            $partCol     = $colsByObject[$objId] | Where-Object { [string]$_.ColumnName -eq $partColName } | Select-Object -First 1
            $literal     = if ($partCol) { Get-BoundaryLiteral ([string]$partCol.TypeName) } else { $null }

            if (-not $partCol -or -not (Test-PartitionLegal $partCol) -or -not $literal) {
                $pathLabel = "Clustered in-row only (LOB left; key '$partColName' not partitionable)"
                $skips.Add("$full : LOB present but the clustered key's leading column ('$partColName') is not partition-legal - in-row moved, LOB_DATA left in place. Recreate the table manually to relocate LOB.")
                Add-Batch "CREATE ${unique}CLUSTERED INDEX $qci ON $qFull ($keyList)`n$with`nON $tgtFG;"
            }
            else {
                $pathLabel = 'Clustered + LOB partition move'
                $qPartCol = Quote-Name $partColName
                Add-Batch @"
IF EXISTS (SELECT 1 FROM sys.partition_schemes   WHERE name = N'$ps') DROP PARTITION SCHEME $qps;
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'$pf') DROP PARTITION FUNCTION $qpf;
CREATE PARTITION FUNCTION $qpf ($($partCol.TypeName)) AS RANGE RIGHT FOR VALUES ($literal);
CREATE PARTITION SCHEME   $qps AS PARTITION $qpf ALL TO ($tgtFG);
"@
                Add-Batch "CREATE ${unique}CLUSTERED INDEX $qci ON $qFull ($keyList)`n$with`nON $qps ($qPartCol);"
                Add-Batch "CREATE ${unique}CLUSTERED INDEX $qci ON $qFull ($keyList)`n$with`nON $tgtFG;"
                Add-Batch "DROP PARTITION SCHEME $qps;`nDROP PARTITION FUNCTION $qpf;"
            }
        }
    }
    # ---- HEAP -----------------------------------------------------------------------------
    else {
        $structure = 'Heap'
        $tmp  = "ZZ_MoveFG_CI_$objId"
        $qtmp = Quote-Name $tmp
        $legalCols = ($colsByObject[$objId] | Where-Object { Test-PartitionLegal $_ })
        $keyCol = $legalCols | Select-Object -First 1
        if (-not $keyCol) {
            $skips.Add("$full : heap has no index-legal column for a temporary clustered index - skipped.")
            Add-Report -Schema $schemaName -Table $tableName -Structure 'Heap' -Lob $hasLob -Path 'SKIPPED: no index-legal column' -ObjectId $objId
            continue
        }
        $qKeyCol = Quote-Name $keyCol.ColumnName

        if (-not $hasLob) {
            $pathLabel = 'Heap move (temp CI)'
            Add-Batch "CREATE CLUSTERED INDEX $qtmp ON $qFull ($qKeyCol) $(Get-WithOptions -SuppressOnline:$tableHasCs) ON $tgtFG;"
            Add-Batch "DROP INDEX $qtmp ON $qFull WITH (MOVE TO $tgtFG$(if ($onlineEff) { ', ONLINE = ON' }));"
        }
        else {
            $pathLabel = 'Heap + LOB partition move'
            $literal = Get-BoundaryLiteral ([string]$keyCol.TypeName)
            Add-Batch @"
IF EXISTS (SELECT 1 FROM sys.partition_schemes   WHERE name = N'$ps') DROP PARTITION SCHEME $qps;
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'$pf') DROP PARTITION FUNCTION $qpf;
CREATE PARTITION FUNCTION $qpf ($($keyCol.TypeName)) AS RANGE RIGHT FOR VALUES ($literal);
CREATE PARTITION SCHEME   $qps AS PARTITION $qpf ALL TO ($tgtFG);
"@
            Add-Batch "CREATE CLUSTERED INDEX $qtmp ON $qFull ($qKeyCol) $(Get-WithOptions -SuppressOnline:$tableHasCs) ON $qps ($qKeyCol);"
            # De-partition onto the plain target FG (LOB already relocated), THEN drop to a heap.
            # Dropping a *partitioned* clustered index WITH (MOVE TO <single FG>) is not reliably
            # allowed, so we de-partition first via DROP_EXISTING.
            Add-Batch "CREATE CLUSTERED INDEX $qtmp ON $qFull ($qKeyCol) $(Get-WithOptions -DropExisting -SuppressOnline:$tableHasCs) ON $tgtFG;"
            Add-Batch "DROP INDEX $qtmp ON $qFull WITH (MOVE TO $tgtFG$(if ($onlineEff) { ', ONLINE = ON' }));"
            Add-Batch "DROP PARTITION SCHEME $qps;`nDROP PARTITION FUNCTION $qpf;"
        }

        # Re-apply compression to the heap (dropping the temp CI leaves an uncompressed heap).
        if ($resolved -ne 'NONE') {
            Add-Batch "ALTER TABLE $qFull REBUILD WITH (DATA_COMPRESSION = $resolved$(if ($onlineEff) { ', ONLINE = ON' }));"
        }
    }

    # ---- NONCLUSTERED INDEXES -------------------------------------------------------------
    if ($idxByObject.ContainsKey($objId)) {
        $ncTypes = if ($IncludeColumnstore) { @(2, 6) } else { @(2) }
        foreach ($ix in ($idxByObject[$objId] | Where-Object { $ncTypes -contains [int]$_.IndexType })) {
            if ([string]$ix.DataSpaceType -eq 'PS') {
                $skips.Add("$full.$($ix.IndexName) : partitioned nonclustered index - skipped.")
                continue
            }
            $ixName  = [string]$ix.IndexName
            $qix     = Quote-Name $ixName

            # Nonclustered columnstore: distinct syntax (column list, columnstore compression).
            if ([int]$ix.IndexType -eq 6) {
                $cols = Get-ColumnstoreColumnList -ObjectId $objId -IndexId ([int]$ix.IndexId)
                Add-Batch "CREATE NONCLUSTERED COLUMNSTORE INDEX $qix ON $qFull ($cols)`n$(Get-ColumnstoreWithOptions ([string]$ix.CurrentCompression))`nON $tgtFG;"
                continue
            }

            $ixUniq  = if ([int]$ix.IsUnique -eq 1) { 'UNIQUE ' } else { '' }
            $ixKeys  = Get-KeyColumnList   -ObjectId $objId -IndexId ([int]$ix.IndexId)
            $ixIncl  = Get-IncludeColumnList -ObjectId $objId -IndexId ([int]$ix.IndexId)
            $ixComp  = Resolve-Compression ([string]$ix.CurrentCompression)
            $ixWith  = Get-WithOptions -DropExisting -Compression $ixComp -SuppressOnline:$tableHasCs

            if (Test-IncludeHasLob -ObjectId $objId -IndexId ([int]$ix.IndexId)) {
                $skips.Add("$full.$ixName : nonclustered index has LOB included column(s); in-row moved, the index's own LOB allocation may remain on the source FG.")
            }

            $stmt = "CREATE ${ixUniq}NONCLUSTERED INDEX $qix ON $qFull ($ixKeys)"
            if ($ixIncl) { $stmt += " INCLUDE ($ixIncl)" }
            if ([int]$ix.HasFilter -eq 1 -and $ix.FilterDefinition) { $stmt += " WHERE $($ix.FilterDefinition)" }
            $stmt += "`n$ixWith`nON $tgtFG;"
            Add-Batch $stmt
        }
    }

    Add-Report -Schema $schemaName -Table $tableName -Structure $structure -Lob $hasLob -Path $pathLabel -ObjectId $objId
}

# Warn about index types this script does not relocate (left on their current filegroup).
foreach ($si in $specialIdx) {
    $sFull = "$($si.SchemaName).$($si.TableName)"
    if ($Schema -and ($Schema -notcontains [string]$si.SchemaName)) { continue }
    if ($ExcludeTable -and ($ExcludeTable -contains $sFull)) { continue }
    $skips.Add("$sFull.$($si.IndexName) : $($si.TypeDesc) index - NOT moved (needs bespoke handling); left on its current filegroup.")
    Add-Report -Schema ([string]$si.SchemaName) -Table ([string]$si.TableName) -Structure ([string]$si.TypeDesc) -Lob $false -Path "SKIPPED: $($si.TypeDesc) index (not relocated)" -ObjectId ([int]$si.ObjectId)
}

# --------------------------------------------------------------------------------------------
# -ReportOnly : classify + size every object, emit no DDL.
# --------------------------------------------------------------------------------------------
if ($ReportOnly) {
    $ordered = $report | Sort-Object -Property TotalMB -Descending

    $ordered | Format-Table -Property Schema, Table, Structure, LOB, NCIdx, DataMB, LobMB, TotalMB, Path -AutoSize | Out-Host

    $moved      = @($report | Where-Object { $_.Path -notlike 'SKIPPED:*' })
    $skippedRpt = @($report | Where-Object { $_.Path -like  'SKIPPED:*' })
    $lobLeft    = @($report | Where-Object { $_.Path -like  '*LOB left*' })

    $totalData  = ($report | Measure-Object DataMB  -Sum).Sum
    $totalLob   = ($report | Measure-Object LobMB   -Sum).Sum
    $totalAll   = ($report | Measure-Object TotalMB -Sum).Sum
    $largest    = ($moved   | Measure-Object TotalMB -Maximum).Maximum
    if (-not $totalData) { $totalData = 0 }
    if (-not $totalLob)  { $totalLob  = 0 }
    if (-not $totalAll)  { $totalAll  = 0 }
    if (-not $largest)   { $largest   = 0 }

    Write-Host ""
    Write-Host "==== Filegroup move report: [$Database] -> [$TargetFileGroup] ====" -ForegroundColor Cyan
    Write-Host ("  Tables to move        : {0}" -f $moved.Count)
    Write-Host ("  Tables skipped        : {0}" -f $skippedRpt.Count)
    Write-Host ("  Tables w/ LOB left    : {0}" -f $lobLeft.Count)
    Write-Host ("  Data (in-row+ovf)     : {0:N0} MB" -f $totalData)
    Write-Host ("  LOB data              : {0:N0} MB" -f $totalLob)
    Write-Host ("  Total to relocate     : {0:N0} MB ({1:N1} GB)" -f $totalAll, ($totalAll / 1024))
    Write-Host ("  Largest single object : {0:N0} MB  <- transient headroom on [$TargetFileGroup]" -f $largest)
    Write-Host ""
    Write-Host "  Sizing guidance:" -ForegroundColor Yellow
    Write-Host ("    - Pre-size [$TargetFileGroup] to ~{0:N0} MB (it ends up holding all moved data)." -f $totalAll)
    Write-Host ("    - Add headroom of at least the largest object (~{0:N0} MB); LOB tables briefly double-write." -f $largest)
    Write-Host  "    - SORT_IN_TEMPDB routes sort space to tempdb (~largest index); size tempdb accordingly."
    Write-Host  "    - In FULL recovery these rebuilds are fully logged; watch the log / take log backups."
    if ($skippedRpt.Count -gt 0 -or $lobLeft.Count -gt 0) {
        Write-Host ""
        Write-Host "  Skipped / partial (see Path column above):" -ForegroundColor Yellow
        @($skippedRpt + $lobLeft) | Sort-Object Schema, Table -Unique |
            ForEach-Object { Write-Host ("    - {0}.{1}: {2}" -f $_.Schema, $_.Table, $_.Path) }
    }

    if ($ReportCsvPath) {
        $ordered | Export-Csv -Path $ReportCsvPath -NoTypeInformation -Encoding UTF8
        Write-Host ""
        Write-Host "Report exported to: $ReportCsvPath" -ForegroundColor Cyan
    }
    return
}

# --------------------------------------------------------------------------------------------
# Assemble script
# --------------------------------------------------------------------------------------------
# Filegroup/file creation (if any) must run before everything else.
if ($prepBatches.Count -gt 0) {
    $marker = [System.Collections.Generic.List[string]]::new()
    $marker.Add("-- ===== Ensure target filegroup [$TargetFileGroup] exists =====")
    $marker.AddRange($prepBatches)
    $batches.InsertRange(0, $marker)
}

$header = @"
/* ============================================================================
   Move database objects to filegroup [$TargetFileGroup]
   Database     : $Database
   Instance     : $SqlInstance
   Compression  : $Compression
   Online       : $([bool]$Online)
   Generated    : $(Get-Date -Format 'u')
   ----------------------------------------------------------------------------
   REVIEW BEFORE RUNNING. Take a backup. Run in a maintenance window. Ensure the
   target filegroup has free space >= your largest object, and watch log growth.
   ============================================================================ */
USE $(Quote-Name $Database);
SET XACT_ABORT ON;
GO
"@

$scriptText = $header + "`n" + (($batches | ForEach-Object { $_ + "`nGO" }) -join "`n`n") + "`n"

if ($OutputScriptPath) {
    $scriptText | Out-File -FilePath $OutputScriptPath -Encoding UTF8
    Write-Host "T-SQL written to: $OutputScriptPath" -ForegroundColor Cyan
}

if ($skips.Count -gt 0) {
    Write-Warning "Skipped / flagged objects ($($skips.Count)):"
    $skips | ForEach-Object { Write-Warning "  - $_" }
}

# --------------------------------------------------------------------------------------------
# Execute (only with -Execute, and honouring -WhatIf / -Confirm)
# --------------------------------------------------------------------------------------------
if (-not $Execute) {
    Write-Host "`nDry run complete. $($batches.Count) batch(es) generated. Re-run with -Execute to apply." -ForegroundColor Yellow
    if (-not $OutputScriptPath) { return $scriptText }
    return
}

if ($PSCmdlet.ShouldProcess("$Database on $SqlInstance", "Move objects to filegroup [$TargetFileGroup]")) {
    $n = 0
    foreach ($b in $batches) {
        $n++
        if ($b -match '^\s*--') { continue }   # comment-only marker batch
        Write-Progress -Activity "Moving objects to [$TargetFileGroup]" -Status "Batch $n of $($batches.Count)" -PercentComplete (($n / $batches.Count) * 100)
        try { Invoke-Query -Sql $b -NonQuery }
        catch { throw "Failed on batch ${n}:`n$b`n`n$($_.Exception.Message)" }
    }
    Write-Progress -Activity "Moving objects to [$TargetFileGroup]" -Completed
    Write-Host "Done. Applied $n batch(es)." -ForegroundColor Green
}
