<#
    Name:           sqlServerPermisisonsToSpreadsheet.ps1
    Written by:     Richard Armstrong-Finnerty richard.armstrong.finnerty@gmail.com & ChatGPT
    Version:        1.1
    Date:           08-FEB-2026
    Purpose:        Based upon a list of SQL Server instances:
                    1. Write all instance-level permissions to an Excel worksheet.
                    2. Write individual instance-level permissions to an individual Excel worksheet.
                    3. Write all database-level permissions to an Excel worksheet.
                    4. Write individual database-level permissions to an individual Excel worksheet.
                    5. Delete spreadsheets older than 90 days.
                    Spreadsheet is auto-width and has filters on every column.
    Notes:          Requires sysadmin role membership on all instances in $instances list.
                    The clever T-SQL was lifted from StackExchange: https://dba.stackexchange.com/questions/36618/list-all-permissions-for-a-given-role
#>

 
 

param(
    # AD domain / prefix used in SQL logins (e.g. 'myDomain' for 'myDomain\jdoe')
    [Parameter(Mandatory=$false)]
    [string]$AdDomainPrefix = 'myDomain',

    # Output directory for the generated spreadsheet + zip (default: current user's Documents)
    [Parameter(Mandatory=$false)]
    [string]$OutputDir = [Environment]::GetFolderPath('MyDocuments'),

    # SQL instances to process. If omitted, InstancesFile is read.
    [Parameter(Mandatory=$false)]
    [string[]]$Instances,

    # Path to a text file containing instances (one per line). Blank lines and lines starting with # are ignored.
    [Parameter(Mandatory=$false)]
    [string]$InstancesFile = (Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'instances.txt'),

    # Optional list of logins to exclude from AD resolution (often computer accounts or service principals)
    [Parameter(Mandatory=$false)]
    [string[]]$ExcludedLogins = @(),

# Optional comma-separated list of database user names to exclude from output (matches [Database User Name])
[Parameter(Mandatory=$false)]
[string]$ExcludedDbUsersCsv = '',

    # Include internal SQL Policy logins (##MS_Policy*##). By default they are excluded to reduce noise.
    [Parameter(Mandatory=$false)]
    [switch]$IncludePolicyLogins,

    # Include rows where [Login Name] is blank/null. By default they are excluded to reduce noise.
    [Parameter(Mandatory=$false)]
    [switch]$IncludeBlankLogins,

    # Include system/service principals that start with NT* in the instance-role membership output
    [Parameter(Mandatory=$false)]
    [switch]$IncludeSystemLogins
)

# Current time.
$timestamp = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss').ToString()
# Excel Sheet Name.
$excelSheetNametimestamp = ($timestamp.Replace(' ','_').Replace(':',''))

# Import required PowerShell modules:
Import-Module dbatools
Import-Module importexcel
# Output date for interactive call.
'
STARTING'
Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
# Ensure output directory exists
if (-not (Test-Path -LiteralPath $OutputDir)) { New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null }

# Spreadsheet path.
$path = (Join-Path $OutputDir "sqlServerPermissions_$excelSheetNametimestamp.xlsx")

# Resolve instances
if ($Instances -and $Instances.Count -gt 0) {
    $instances = $Instances
} else {
    if (-not (Test-Path -LiteralPath $InstancesFile)) {
        throw "InstancesFile not found: $InstancesFile"
    }
    $instances = Get-Content -LiteralPath $InstancesFile | ForEach-Object { $_.Trim() } | Where-Object { $_ -and -not $_.StartsWith("#") }
}

# initialize array that holds PS objects for instance-level permissions details
$instanceArray  = @()

# initialize array that holds PS objects for database-permissions details
$dbArray        = @()
 
# initialize hash table that holds AD names
$adNames = @{}
 
$computers = $ExcludedLogins

# Parse excluded DB users (comma-separated string) into an array
$ExcludedDbUsers = @()
if ($ExcludedDbUsersCsv) {
    $ExcludedDbUsers = $ExcludedDbUsersCsv.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
}

# Cache of system/internal principals discovered via T-SQL (per instance / per database)
$systemServerLoginsByInstance = @{}   # key: instance string, value: HashSet[string] of login names
$systemDbUsersByInstanceDb    = @{}   # key: "$instance|$db", value: HashSet[string] of db user names

function New-StringHashSet {
    param([string[]]$Items)
    $hs = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
    if ($Items) { foreach($i in $Items) { if ($i) { [void]$hs.Add($i) } } }
    return $hs
}


# Case-insensitive set of excluded DB users for quick filtering
$excludedDbUsersSet = New-StringHashSet -Items $ExcludedDbUsers

# Loop through instances
foreach ($instance in $instances)
{
# Next 2 lines for interactive call.
' '
Write-Host $instance -ForegroundColor Yellow

# Discover system/internal server-level logins via T-SQL (cached per instance)
if (-not $systemServerLoginsByInstance.ContainsKey($instance)) {
    $sysLoginQuery = @"
SELECT sp.name
FROM sys.server_principals AS sp
WHERE sp.type IN ('S','U','G','C','K')
AND (
    sp.name LIKE N'NT SERVICE\%' OR
    sp.name LIKE N'NT AUTHORITY\%' OR
    sp.name LIKE N'BUILTIN\%' OR
    sp.name LIKE N'##%##' OR
    sp.type_desc IN (N'CERTIFICATE_MAPPED_LOGIN', N'ASYMMETRIC_KEY_MAPPED_LOGIN') OR
    sp.name = N'sa'
);
"@
    try {
        $sysLogins = Invoke-DbaQuery -SqlInstance $instance -Database master -Query $sysLoginQuery | Select-Object -ExpandProperty name
    } catch {
        $sysLogins = @()
    }
    $systemServerLoginsByInstance[$instance] = New-StringHashSet -Items $sysLogins
}

$instanceRoles = (Get-DbaServerRole -SqlInstance $instance) | Sort-Object name -Unique

    # Loop through the instance-level roles
    foreach ($instanceRole in $instanceRoles)
    {

    # Get instance role members for this instance role
    $instanceRoleMembers = (Get-DbaServerRoleMember -SqlInstance $instance -serverRole $instancerole.name) | Sort-Object name -Unique

        # Loop through the instance role members
        foreach ($instanceRoleMember in $instanceRoleMembers)
        {
            # Ignore System ones
            if ($IncludeSystemLogins -or $instanceRoleMember.Name -notlike "NT*")
            {
            
            # Write to array of hashes.
            $instanceArray +=   [pscustomobject] `
            @{`
            'Instance' = $instance; `
            'Instance Role' = $instanceRole.Role; `
            'Instance Role Member' = $instanceRolemember.Name;}
            }
        }
    }
              
# Get databases for this instance - avoid inaccessible ones.
$databases = Get-DbaDatabase -SqlInstance $instance -Status Normal| Sort-Object Name

    # Loop through the databases and get permissions.
    foreach ($database in $databases)
    {
    # Database-level permissions T-SQL
    $tSql =
    "
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    DECLARE @HideDatabaseDiagrams BIT;
    SET @HideDatabaseDiagrams = 1;
   
    -- CTE to get nested roles
    ;WITH theRoles (member_principal_id, role_principal_id)
    AS
    (
    SELECT
    r.member_principal_id,
    r.role_principal_id
    FROM sys.database_role_members r
    UNION ALL
    SELECT
    tr.member_principal_id,
    rm.role_principal_id
    FROM sys.database_role_members rm
    INNER JOIN theRoles tr
    ON rm.member_principal_id = tr.role_principal_id
    )
   
        --1) List all access provisioned to a SQL user or Windows user/group directly
        SELECT '" + $instance + "' [Instance Name],
            db_name() [Database Name],
            [User Type] = CASE princ.[type]
                             WHEN 'S' THEN 'SQL User'
                             WHEN 'U' THEN 'Windows User'
                             WHEN 'G' THEN 'Windows Group'
                         END,
            [Database User Name] = princ.[name],
            [Login Name]        = ulogin.[name],
            [AD Name]           = '',
            [Role]              = NULL,
            [Permission Type]   = perm.[permission_name],
            [Permission State]  = perm.[state_desc],
            [Object Type] = CASE perm.[class]
                               WHEN 1 THEN obj.[type_desc]        -- Schema-contained objects
                               ELSE perm.[class_desc]             -- Higher-level objects
                           END,
            [Object Schema] = objschem.[name],
            [Object Name] = CASE perm.[class]
                               WHEN 3 THEN permschem.[name]       -- Schemas
                               WHEN 4 THEN imp.[name]             -- Impersonations
                               ELSE OBJECT_NAME(perm.[major_id])  -- General objects
                           END,
            [Column Name] = col.[name]
        FROM
            --Database user
            sys.database_principals            AS princ
            --Login accounts
            LEFT JOIN sys.server_principals    AS ulogin    ON ulogin.[sid] = princ.[sid]
            --Permissions
            LEFT JOIN sys.database_permissions AS perm      ON perm.[grantee_principal_id] = princ.[principal_id]
            LEFT JOIN sys.schemas              AS permschem ON permschem.[schema_id] = perm.[major_id]
            LEFT JOIN sys.objects              AS obj       ON obj.[object_id] = perm.[major_id]
            LEFT JOIN sys.schemas              AS objschem  ON objschem.[schema_id] = obj.[schema_id]
            --Table columns
            LEFT JOIN sys.columns              AS col       ON col.[object_id] = perm.[major_id]
                                                               AND col.[column_id] = perm.[minor_id]
            --Impersonations
            LEFT JOIN sys.database_principals  AS imp       ON imp.[principal_id] = perm.[major_id]
        WHERE
            princ.[type] IN ('S','U','G')
            -- No need for these system accounts
            AND princ.[name] NOT IN ('sys', 'INFORMATION_SCHEMA')
   
    UNION
   
        --2) List all access provisioned to a SQL user or Windows user/group through a database or application role
        SELECT '" + $instance + "' [Instance Name],
            db_name() [Database Name],
            [User Type] = CASE membprinc.[type]
                             WHEN 'S' THEN 'SQL User'
                             WHEN 'U' THEN 'Windows User'
                             WHEN 'G' THEN 'Windows Group'
                         END,
            [Database User Name] = membprinc.[name],
            [Login Name]        = ulogin.[name],
            [AD Name]           = '',
            [Role]              = roleprinc.[name],
            [Permission Type]   = perm.[permission_name],
            [Permission State]  = perm.[state_desc],
            [Object Type] = CASE perm.[class]
                               WHEN 1 THEN obj.[type_desc]        -- Schema-contained objects
                               ELSE perm.[class_desc]             -- Higher-level objects
                           END,
            [Object Schema] = objschem.[name],
            [Object Name] = CASE perm.[class]
                               WHEN 3 THEN permschem.[name]       -- Schemas
                               WHEN 4 THEN imp.[name]             -- Impersonations
                               ELSE OBJECT_NAME(perm.[major_id])  -- General objects
                           END,
            [Column Name] = col.[name]
        FROM
            theRoles AS [members]
            --Role/member associations
            --- RAF ---sys.database_role_members          AS members
            --Roles
            JOIN      sys.database_principals  AS roleprinc ON roleprinc.[principal_id] = members.[role_principal_id]
            --Role members (database users)
            JOIN      sys.database_principals  AS membprinc ON membprinc.[principal_id] = members.[member_principal_id]
            --Login accounts
            LEFT JOIN sys.server_principals    AS ulogin    ON ulogin.[sid] = membprinc.[sid]
            --Permissions
            LEFT JOIN sys.database_permissions AS perm      ON perm.[grantee_principal_id] = roleprinc.[principal_id]
            LEFT JOIN sys.schemas              AS permschem ON permschem.[schema_id] = perm.[major_id]
            LEFT JOIN sys.objects              AS obj       ON obj.[object_id] = perm.[major_id]
            LEFT JOIN sys.schemas              AS objschem  ON objschem.[schema_id] = obj.[schema_id]
            --Table columns
            LEFT JOIN sys.columns              AS col       ON col.[object_id] = perm.[major_id]
                                                               AND col.[column_id] = perm.[minor_id]
            --Impersonations
            LEFT JOIN sys.database_principals  AS imp       ON imp.[principal_id] = perm.[major_id]
        WHERE
            membprinc.[type] IN ('S','U','G')
            -- No need for these system accounts
            AND membprinc.[name] NOT IN ('sys', 'INFORMATION_SCHEMA')
   
    UNION
   
        --3) List all access provisioned to the public role, which everyone gets by default
        SELECT '" + $instance + "' [Instance Name],
            db_name() [Database Name],
            [User Type]         = '{All Users}',
            [Database User Name] = '{All Users}',
            [Login Name]        = '{All Users}',
            [AD Name]           = '',
            [Role]              = roleprinc.[name],
            [Permission Type]   = perm.[permission_name],
            [Permission State]  = perm.[state_desc],
            [Object Type] = CASE perm.[class]
                               WHEN 1 THEN obj.[type_desc]        -- Schema-contained objects
                               ELSE perm.[class_desc]             -- Higher-level objects
                           END,
            [Object Schema] = objschem.[name],
            [Object Name] = CASE perm.[class]
                               WHEN 3 THEN permschem.[name]       -- Schemas
                               WHEN 4 THEN imp.[name]             -- Impersonations
                               ELSE OBJECT_NAME(perm.[major_id])  -- General objects
                           END,
            [Column Name] = col.[name]
        FROM
            --Roles
            sys.database_principals            AS roleprinc
            --Role permissions
            LEFT JOIN sys.database_permissions AS perm      ON perm.[grantee_principal_id] = roleprinc.[principal_id]
            LEFT JOIN sys.schemas              AS permschem ON permschem.[schema_id] = perm.[major_id]
            --All objects
            JOIN      sys.objects              AS obj       ON obj.[object_id] = perm.[major_id]
            LEFT JOIN sys.schemas              AS objschem  ON objschem.[schema_id] = obj.[schema_id]
            --Table columns
            LEFT JOIN sys.columns              AS col       ON col.[object_id] = perm.[major_id]
                                                               AND col.[column_id] = perm.[minor_id]
            --Impersonations
            LEFT JOIN sys.database_principals  AS imp       ON imp.[principal_id] = perm.[major_id]
        WHERE
            roleprinc.[type] = 'R'
            AND roleprinc.[name] = 'public'
            AND obj.[is_ms_shipped] = 0
    ORDER BY
        1,2,3,4,5,6,9,10,11,12,7,8"
 
        # Outout DB name for interactive call.
        $database.Name

        # Discover system/internal database principals for this DB via T-SQL (cached)
        $sysDbKey = "$instance|$($database.Name)"
        if (-not $systemDbUsersByInstanceDb.ContainsKey($sysDbKey)) {
            $sysDbQuery = @"
SELECT dp.name
FROM sys.database_principals AS dp
WHERE dp.principal_id > 0
AND (
    dp.name IN (N'dbo', N'guest', N'sys', N'INFORMATION_SCHEMA') OR
    dp.name LIKE N'##%##' OR
    dp.name LIKE N'MS_DataCollector%' OR
    dp.type_desc IN (N'CERTIFICATE_MAPPED_USER', N'ASYMMETRIC_KEY_MAPPED_USER') OR
    (dp.type_desc = N'DATABASE_ROLE' AND dp.principal_id BETWEEN 16384 AND 16393)
);
"@
            try {
                $sysDbUsers = Invoke-DbaQuery -SqlInstance $instance -Database $database.Name -Query $sysDbQuery | Select-Object -ExpandProperty name
            } catch {
                $sysDbUsers = @()
            }
            $systemDbUsersByInstanceDb[$sysDbKey] = New-StringHashSet -Items $sysDbUsers
        }

# Add to DB array (optionally excluding specific database user names)
$dbRows = @(Invoke-DbaQuery -SqlInstance $instance -database $database.name -Query $tSql)
if ($excludedDbUsersSet.Count -gt 0) {
    $dbRows = $dbRows | Where-Object { -not $_."Database User Name" -or -not $excludedDbUsersSet.Contains($_."Database User Name") }
}
$dbArray += $dbRows

    }
}
 
 
# Remove any duplicates.
$instanceArrayUnique = $instanceArray | Group-Object -Property "Instance", "Instance Role", "Instance Role Member" | ForEach-Object {$_.Group | Select-Object -First 1}
 
# Write all instances' instance-level permissions data to "All Instance Perms" worksheet
Write-Output ""
Write-Output "Writing to spreadsheet: worksheet All Instance Perms"
$ws = $instanceArrayUnique | Sort-Object "Instance","Instance Role","Instance Role Member"  | Export-Excel `
-Path $path `
-WorksheetName "All Instance Perms" `
-Append `
-ExcludeProperty 'ItemArray','RowError','RowState','Table','HasErrors' `
-BoldTopRow `
-FreezeTopRow `
-AutoSize `
-AutoFilter `
-passThru
Close-ExcelPackage $ws
 
# Add login details from AD
# NOTE: We resolve AD names for Windows principals that look like DOMAIN
## ame (not SQL/internal/system principals).
# We export two AD-derived strings:
#  - "AD Name"        -> SamAccountName (e.g. dbsa)
#  - "SamAccountName" -> Display name / Name (e.g. DBA User)
# This matches the requested spreadsheet layout.
$adSam = @{}          # login -> SamAccountName
$adDisplay = @{}      # login -> DisplayName/Name

$adEntities = $dbArray.Where({
    $_."Login Name" -and
    $_."Login Name" -like "*\*" -and
    $_."Login Name" -notlike "##*" -and
    $_."Login Name" -notlike "NT *" -and
    $_."Login Name" -notlike "NT SERVICE\*" -and
    $_."Login Name" -notlike "NT AUTHORITY\*" -and
    $_."Login Name" -notlike "BUILTIN\*" -and
    $_."Login Name" -notin $computers
}) | Select-Object -Property "Login Name","User Type" -Unique

foreach($element in $adEntities)
{
    $login = $element."Login Name"

    # Extract SAM account portion (after the backslash) and remove trailing $ (computer accounts)
    $sam = ($login.Split('\')[-1]).Trim().TrimEnd('$')
    if (-not $sam) { continue }

	    if ($login -notin $adSam.Keys -and $login -notlike '*$')
    {
	        try {
	            switch ($element."User Type") {
	                "Windows Group" {
	                    $g = Get-ADGroup -Identity $sam -Properties DisplayName,SamAccountName -ErrorAction Stop
	                    $adSam[$login] = $g.SamAccountName
                    $adDisplay[$login] = @($g.Name, $g.DisplayName) | Where-Object { $_ } | Select-Object -First 1
	                }
	                default {
	                    # Treat anything else as a user for lookup purposes
	                    $u = Get-ADUser -Identity $sam -Properties DisplayName,SamAccountName -ErrorAction Stop
	                    $adSam[$login] = $u.SamAccountName
                    $adDisplay[$login] = @($u.Name, $u.DisplayName) | Where-Object { $_ } | Select-Object -First 1
	                }
	            }
	        } catch {

            # Leave empty if lookup fails (e.g., non-local domain, no RSAT/AD module, or not found)
        }
    }
}

# Stamp AD fields back onto the main data rows
foreach($row in $dbArray)
{
    $ln = $row."Login Name"

    # Ensure the new column exists for Export-Excel
    if (-not ($row.PSObject.Properties.Name -contains 'SamAccountName')) {
        $row | Add-Member -NotePropertyName 'SamAccountName' -NotePropertyValue ''
    }

    if ($ln -like "##*") {
        $row."AD Name" = "Internal"
        $row."SamAccountName" = "Internal"
    }
    elseif ($ln -like "NT *" -or $ln -like "NT SERVICE\*" -or $ln -like "NT AUTHORITY\*" -or $ln -like "BUILTIN\*") {
        $row."AD Name" = "System"
        $row."SamAccountName" = "System"
    }
    elseif ($ln -and $adSam.ContainsKey($ln)) {

        # AD mapping:
        #   AD Name        = Display name / Name (e.g. DBA User)
        #   SamAccountName = SamAccountName (e.g. dbsa)
        $row."AD Name" = $adDisplay[$ln]
        $row."SamAccountName" = $adSam[$ln]
    }
}

# Mark system/internal principals as User Type = System using T-SQL-discovered lists (fallback to pattern checks)
foreach ($row in $dbArray)
{
    $inst = $row."Instance Name"
    $dbn  = $row."Database Name"
    $ln   = $row."Login Name"
    $dbu  = $row."Database User Name"

    $isSystem = $false

    # Server-level system logins
    if ($ln -and $systemServerLoginsByInstance.ContainsKey($inst)) {
        if ($systemServerLoginsByInstance[$inst].Contains($ln)) { $isSystem = $true }
    }

    # Database-level system users/roles
    if (-not $isSystem -and $dbu) {
        $k = "$inst|$dbn"
        if ($systemDbUsersByInstanceDb.ContainsKey($k)) {
            if ($systemDbUsersByInstanceDb[$k].Contains($dbu)) { $isSystem = $true }
        }
    }

    # Fallback pattern rules (helps if query fails or for unusual internal principals)
    if (-not $isSystem) {
        if ($ln) {
            if ($ln -like "##*" -or
                $ln -like "NT *" -or
                $ln -like "NT SERVICE\*" -or
                $ln -like "NT AUTHORITY\*" -or
                $ln -like "BUILTIN\*" -or
                $ln -eq "sa") {
                $isSystem = $true
            }
        }
        if (-not $isSystem -and $dbu) {
            if ($dbu -in @("dbo","guest","INFORMATION_SCHEMA","sys") -or
                $dbu -like "MS_DataCollector*" -or
                $dbu -like "##*") {
                $isSystem = $true
            }
        }
    }

    if ($isSystem) {
        $row."User Type" = "System"
    }
}

# Optional noise filters (defaults: exclude policy logins and blank login names)
if (-not $IncludePolicyLogins) {
    $dbArray = $dbArray | Where-Object { $_."Login Name" -notlike "##MS_Policy*##" }
}

if (-not $IncludeBlankLogins) {
    $dbArray = $dbArray | Where-Object { $_."Login Name" }
}


# Label supplied principals (from -ExcludedLogins) vs discovered ones
# Note: "supplied" here means the login/user was explicitly provided in -ExcludedLogins (often service/computer accounts you want to track)
foreach ($row in $dbArray) {
    $source = if ($row."Login Name" -and $row."Login Name" -in $ExcludedLogins) { "Supplied" } else { "Discovered" }
    $row | Add-Member -NotePropertyName "Principal Source" -NotePropertyValue $source -Force
}

# Write all instances' database-level permissions data to "All DBs Perms" worksheet
Write-Output ""
Write-Output "Writing to spreadsheet: worksheet All DBs Perms"
$dbArray | Select-Object "Principal Source","Instance Name","Database Name","User Type","Database User Name","Login Name","AD Name","SamAccountName","Role","Permission Type","Permission State","Object Type","Object Schema","Object Name","Column Name" | Sort-Object "Instance Name","Database Name","User Type","Database User Name","Login Name","Role","Permission Type","Permission State","Object Type","Object Schema","Object Name","Column Name" | Export-Excel `
-Path $path `
-WorksheetName "All DBs Perms" `
-Append `
-ExcludeProperty 'ItemArray','RowError','RowState','Table','HasErrors' `
-BoldTopRow `
-FreezeTopRow `
-AutoSize `
-AutoFilter
 
 
# Zip-up the spreadsheet.
'
Zipping spreadsheet ...'
$zipFile = "$path.zip"
Compress-Archive -LiteralPath $path -CompressionLevel Optimal -DestinationPath $zipFile
 
# remove unzipped spreadsheet
'
Dropping unzipped spreadsheet ...'
Remove-Item $path
