<#
.SYNOPSIS
  Parse SQL Server ShowPlan XML (.sqlplan / .xml) and output advanced performance insights.

.DESCRIPTION
  This script acts as an automated query tuning consultant. It parses execution plans to identify 
  Cardinality Estimation (CE) mismatches, TempDB spills, SARGability violations, implicit conversions, 
  and parameter sniffing risks. 

  It features 2 primary modes of operation:
  1. Offline File Parsing: Analyze a static .xml or .sqlplan file.
  2. Telemetry & Regression Detection: Connect to the database to append real-time Query Store 
     metrics to the analysis, and automatically generate plan-forcing scripts if regressions are found.
 
.USAGE
  # MODE 1: Standard File Analysis
  Get-SqlPlanInsights -Path ".\Execution plan.xml"

  # MODE 2: File Analysis + Query Store Telemetry + Missing Index DB Check
  Get-SqlPlanInsights -Path ".\Execution plan.xml" -ServerInstance "PROD-SQL-01" -Database "StackOverflow2013" -InspectDatabase

  # AUTHENTICATION: Using SQL Server Authentication (uses dbatools)
  $cred = Get-Credential
  Get-SqlPlanInsights -ServerInstance "192.168.1.195" -Database "StackOverflow" -TopCPU 3 -SqlCredential $cred

  # MISC PARAMETERS:
  # Show every matching operator for heuristic sections (instead of deduplicated/grouped findings):
  Get-SqlPlanInsights -Path ".\Execution plan.xml" -ShowAllHeuristicMatches

  # Tweak CE mismatch sensitivity (e.g., flag rows with > 5x ratio and minimum 50 rows):
  Get-SqlPlanInsights -Path ".\Execution plan.xml" -CEMismatchRatio 5 -CEMinRows 50

.NOTES
  - Requires the 'dbatools' module if connecting to a database for Telemetry/Harvesting.
  - Missing Index suggestions are intelligently merged to prevent bloat.
  - Parameter Sniffing test scripts (OPTION RECOMPILE, OPTIMIZE FOR) are automatically generated.
#>

function Get-SqlPlanInsights {
  [CmdletBinding()]
  param(
    [Parameter(Position = 0)]
    [string]$Path = 'C:\Users\vboxuser\Documents\ExecutionPlan1.xml',

    [int]$TopOperators = 15,

    # Cardinality estimate mismatch thresholds
    [double]$CEMismatchRatio = 10,
    [double]$CEMinRows = 10,

    # Lookup call volume heuristic
    [double]$LookupCallsThreshold = 10000,

    # Optional: DB inspection
    [string]$ServerInstance,
    [string]$Database,
    [switch]$InspectDatabase,
    [pscredential]$SqlCredential, # <--- NEW: For SQL Authentication


    # Diagnostics
    [switch]$IncludeOperatorRows,
    [switch]$SanityCheck,

    # Output: show each matching operator (ungrouped) for heuristic sections
    [switch]$ShowAllHeuristicMatches,

    # Debug SARGability detection
    [switch]$DebugSargability
  )

  Write-Output 'v3'

  # IMPORTANT: when calling .NET APIs like XmlDocument.Load(), relative paths are resolved using
  # the process working directory (often C:\Windows\System32), NOT PowerShell's current location.
  # Resolve to a full filesystem path first to avoid surprising file-not-found errors.
  $resolvedPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Path)
  if (-not (Test-Path -LiteralPath $resolvedPath)) { throw "File not found: $resolvedPath" }

  $xml = New-Object System.Xml.XmlDocument
  $xml.PreserveWhitespace = $false
  $xml.Load($resolvedPath)
  $nsUri = $xml.DocumentElement.NamespaceURI
  if ([string]::IsNullOrWhiteSpace($nsUri)) { throw "Could not detect ShowPlan XML namespace. Is this a SQL Server plan file?" }

  $nsm = New-Object System.Xml.XmlNamespaceManager ($xml.NameTable)
  $nsm.AddNamespace("sp",$nsUri)

  $rootForSelect = $xml

  function Normalize-XPath ([string]$xpath) {
    if ([string]::IsNullOrWhiteSpace($xpath)) { return $xpath }
    # When we scope $rootForSelect to a <StmtSimple>, absolute XPaths (starting with / or //)
    # still evaluate from the document root. Rewrite them to be relative to the chosen statement.
    if ($xpath.StartsWith('//')) { return '.' + $xpath }
    if ($xpath.StartsWith('/')) { return '.' + $xpath }
    return $xpath
  }

  function Select-Nodes ([string]$xpath) { $rootForSelect.SelectNodes((Normalize-XPath $xpath),$nsm) }
  function Select-Single ([string]$xpath) { $rootForSelect.SelectSingleNode((Normalize-XPath $xpath),$nsm) }

  function To-DoubleOrNull ([string]$s) {
    if ([string]::IsNullOrWhiteSpace($s)) { return $null }
    $t = $s.Trim()

    # ShowPlan often wraps numeric params in parentheses: (10000)
    $t = $t.Trim('(',')').Trim()

    # Strip quotes / N'...' (still wont parse datetimes, by design)
    if ($t -match "^[Nn]?'.*'$") {
      $t = $t -replace "^[Nn]?'",""
      $t = $t.TrimEnd("'")
      $t = $t.Trim()
    }

    # Only parse if it looks numeric
    if ($t -notmatch '^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$') { return $null }

    try { return [double]$t } catch { return $null }
  }

  function To-IntOrNull ([string]$s) { if ([string]::IsNullOrWhiteSpace($s)) { return $null }; try { [int]$s } catch { $null } }

  # --- statement selection (when plan contains multiple statements) ---
  $chosenStmt = $null
  $chosenStmtSummary = $null
  $stmtNodes = $xml.SelectNodes("//sp:StmtSimple",$nsm)
  if ($stmtNodes -and @($stmtNodes).Count -gt 1) {
    $stmtScores = foreach ($s in $stmtNodes) {
      $sid = $s.GetAttribute("StatementId")
      $cost = To-DoubleOrNull ($s.GetAttribute("StatementSubTreeCost"))
      $pNodes = $s.SelectNodes(".//sp:ParameterList/sp:ColumnReference",$nsm)
      $runtimeCount = 0
      $mismatchCount = 0
      foreach ($p in $pNodes) {
        $cv = $p.GetAttribute("ParameterCompiledValue")
        $rv = $p.GetAttribute("ParameterRuntimeValue")
        if (-not [string]::IsNullOrWhiteSpace($rv)) { $runtimeCount++ }
        if (-not [string]::IsNullOrWhiteSpace($rv) -and -not [string]::IsNullOrWhiteSpace($cv) -and $rv -ne $cv) { $mismatchCount++ }
      }
      [pscustomobject]@{
        StatementId = $sid
        Cost = $cost
        RuntimeParamCount = $runtimeCount
        ParamMismatchCount = $mismatchCount
        Node = $s
      }
    }
    $chosen = $stmtScores |
    Sort-Object @{ Expression = 'ParamMismatchCount'; Descending = $true },@{ Expression = 'RuntimeParamCount'; Descending = $true },@{ Expression = 'Cost'; Descending = $true } |
    Select-Object -First 1
    $chosenStmt = $chosen.Node
  } elseif ($stmtNodes -and @($stmtNodes).Count -eq 1) {
    $chosenStmt = $stmtNodes[0]
  }

  if ($chosenStmt) {
    $chosenStmtId = $chosenStmt.GetAttribute("StatementId")
    $chosenStmtText = $chosenStmt.GetAttribute("StatementText")

    # --- NEW: Extract Hashes for Query Store ---
    $queryHash = $chosenStmt.GetAttribute("QueryHash")
    $queryPlanHash = $chosenStmt.GetAttribute("QueryPlanHash")
    if ($chosenStmtText -and $chosenStmtText.Length -gt 220) { $chosenStmtText = $chosenStmtText.Substring(0,220) + "..." }
    $chosenStmtSummary = [pscustomobject]@{ StatementId = $chosenStmtId; StatementText = $chosenStmtText }
    # Scope all subsequent XPath queries to the chosen statement
    $rootForSelect = $chosenStmt
  }

  # --- numeric formatting helpers ---
  # Row counts: 0 decimal places
  function F0 ($v) {
    if ($null -eq $v) { return "" }
    if ($v -is [string] -and [string]::IsNullOrWhiteSpace($v)) { return "" }
    try { return ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture,"{0:0}",[double]$v)) }
    catch { return "$v" }
  }

  # Non-row numeric values (costs, ratios, ms, KB, etc.): 2 decimal places
  function F2 ($v) {
    if ($null -eq $v) { return "" }
    if ($v -is [string] -and [string]::IsNullOrWhiteSpace($v)) { return "" }
    try { return ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture,"{0:0.00}",[double]$v)) }
    catch { return "$v" }
  }

  # Back-compat: old name used throughout the script
  function F3 ($v) { return (F2 $v) }


  function Get-ScalarString ($node) {
    if (-not $node) { return $null }
    $so = $node.SelectSingleNode(".//sp:ScalarOperator",$nsm)
    if ($so) {
      $ss = $so.GetAttribute("ScalarString")
      if (-not [string]::IsNullOrWhiteSpace($ss)) { return $ss }
    }
    return $null
  }

  function Get-WarningsText ($relOpNode) {
    $warningsNode = $relOpNode.SelectSingleNode(".//sp:Warnings",$nsm)
    if (-not $warningsNode) { return $null }
    $warnFlags = @()
    foreach ($attr in $warningsNode.Attributes) {
      if ($attr.Value -eq "true") { $warnFlags += $attr.Name }
    }
    if ($warnFlags.Count -gt 0) { return ($warnFlags -join ",") }
    return $null
  }

  function Get-StatementInfo ($relOpNode) {
    $stmt = $relOpNode.SelectSingleNode("ancestor::sp:StmtSimple[1]",$nsm)
    if (-not $stmt) { return $null }
    $text = $stmt.GetAttribute("StatementText")
    if ($text -and $text.Length -gt 220) { $text = $text.Substring(0,220) + "..." }
    return [pscustomobject]@{
      StatementId = $stmt.GetAttribute("StatementId")
      StatementText = $text
    }
  }

  # Bubbles up to 2 distinct objects from the subtree so operational nodes have context in summary grids
  # Strict scoping: only return objects that belong DIRECTLY to this operator.
 # Hybrid scoping: Returns direct objects if present. Otherwise, returns downstream objects as "(via: ...)".
  # Hybrid scoping: Native PowerShell Array implementation (bulletproof)
  # Targeted Hybrid scoping: Only allows Join operators to inherit downstream table names.
  function Get-ObjectNames ($relOpNode,[int]$max = 2) {
    $objs = $relOpNode.SelectNodes(".//sp:Object",$nsm)
    if (-not $objs -or @($objs).Count -eq 0) { return "" }

    $currentNodeId = [string]$relOpNode.GetAttribute("NodeId")
    $physOp = [string]$relOpNode.GetAttribute("PhysicalOp")

    $directNames = @()
    $childNames = @()

    # Only bubble up downstream objects for Join operators to reduce noise
    $allowDownstream = ($physOp -match "Join|Nested Loops|Hash Match")

    foreach ($o in $objs) {
      $db = $o.GetAttribute("Database")
      $schema = $o.GetAttribute("Schema")
      $table = $o.GetAttribute("Table")
      $index = $o.GetAttribute("Index")

      $base = (@($db,$schema,$table) | Where-Object { $_ -and $_ -ne "" }) -join "."
      if ([string]::IsNullOrWhiteSpace($base)) { continue }
      if ($index -and $index -ne "") { $base = "$base ($index)" }

      # Check if this object belongs directly to the current RelOp
      $nearestRelOp = $o.SelectSingleNode("ancestor::sp:RelOp[1]", $nsm)
      $nearId = if ($nearestRelOp) { [string]$nearestRelOp.GetAttribute("NodeId") } else { "" }

      if ($nearId -eq $currentNodeId) {
          if ($directNames -notcontains $base) { $directNames += $base }
      } elseif ($allowDownstream) {
          if ($childNames -notcontains $base) { $childNames += $base }
      }
    }

    # 1. Access nodes (Scan/Seek) get direct assignment
    if ($directNames.Count -gt 0) {
        $lim = if ($directNames.Count -gt $max) { $max } else { $directNames.Count }
        return ($directNames[0..($lim - 1)] -join " | ")
    }

    # 2. Joins show downstream context; all other operational nodes stay cleanly blank
    if ($childNames.Count -gt 0) {
        $lim = if ($childNames.Count -gt $max) { $max } else { $childNames.Count }
        return "(via: " + ($childNames[0..($lim - 1)] -join " | ") + ")"
    }

    return ""
  }

  function Get-RunTimeSum ($relOpNode) {
    $rt = $relOpNode.SelectNodes("./sp:RunTimeInformation/sp:RunTimeCountersPerThread",$nsm)
    if (-not $rt -or @($rt).Count -eq 0) { return $null }

    # Use arrays for keys to avoid "Collection was modified..." edge cases
    $sum = [ordered]@{
      ActualRows = 0
      ActualExecutions = 0
      ActualRowsRead = 0
      ActualRebinds = 0
      ActualRewinds = 0
      ActualEndOfScans = 0
    }
    $keys = @($sum.Keys)

    $has = @{}
    foreach ($k in $keys) { $has[$k] = $false }

    # Collect per-thread ActualRows for readability/skew analysis
    $threadRows = @()

    foreach ($t in $rt) {
      $thr = To-IntOrNull ($t.GetAttribute("Thread"))
      $ar = To-DoubleOrNull ($t.GetAttribute("ActualRows"))
      if ($thr -ne $null -and $ar -ne $null) {
        $threadRows += [pscustomobject]@{ Thread = $thr; ActualRows = [double]$ar }
      }

      foreach ($k in $keys) {
        $v = $t.GetAttribute($k)
        if (-not [string]::IsNullOrWhiteSpace($v)) {
          $sum[$k] += [double]$v
          $has[$k] = $true
        }
      }
    }

    # Aggregate per-thread rows by Thread id (ShowPlan can emit multiple counters for the same Thread)
    # This prevents duplicate thread IDs in output and makes skew math stable/readable.
    if ($threadRows -and @($threadRows).Count -gt 0) {
      $rowsByThread = @{}
      foreach ($tr in @($threadRows)) {
        $tid = [int]$tr.Thread
        $rowsByThread[$tid] = ($rowsByThread[$tid] + [double]$tr.ActualRows)
      }
      $threadRowsAgg = @()
      foreach ($k in @($rowsByThread.Keys | Sort-Object)) {
        $threadRowsAgg += [pscustomobject]@{ Thread = [int]$k; ActualRows = [double]$rowsByThread[$k] }
      }
      $threadRows = $threadRowsAgg
    }

    # --- ActualRows aggregation nuance (parallel plans) ---
    # ShowPlan records counters per thread. In parallel plans:
    #  - "Work" rows = sum(ActualRows) across worker threads (useful to spot wasted work / overshoot)
    #  - "Out" rows  = best-effort output rows (coordinator Thread=0) but only trusted for some operators
    $t0 = $null
    foreach ($t in $rt) {
      if ($t.GetAttribute("Thread") -eq "0") { $t0 = $t; break }
    }

    $phys = $relOpNode.GetAttribute("PhysicalOp")
    $log = $relOpNode.GetAttribute("LogicalOp")
    $par = $relOpNode.GetAttribute("Parallel")

    $trustT0 = $false
    if ($par -eq "false") { $trustT0 = $true }
    elseif ($phys -eq "Parallelism" -and $log -match "Gather Streams") { $trustT0 = $true }

    $sumActualRowsWork = $sum.ActualRows
    $sumActualRowsOut = $null
    if ($trustT0 -and $t0 -ne $null) {
      $v0 = $t0.GetAttribute("ActualRows")
      if (-not [string]::IsNullOrWhiteSpace($v0)) { $sumActualRowsOut = [double]$v0 }
    }

    # Per-thread skew metrics (workers only, exclude Thread=0)
    $workers = $threadRows | Where-Object { $_.Thread -ne 0 }
    if (@($workers | Where-Object { $_.ActualRows -gt 0 }).Count -gt 0) { $workers = $workers | Where-Object { $_.ActualRows -gt 0 } }
    $wCount = @($workers).Count
    $minW = $null; $maxW = $null; $avgW = $null; $skewMaxAvg = $null; $skewMaxMin = $null
    if ($wCount -gt 0) {
      $mW = $workers | Measure-Object -Property ActualRows -Minimum -Maximum -Average
      $minW = [double]$mW.Minimum
      $maxW = [double]$mW.Maximum
      $avgW = [double]$mW.Average
      if ($avgW -gt 0) { $skewMaxAvg = ($maxW / $avgW) }
      if ($minW -gt 0) { $skewMaxMin = ($maxW / $minW) }
    }

    $o = [ordered]@{}
    foreach ($k in $keys) { if ($has[$k]) { $o[$k] = $sum[$k] } }

    # Preserve both interpretations
    $o['ActualRowsWork'] = $sumActualRowsWork
    $o['ActualRowsOut'] = $sumActualRowsOut

    # Back-compat: prefer Out where trusted, otherwise fall back to Work
    $o['ActualRows'] = $(if ($sumActualRowsOut -ne $null) { $sumActualRowsOut } else { $sumActualRowsWork })

    # Per-thread detail for readability
    $o['ThreadRows'] = $threadRows
    $o['WorkerThreadCount'] = $wCount
    $o['ThreadMinRows'] = $minW
    $o['ThreadMaxRows'] = $maxW
    $o['ThreadAvgRows'] = $avgW
    $o['ThreadSkewMaxAvg'] = $skewMaxAvg
    $o['ThreadSkewMaxMin'] = $skewMaxMin
    return [pscustomobject]$o
  }

  function Get-NonSargableFlags ([string]$s) {
    if ([string]::IsNullOrWhiteSpace($s)) { return $null }
    $flags = @()
    if ($s -match "TRY_CONVERT\(") { $flags += "try_convert()" }
    if ($s -match "TRY_CAST\(") { $flags += "try_cast()" }
    if ($s -match "CONVERT\(" -or $s -match "Convert\(") { $flags += "convert()" }
    if ($s -match "CONVERT_IMPLICIT") { $flags += "convert_implicit" }
    if ($s -match "ISNULL\(") { $flags += "isnull()" }
    if ($s -match "ABS\(") { $flags += "abs()" }
    if ($s -match "LEFT\(" -or $s -match "RIGHT\(" -or $s -match "SUBSTRING\(") { $flags += "string-fn" }
    if ($s -match "LOWER\(" -or $s -match "UPPER\(") { $flags += "case-fn" }
    if ($s -match "(?i)(YEAR|MONTH|DAY|DATEPART|DATENAME)\(") { $flags += "date-fn" }
    if ($s -match "(?i)LIKE\s+N?'%[^']*'") { $flags += "leading-wildcard" }
    if ($flags.Count -gt 0) { return ($flags -join ",") }
    return $null
  }


  # Build a clearer seek predicate when the ShowPlan exposes RangeColumns + RangeExpressions
  # (e.g. show: [Users].[Id] = [Posts].[OwnerUserId]) instead of only the expression side.
  function Get-SeekPredicatePretty ($ixNode) {
    if (-not $ixNode) { return $null }

    function Format-ColumnRef ($cr) {
      if (-not $cr) { return $null }
      $d = $cr.GetAttribute("Database")
      $s = $cr.GetAttribute("Schema")
      $t = $cr.GetAttribute("Table")
      $c = $cr.GetAttribute("Column")
      $parts = @($d,$s,$t,$c) | Where-Object { $_ -and $_ -ne "" }
      if ($parts.Count -eq 0) { return $null }
      return "[" + ($parts -join "].[") + "]"
    }

    $preds = $ixNode.SelectNodes(".//sp:SeekPredicateNew",$nsm)
    if (-not $preds -or @($preds).Count -eq 0) { return $null }

    $clauses = @()
    foreach ($p in @($preds)) {
      $sk = $p.SelectSingleNode(".//sp:SeekKeys",$nsm)
      if (-not $sk) { continue }

      $startCols = $sk.SelectNodes(".//sp:StartRange/sp:RangeColumns/sp:ColumnReference",$nsm)
      $startExprs = $sk.SelectNodes(".//sp:StartRange/sp:RangeExpressions/sp:ScalarOperator",$nsm)
      $endCols = $sk.SelectNodes(".//sp:EndRange/sp:RangeColumns/sp:ColumnReference",$nsm)
      $endExprs = $sk.SelectNodes(".//sp:EndRange/sp:RangeExpressions/sp:ScalarOperator",$nsm)

      # Only render the 1:1 simple case cleanly; fall back to ScalarString elsewhere.
      if ($startCols -and $startExprs -and @($startCols).Count -eq 1 -and @($startExprs).Count -eq 1) {
        $col = Format-ColumnRef $startCols[0]
        $start = $startExprs[0].GetAttribute("ScalarString")

        if (-not [string]::IsNullOrWhiteSpace($col) -and -not [string]::IsNullOrWhiteSpace($start)) {
          if ($endCols -and $endExprs -and @($endCols).Count -eq 1 -and @($endExprs).Count -eq 1) {
            $endCol = Format-ColumnRef $endCols[0]
            $end = $endExprs[0].GetAttribute("ScalarString")

            if ($endCol -eq $col -and -not [string]::IsNullOrWhiteSpace($end)) {
              if ($end -eq $start) {
                $clauses += ("{0} = {1}" -f $col,$start)
              } else {
                $clauses += ("{0} BETWEEN {1} AND {2}" -f $col,$start,$end)
              }
              continue
            }
          }
          $clauses += ("{0} >= {1}" -f $col,$start)
        }
      }
    }

    if ($clauses.Count -gt 0) { return ($clauses -join " AND ") }
    return $null
  }
  function Get-AccessDetails ($relOpNode) {
    $ixSeek = $relOpNode.SelectSingleNode("./sp:IndexSeek",$nsm)
    $ixScan = $relOpNode.SelectSingleNode("./sp:IndexScan",$nsm)
    $tblScan = $relOpNode.SelectSingleNode("./sp:TableScan",$nsm)

    $seekPred = $null
    $residualPred = $null
    $kind = $null

    if ($ixSeek) {
      $kind = "Index Seek"
      $seekPred = Get-SeekPredicatePretty $ixSeek
      if (-not $seekPred) { $seekPred = Get-ScalarString ($ixSeek.SelectSingleNode(".//sp:SeekPredicates",$nsm)) }
      $residualPred = Get-ScalarString ($ixSeek.SelectSingleNode(".//sp:Predicate",$nsm))
    }
    elseif ($ixScan) {
      $kind = "Index Scan"
      $seekPred = Get-SeekPredicatePretty $ixScan
      if (-not $seekPred) { $seekPred = Get-ScalarString ($ixScan.SelectSingleNode(".//sp:SeekPredicates",$nsm)) }
      $residualPred = Get-ScalarString ($ixScan.SelectSingleNode(".//sp:Predicate",$nsm))
    }
    elseif ($tblScan) {
      $kind = "Table Scan"
      $residualPred = Get-ScalarString ($tblScan.SelectSingleNode(".//sp:Predicate",$nsm))
    }

    return [pscustomobject]@{
      AccessType = $kind
      SeekPredicate = $seekPred
      Residual = $residualPred
    }
  }

  # For operators that don't have a seek/scan, give a useful category so "Access" isn't blank
  function Get-AccessCategory ($physicalOp,$accessType) {
    if ($accessType) { return $accessType }
    if ($physicalOp -match "Nested Loops|Hash Match|Merge Join") { return "Join" }
    if ($physicalOp -match "Spool") { return "Spool" }
    if ($physicalOp -match "Sort") { return "Sort" }
    if ($physicalOp -match "Aggregate") { return "Aggregate" }
    if ($physicalOp -match "Parallelism") { return "Exchange" }
    if ($physicalOp -match "Compute Scalar") { return "Compute" }
    if ($physicalOp -match "Segment") { return "Segment" }
    return "Other"
  }

  function Get-JoinDetails ($relOpNode) {
    $joinPred = $null
    $hash = $relOpNode.SelectSingleNode(".//sp:Hash",$nsm)
    if ($hash) {
      $joinPred = Get-ScalarString ($hash.SelectSingleNode(".//sp:ProbeResidual",$nsm))
      if (-not $joinPred) { $joinPred = Get-ScalarString ($hash.SelectSingleNode(".//sp:HashKeysBuild",$nsm)) }
      if (-not $joinPred) { $joinPred = Get-ScalarString ($hash.SelectSingleNode(".//sp:HashKeysProbe",$nsm)) }
    }
    $nl = $relOpNode.SelectSingleNode(".//sp:NestedLoops",$nsm)
    if ($nl -and -not $joinPred) { $joinPred = Get-ScalarString ($nl.SelectSingleNode(".//sp:Predicate",$nsm)) }
    $mj = $relOpNode.SelectSingleNode(".//sp:Merge",$nsm)
    if ($mj -and -not $joinPred) { $joinPred = Get-ScalarString ($mj.SelectSingleNode(".//sp:Residual",$nsm)) }
    return $joinPred
  }

  function Get-SortKeys ($relOpNode) {
    $sort = $relOpNode.SelectSingleNode(".//sp:Sort",$nsm)
    if (-not $sort) { return $null }

    $keyRefs = $sort.SelectNodes(".//sp:OrderByColumn/sp:ColumnReference",$nsm)
    if (-not $keyRefs -or @($keyRefs).Count -eq 0) { $keyRefs = $sort.SelectNodes(".//sp:SortColumn/sp:ColumnReference",$nsm) }
    if (-not $keyRefs -or @($keyRefs).Count -eq 0) { return $null }

    $keys = @()
    foreach ($kr in $keyRefs) {
      $c = $kr.GetAttribute("Column")
      $t = $kr.GetAttribute("Table")
      $s = $kr.GetAttribute("Schema")
      $d = $kr.GetAttribute("Database")
      $txt = (@($d,$s,$t,$c) | Where-Object { $_ -and $_ -ne "" }) -join "."
      if ($txt) { $keys += $txt }
    }
    if ($keys.Count -gt 0) { return ($keys -join ", ") }
    return $null
  }

  # -----------------------------
  # Useful links (always printed)
  # -----------------------------
  $referenceLinks = [pscustomobject]@{
    "Cardinality estimation (Microsoft)" = "https://learn.microsoft.com/en-us/sql/relational-databases/performance/cardinality-estimation-sql-server?view=sql-server-ver17"
    "CE Feedback (Microsoft IQP)" = "https://learn.microsoft.com/en-us/sql/relational-databases/performance/intelligent-query-processing-cardinality-estimation-feedback?view=sql-server-ver17"
    "Implicit conversion is a SARGability problem (Erik Darling)" = "https://erikdarling.com/implicit-conversion-is-a-sargability-problem/"
    "Why implicit conversions aren't SARGable (Erik Darling)" = "https://erikdarling.com/sargability-week-why-implicit-conversions-arent-sargable/"
    "Bad guesses -> bad choices (Erik Darling)" = "https://erikdarling.com/bad-guesses-and-bad-choices-better-living-through-indexes/"
    "SSMS hides missing indexes (Brent Ozar)" = "https://www.brentozar.com/archive/2018/07/management-studio-hides-missing-indexes-from-you/"
    "Missing index recs aren't perfect (Brent Ozar)" = "https://www.brentozar.com/archive/2017/08/missing-index-recommendations-arent-perfect/"
    "Expensive key lookups (Brent Ozar)" = "https://www.brentozar.com/blitzcache/expensive-key-lookups/"
    "Expensive sorts (Brent Ozar)" = "https://www.brentozar.com/blitzcache/expensive-sorts/"
  }

  # -----------------------------
  # Plan-level info
  # -----------------------------
  $qp = Select-Single "//sp:QueryPlan"
  $dop = $null
  if ($qp) { $dop = To-IntOrNull ($qp.GetAttribute("DegreeOfParallelism")) }

  # Observed DOP (best-effort) from runtime thread counters.
  # We compute TWO observations:
  #  - observed(maxRuntime): max worker-thread count across all runtime operators (most practical / robust).
  #  - observed(parallelism): max worker-thread count across Parallelism (exchange) operators *only when they expose worker threads*.
  # Preferred observed DOP:
  #  - If any Parallelism operator exposes >1 worker threads, prefer observed(parallelism).
  #  - Otherwise, fall back to observed(maxRuntime).
  $dopObserved = $null
  $dopObservedParallelism = $null
  $dopObservedMaxRuntime = $null
  try {
    function Get-ThreadTeamSize ([System.Xml.XmlNode]$relOp) {
      if (-not $relOp) { return $null }
      $threads = $relOp.SelectNodes("./sp:RunTimeInformation/sp:RunTimeCountersPerThread",$nsm)
      if (-not $threads -or @($threads).Count -eq 0) { return $null }

      $ids = @()
      foreach ($t in @($threads)) {
        $tid = To-IntOrNull ($t.GetAttribute("Thread"))
        if ($tid -ne $null) { $ids += $tid }
      }
      if ($ids.Count -eq 0) { return $null }

      $uniq = @($ids | Sort-Object -Unique)
      # Exclude coordinator Thread=0 when there are other worker threads
      if ($uniq.Count -gt 1 -and ($uniq -contains 0)) { $uniq = @($uniq | Where-Object { $_ -ne 0 }) }
      return $uniq.Count
    }

    $runtimeOps = Select-Nodes ("//sp:RelOp[sp:RunTimeInformation/sp:RunTimeCountersPerThread]")
    if ($runtimeOps -and @($runtimeOps).Count -gt 0) {
      $dopObservedMaxRuntime = 1
      foreach ($r in @($runtimeOps)) {
        $sz = Get-ThreadTeamSize $r
        if ($sz -ne $null -and $sz -gt $dopObservedMaxRuntime) { $dopObservedMaxRuntime = $sz }
      }

      $parOps = Select-Nodes ("//sp:RelOp[@PhysicalOp='Parallelism' and sp:RunTimeInformation/sp:RunTimeCountersPerThread]")
      if ($parOps -and @($parOps).Count -gt 0) {
        $dopObservedParallelism = 1
        foreach ($r in @($parOps)) {
          $sz = Get-ThreadTeamSize $r
          if ($sz -ne $null -and $sz -gt $dopObservedParallelism) { $dopObservedParallelism = $sz }
        }
        # Only treat it as a meaningful observation if > 1
        if ($dopObservedParallelism -le 1) { $dopObservedParallelism = $null }
      }

      if ($dopObservedParallelism -ne $null -and $dopObservedParallelism -gt 1) {
        $dopObserved = $dopObservedParallelism
      } else {
        $dopObserved = $dopObservedMaxRuntime
      }
    } else {
      $dopObserved = $null
      $dopObservedParallelism = $null
      $dopObservedMaxRuntime = $null
    }
  } catch {
    $dopObserved = $null
    $dopObservedParallelism = $null
    $dopObservedMaxRuntime = $null
  }

  $mg = Select-Single "//sp:MemoryGrantInfo"
  $memoryGrantInfo = $null
  if ($mg) {
    $memoryGrantInfo = [pscustomobject]@{
      RequestedKB = To-DoubleOrNull ($mg.GetAttribute("RequestedMemory"))
      GrantedKB = To-DoubleOrNull ($mg.GetAttribute("GrantedMemory"))
      UsedKB = To-DoubleOrNull ($mg.GetAttribute("UsedMemory"))
      MaxUsedKB = To-DoubleOrNull ($mg.GetAttribute("MaxUsedMemory"))
      GrantWaitMS = To-DoubleOrNull ($mg.GetAttribute("GrantWaitTime"))
      FeedbackAdjusted = $mg.GetAttribute("IsMemoryGrantFeedbackAdjusted")
    }
  }

  $xmlText = $xml.OuterXml
  $spillSignals = @("SpillLevel","HashSpillDetails","SortSpillDetails","Spilled","SpillToTempDb")
  $spillLikely = @($spillSignals | Where-Object { $xmlText -match $_ } | Select-Object -Unique)

  # -----------------------------
  # Operator rows (RelOp)
  # -----------------------------
  $relOps = Select-Nodes "//sp:RelOp"

  # --- MOVED UP FOR INDENTATION LOGIC ---
  function Get-NearestRelOpAncestor ($node) {
    $p = $node.ParentNode
    while ($p -and $p.LocalName -ne 'RelOp') { $p = $p.ParentNode }
    return $p
  }

  function Get-DirectChildRelOps ($relOpNode) {
    $kids = @()
    $desc = $relOpNode.SelectNodes(".//*[local-name()='RelOp']",$nsm)
    if (-not $desc -or @($desc).Count -eq 0) { return $kids }
    foreach ($d in $desc) {
      if ($d -eq $relOpNode) { continue }
      $nearest = Get-NearestRelOpAncestor $d
      if ($nearest -eq $relOpNode) { $kids += $d }
    }
    return $kids
  }

  # --- NEW: Find max depth for reversed (left-to-right) indentation ---
  # --- NEW: Calculate Bottom-Up Height for Perfect Left-to-Right Flow ---
  $nodeHeightMap = @{}
  
  function Get-NodeHeight([System.Xml.XmlNode]$node) {
      $id = $node.GetAttribute("NodeId")
      if ([string]::IsNullOrWhiteSpace($id)) { return 0 }
      if ($nodeHeightMap.ContainsKey($id)) { return $nodeHeightMap[$id] }

      $kids = Get-DirectChildRelOps $node
      if (-not $kids -or @($kids).Count -eq 0) {
          $nodeHeightMap[$id] = 0
          return 0
      }

      $maxKidHeight = -1
      foreach ($k in $kids) {
          $h = Get-NodeHeight $k
          if ($h -gt $maxKidHeight) { $maxKidHeight = $h }
      }

      $myHeight = $maxKidHeight + 1
      $nodeHeightMap[$id] = $myHeight
      return $myHeight
  }

  # Prime the height map for all operators
  foreach ($op in $relOps) {
      [void](Get-NodeHeight $op)
  }

  $operatorRows = @(
    foreach ($op in $relOps) {
      $nodeIdString = $op.GetAttribute("NodeId")
      $nodeId = To-IntOrNull $nodeIdString
      $physical = $op.GetAttribute("PhysicalOp")
      $logical = $op.GetAttribute("LogicalOp")

      # --- UPDATED: Left-to-Right Funnel Indentation ---
      # --- UPDATED: Left-to-Right Funnel Indentation with Node Leaders ---
      $reversedDepth = if ($nodeIdString -and $nodeHeightMap.ContainsKey($nodeIdString)) { $nodeHeightMap[$nodeIdString] } else { 0 }
      
      # Define operators that dictate major execution phases
      $isLeader = $physical -match "Hash Match|Nested Loops|Merge Join|Sort"
      
      if ($reversedDepth -eq 0) {
          # Leaf nodes (origin points)
          $displayPhysical = $physical
      } elseif ($isLeader) {
          # Node Leaders act as anchors for the eyes
          $indent = ("  " * $reversedDepth)
          $displayPhysical = $indent + ">> " + $physical.ToUpper() + " <<"
      } else {
          # Standard flowing operators
          $indent = ("  " * $reversedDepth) + "-> "
          $displayPhysical = $indent + $physical
      }



      $estRows = To-DoubleOrNull ($op.GetAttribute("EstimateRows"))
      $estCost = To-DoubleOrNull ($op.GetAttribute("EstimatedTotalSubtreeCost"))

      $warn = Get-WarningsText $op
      $st = Get-StatementInfo $op

      $rt = Get-RunTimeSum $op
      $actRows = $null; $actRowsOut = $null; $actRowsWork = $null
      $threadRows = $null; $workerThreads = $null; $tMin = $null; $tMax = $null; $tAvg = $null; $tSkewMA = $null; $tSkewMM = $null
      $execs = $null; $rowsRead = $null; $rebinds = $null; $rewinds = $null
      if ($rt) {
        if ($rt.PSObject.Properties.Name -contains "ActualRows") { $actRows = [double]$rt.ActualRows }
        if ($rt.PSObject.Properties.Name -contains "ActualRowsOut") { $actRowsOut = if ($rt.ActualRowsOut -ne $null -and $rt.ActualRowsOut -ne '') { [double]$rt.ActualRowsOut } else { $null } }
        if ($rt.PSObject.Properties.Name -contains "ActualRowsWork") { $actRowsWork = if ($rt.ActualRowsWork -ne $null -and $rt.ActualRowsWork -ne '') { [double]$rt.ActualRowsWork } else { $null } }
        if ($rt.PSObject.Properties.Name -contains "ThreadRows") { $threadRows = $rt.ThreadRows }
        if ($rt.PSObject.Properties.Name -contains "WorkerThreadCount") { $workerThreads = [int]$rt.WorkerThreadCount }
        if ($rt.PSObject.Properties.Name -contains "ThreadMinRows") { $tMin = $rt.ThreadMinRows }
        if ($rt.PSObject.Properties.Name -contains "ThreadMaxRows") { $tMax = $rt.ThreadMaxRows }
        if ($rt.PSObject.Properties.Name -contains "ThreadAvgRows") { $tAvg = $rt.ThreadAvgRows }
        if ($rt.PSObject.Properties.Name -contains "ThreadSkewMaxAvg") { $tSkewMA = $rt.ThreadSkewMaxAvg }
        if ($rt.PSObject.Properties.Name -contains "ThreadSkewMaxMin") { $tSkewMM = $rt.ThreadSkewMaxMin }
        if ($rt.PSObject.Properties.Name -contains "ActualExecutions") { $execs = [double]$rt.ActualExecutions }
        if ($rt.PSObject.Properties.Name -contains "ActualRowsRead") { $rowsRead = [double]$rt.ActualRowsRead }
        if ($rt.PSObject.Properties.Name -contains "ActualRebinds") { $rebinds = [double]$rt.ActualRebinds }
        if ($rt.PSObject.Properties.Name -contains "ActualRewinds") { $rewinds = [double]$rt.ActualRewinds }
      }

      $executed = $null
      if ($execs -ne $null) { $executed = ($execs -gt 0) }

      $ratio = $null
      $direction = $null
      if ($estRows -ne $null -and $actRows -ne $null -and $estRows -ge 0 -and $actRows -ge 0) {
        if ($estRows -eq 0 -and $actRows -eq 0) { $ratio = 1.0 }
        elseif (($estRows -eq 0 -and $actRows -gt 0) -or ($actRows -eq 0 -and $estRows -gt 0)) { $ratio = [double]::PositiveInfinity }
        elseif ($estRows -gt 0 -and $actRows -gt 0) {
          $ratio = [math]::Max($estRows / $actRows,$actRows / $estRows)
          $direction = if ($actRows -gt $estRows) { "under-est" } else { "over-est" }
        }
      }

      $severity = $null
      if ($ratio -ne $null -and $ratio -ne [double]::PositiveInfinity -and $estRows -ne $null -and $actRows -ne $null -and $estCost -ne $null) {
        $scale = [math]::Log10([math]::Max($estRows,$actRows) + 1)
        $severity = ([math]::Log10($ratio) * ($scale + 1) * (1 + $estCost))
      }

      $access = Get-AccessDetails $op
      $predNode = $op.SelectSingleNode(
        "./sp:Predicate | ./sp:Filter/sp:Predicate | ./sp:IndexScan/sp:Predicate | ./sp:IndexSeek/sp:Predicate | ./sp:TableScan/sp:Predicate",
        $nsm
      )
      $predCtx = Get-ScalarString $predNode
      $joinPred = (Get-JoinDetails $op)
      $nonSarg = Get-NonSargableFlags ($predCtx + " " + $access.SeekPredicate + " " + $access.Residual + " " + $joinPred)

      if ($DebugSargability) {
        Write-Host "DEBUG SARG: NodeId=$nodeId, Physical=$physical" -ForegroundColor DarkYellow
        Write-Host "  Predicate: $predCtx" -ForegroundColor Gray
        Write-Host "  SeekPredicate: $($access.SeekPredicate)" -ForegroundColor Gray
        Write-Host "  Residual: $($access.Residual)" -ForegroundColor Gray
        Write-Host "  NonSarg Result: $nonSarg" -ForegroundColor Cyan
      }

      $objNames = Get-ObjectNames $op 2
      $accessCategory = Get-AccessCategory $physical $access.AccessType


      # RowsRead can appear on many non-access operators in runtime counters; for readability
      # only surface it for true access operators (Scan/Seek/Lookup). Otherwise leave it blank.
      if ($rowsRead -ne $null) {
        $readOps = @('Index Scan','Clustered Index Scan','Index Seek','Clustered Index Seek','Table Scan','Key Lookup','RID Lookup')
        if ($readOps -notcontains $physical) { $rowsRead = $null }
      }
      [pscustomobject]@{
        NodeId = $nodeId
        StatementId = if ($st) { $st.StatementId } else { "" }
        StatementText = if ($st) { $st.StatementText } else { "" }

        PhysicalOp = $physical
        LogicalOp = $logical



        PhysicalOpDisplay = $displayPhysical





        EstRows = $estRows
        ActRows = $actRows
        ActRowsOut = $actRowsOut
        ActRowsWork = $actRowsWork
        ThreadRows = $threadRows
        WorkerThreads = $workerThreads
        ThreadMinRows = $tMin
        ThreadMaxRows = $tMax
        ThreadAvgRows = $tAvg
        ThreadSkewMaxAvg = $tSkewMA
        ThreadSkewMaxMin = $tSkewMM
        RowsRead = $rowsRead
        Execs = $execs
        Rebinds = $rebinds
        Rewinds = $rewinds
        Executed = $executed

        Ratio = $ratio
        Direction = $direction
        Severity = $severity

        EstCost = $estCost
        Object = $objNames
        Warnings = $warn

        Access = $accessCategory
        SeekPredicate = $access.SeekPredicate
        Residual = $access.Residual
        Predicate = $predCtx
        NonSargableHints = $nonSarg

        HasConvertImplicit = if ($nonSarg -and $nonSarg -match 'convert_implicit') { $true } else { $false }

        SortKeys = (Get-SortKeys $op)
        JoinPredicate = $joinPred
      }
    }
  )
  

  # -----------------------------
  # Sort/order support suggestions (heuristic)
  #   Goal: when we see a Sort with explicit keys, suggest an index that could provide that order.
  #   If -InspectDatabase is used, also check whether an existing index already provides the order.
  # -----------------------------

  function Parse-SortKeyToken ([string]$token) {
    if ([string]::IsNullOrWhiteSpace($token)) { return $null }
    $t = $token.Trim()
    # Token format produced by Get-SortKeys: db.schema.table.column (best effort)
    $parts = $t -split '\.'
    if ($parts.Count -lt 2) { return $null }
    $col = $parts[-1]
    $table = $null
    if ($parts.Count -ge 4) {
      $table = ($parts[0..($parts.Count-2)] | Select-Object -Last 3) -join '.'
    } elseif ($parts.Count -eq 3) {
      $table = ($parts[0..1] -join '.')
    } else {
      $table = $parts[0]
    }
    return [pscustomobject]@{ Table = $table; Column = $col }
  }

  function Normalize-FullTable ([string]$t) {
  if ([string]::IsNullOrWhiteSpace($t)) { return $null }
  # Accept tokens like db.schema.table or [db].[schema].[table] and normalize to bracketed multipart names.
  $raw = $t.Trim()
  $p = $raw -split '\.'
  $p = $p | ForEach-Object { $_.Trim().Trim('[',']') } | Where-Object { $_ -ne '' }
  if ($p.Count -eq 3) {
    return ('[{0}].[{1}].[{2}]' -f $p[0],$p[1],$p[2])
  }
  if ($p.Count -eq 2) {
    return ('[{0}].[{1}]' -f $p[0],$p[1])
  }
  # If we can't confidently normalize, return the raw token.
  return $raw
}

  function Index-KeyPrefixMatches ([string]$indexKeyCols,[string[]]$wantedCols) {
    if ([string]::IsNullOrWhiteSpace($indexKeyCols)) { return $false }
    if (-not $wantedCols -or $wantedCols.Count -eq 0) { return $false }
    # indexKeyCols format from inspection: [Col] ASC, [Col2] DESC ...
    $idxCols = @()
    foreach ($seg in ($indexKeyCols -split ',\s*')) {
      $m = [regex]::Match($seg,'\[([^\]]+)\]')
      if ($m.Success) { $idxCols += $m.Groups[1].Value }
    }
    if ($idxCols.Count -lt $wantedCols.Count) { return $false }
    for ($i=0; $i -lt $wantedCols.Count; $i++) {
      if ($idxCols[$i] -ne $wantedCols[$i]) { return $false }
    }
    return $true
  }

  # Capture the ORDER BY keys required by Sort operators. We'll later compare these against
  # existing indexes if -InspectDatabase was requested.
  $sortOrderNeeds = @()
  $sortOpsForHints = @($operatorRows | Where-Object { $_.PhysicalOp -match '^Sort$' -and -not [string]::IsNullOrWhiteSpace($_.SortKeys) })
  if ($sortOpsForHints.Count -gt 0) {
    $byTableCols = @{}
    $byTableNodeIds = @{}
    foreach ($sop in $sortOpsForHints) {
      $sopNodeId = $sop.NodeId
      foreach ($tok in @($sop.SortKeys -split ',\s*')) {
        $x = Parse-SortKeyToken $tok
        if (-not $x) { continue }
        $tbl = Normalize-FullTable $x.Table
        if (-not $tbl) { continue }

        if (-not $byTableCols.ContainsKey($tbl)) {
          $byTableCols[$tbl] = New-Object System.Collections.Generic.List[string]
          $byTableNodeIds[$tbl] = New-Object 'System.Collections.Generic.HashSet[int]'
        }

        if (-not $byTableCols[$tbl].Contains($x.Column)) { [void]$byTableCols[$tbl].Add($x.Column) }
        if ($null -ne $sopNodeId -and $sopNodeId -ne '') { [void]$byTableNodeIds[$tbl].Add([int]$sopNodeId) }
      }
    }

    foreach ($k in $byTableCols.Keys) {
      $cols = @($byTableCols[$k])
      if ($cols.Count -gt 0) {
        $nodeIds = @()
        if ($byTableNodeIds.ContainsKey($k)) { $nodeIds = @($byTableNodeIds[$k] | Sort-Object) }
        $sortOrderNeeds += [pscustomobject]@{ FullTable = $k; Columns = $cols; SortNodeIds = $nodeIds }
      }
    }
  }

$sortIndexSuggestions = @() # populated later (after optional database inspection)
# Build a best-effort map of referenced columns per table in this statement (for INCLUDE suggestions).
# We only use this for "example CREATE INDEX" output; always validate in context.
$refColsByTable = @{}
try {
  $colRefs = Select-Nodes "//sp:ColumnReference"
  foreach ($cr in @($colRefs)) {
    $db = $cr.GetAttribute("Database")
    $schema = $cr.GetAttribute("Schema")
    $table = $cr.GetAttribute("Table")
    $col = $cr.GetAttribute("Column")

    if ([string]::IsNullOrWhiteSpace($table) -or [string]::IsNullOrWhiteSpace($col)) { continue }

    $db = if ($db) { $db.Trim().Trim('[',']') } else { $null }
    $schema = if ($schema) { $schema.Trim().Trim('[',']') } else { $null }
    $table = $table.Trim().Trim('[',']')
    $col = $col.Trim().Trim('[',']')

    $key =
      if ($db -and $schema) { ('[{0}].[{1}].[{2}]' -f $db,$schema,$table) }
      elseif ($schema)      { ('[{0}].[{1}]' -f $schema,$table) }
      else                  { ('[{0}]' -f $table) }

    if (-not $refColsByTable.ContainsKey($key)) {
      $refColsByTable[$key] = New-Object 'System.Collections.Generic.HashSet[string]'
    }
    [void]$refColsByTable[$key].Add($col)
  }
} catch {
  # best effort only
}

  # -----------------------------
  # Expression implicit conversions (Compute Scalar)
  #   These are usually NOT SARGability issues, but can indicate datatype mismatches or add CPU.
  # -----------------------------
  $expressionConvertImplicit = @()
  try {
    $computeScalarOps = Select-Nodes "//sp:RelOp[@PhysicalOp='Compute Scalar' or @LogicalOp='Compute Scalar']"
    foreach ($opNode in @($computeScalarOps)) {
      $nid = $opNode.GetAttribute("NodeId")
      if ([string]::IsNullOrWhiteSpace($nid)) { continue }
      # Skip if we already flagged a predicate-level CONVERT_IMPLICIT for this node
      $row = $operatorRows | Where-Object { $_.NodeId -eq [int]$nid } | Select-Object -First 1
      if ($row -and $row.HasConvertImplicit) { continue }

      $so = $opNode.SelectSingleNode(".//sp:ScalarOperator[contains(@ScalarString,'CONVERT_IMPLICIT')]",$nsm)
      if ($so) {
        $s = $so.GetAttribute("ScalarString")
        if ($s -and $s.Length -gt 220) { $s = $s.Substring(0,220) + "..." }
        $expressionConvertImplicit += [pscustomobject]@{
          NodeId = [int]$nid
          Operator = "Compute Scalar"
          Expression = $s
        }
      }
    }
  } catch {
    # best-effort only
  }


  # -----------------------------
  # -----------------------------
  # Estimated self-cost (delta) per operator
  #   EstSelfCost = EstTotalSubtreeCost - max(child EstTotalSubtreeCost)
  # Notes:
  # - ShowPlan XML does not always use a <Children> container. Child <RelOp> nodes
  #   are typically nested under the physical operator element (e.g. <Top>, <Sort>, <NestedLoops>, etc.).
  # - We treat a RelOp's "children" as descendant RelOps whose nearest ancestor RelOp is the current node.
  # - NodeIds can repeat across statements, so we key by StatementId|NodeId.
  # -----------------------------

  $costByKey = @{}
  foreach ($r in $operatorRows) {
    if ($r.NodeId -ne $null -and $r.EstCost -ne $null) {
      $k = ("{0}|{1}" -f [string]$r.StatementId,[string]$r.NodeId)
      $costByKey[$k] = [double]$r.EstCost
    }
  }

  $childMaxByKey = @{}
  $hasKidsByKey = @{}
  foreach ($op in $relOps) {
    $nodeIdKey = $op.GetAttribute("NodeId")
    if ([string]::IsNullOrWhiteSpace($nodeIdKey)) { continue }

    $stInfo = Get-StatementInfo $op
    $stmtIdKey = if ($stInfo) { [string]$stInfo.StatementId } else { "" }

    $kids = Get-DirectChildRelOps $op
    if (-not $kids -or @($kids).Count -eq 0) { continue }

    $pkTmp = ("{0}|{1}" -f $stmtIdKey,[string]$nodeIdKey)
    $hasKidsByKey[$pkTmp] = $true

    $maxChild = $null
    foreach ($k in $kids) {
      $cid = $k.GetAttribute("NodeId")
      if ([string]::IsNullOrWhiteSpace($cid)) { continue }

      $ck = ("{0}|{1}" -f $stmtIdKey,[string]$cid)
      if ($costByKey.ContainsKey($ck)) {
        $c = [double]$costByKey[$ck]
        if ($maxChild -eq $null -or $c -gt $maxChild) { $maxChild = $c }
      }
    }

    if ($maxChild -ne $null) {
      $pk = ("{0}|{1}" -f $stmtIdKey,[string]$nodeIdKey)
      $childMaxByKey[$pk] = $maxChild
    }
  }

  foreach ($r in $operatorRows) {
    if ($r.NodeId -eq $null -or $r.EstCost -eq $null) {
      $r | Add-Member -NotePropertyName HasKids -NotePropertyValue $false -Force
      $r | Add-Member -NotePropertyName EstSelfCost -NotePropertyValue $null -Force
      continue
    }

    $pk = ("{0}|{1}" -f [string]$r.StatementId,[string]$r.NodeId)
    $tot = [double]$r.EstCost
    $childMax = $null
    if ($childMaxByKey.ContainsKey($pk)) { $childMax = [double]$childMaxByKey[$pk] }

    $hasKids = $false
    if ($hasKidsByKey.ContainsKey($pk)) { $hasKids = [bool]$hasKidsByKey[$pk] }

    $self = if ($childMax -eq $null) {
      if ($hasKids) { 0.0 } else { $tot }
    } else {
      [math]::Max(0.0,($tot - $childMax))
    }
    $r | Add-Member -NotePropertyName HasKids -NotePropertyValue $hasKids -Force
    $r | Add-Member -NotePropertyName EstSelfCost -NotePropertyValue $self -Force
  }

  # Runtime stats presence (actual vs estimated plan)
  # -----------------------------
  $hasRuntimeStats = (@($operatorRows | Where-Object { $_.ActRows -ne $null -or $_.RowsRead -ne $null -or $_.Execs -ne $null }).Count -gt 0)


  # -----------------------------
  # Parameter sensitivity / sniffing hints (from plan XML)
  # -----------------------------
  $parameterSensitivity = @()
  $hasParameterSensitivePlan = $false
  $qp = Select-Single ("//sp:QueryPlan[@ParameterSensitivePlan='true']")

  if ($qp) { $hasParameterSensitivePlan = $true }

  $paramNodes = Select-Nodes ("//sp:ParameterList/sp:ColumnReference")

  foreach ($p in $paramNodes) {
    $name = $p.GetAttribute("Column")
    if ([string]::IsNullOrWhiteSpace($name)) { $name = $p.GetAttribute("Parameter") }
    $compiled = $p.GetAttribute("ParameterCompiledValue")
    $runtime = $p.GetAttribute("ParameterRuntimeValue")
    $ptype = $p.GetAttribute("ParameterDataType")
    if ([string]::IsNullOrWhiteSpace($ptype)) { $ptype = $p.GetAttribute("ParameterType") }

    $cNum = To-DoubleOrNull $compiled
    $rNum = To-DoubleOrNull $runtime

    $ratio = $null
    if ($cNum -ne $null -and $rNum -ne $null -and $cNum -ne 0) {
      $ratio = [math]::Abs($rNum / $cNum)
      if ($ratio -lt 1) { $ratio = 1 / $ratio }
    }

    $parameterSensitivity += [pscustomobject]@{
      Name = $name
      DataType = $ptype
      CompiledValue = $compiled
      RuntimeValue = $runtime
      ValueRatio = $ratio
    }
  }

  # High-risk parameter sensitivity candidates (needs both compiled + runtime values)
  $psIssues = @($parameterSensitivity | Where-Object {
      $_.RuntimeValue -and $_.CompiledValue -and $_.ValueRatio -ne $null -and $_.ValueRatio -ge 2
    })

  # Potential multi-column stats candidates from predicates (per table)
  function Get-ColumnRefsFromExpr ([string]$expr) {
    $out = @()
    if ([string]::IsNullOrWhiteSpace($expr)) { return $out }
    $re = [regex]'\[(?<db>[^\]]+)\]\.\[(?<sch>[^\]]+)\]\.\[(?<tbl>[^\]]+)\]\.\[(?<col>[^\]]+)\]'
    foreach ($m in $re.Matches($expr)) {
      $fullTable = "[{0}].[{1}].[{2}]" -f $m.Groups["db"].Value,$m.Groups["sch"].Value,$m.Groups["tbl"].Value
      $col = "[{0}]" -f $m.Groups["col"].Value
      $out += [pscustomobject]@{ FullTable = $fullTable; Column = $col }
    }
    return $out
  }

  $multiColPredicateCandidates = @()
  foreach ($op in $operatorRows) {
    $expr = $op.SeekPredicate
    if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $op.Predicate }
    if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $op.Residual }

    $refs = Get-ColumnRefsFromExpr $expr
    if ($refs.Count -eq 0) { continue }

    foreach ($g in ($refs | Group-Object FullTable)) {
      $cols = @($g.Group.Column | Select-Object -Unique)
      if ($cols.Count -ge 2) {
        $multiColPredicateCandidates += [pscustomobject]@{
          FullTable = $g.Name
          Columns = $cols
          ColumnsText = ($cols -join ", ")
          NodeId = $op.NodeId
          StatementId = $op.StatementId
          SamplePredicate = if ($expr.Length -gt 220) { $expr.Substring(0,220) + "..." } else { $expr }
        }
      }
    }
  }
  # -----------------------------
  # Views
  # -----------------------------
  $topOps = $operatorRows | Where-Object { $_.EstCost -ne $null } | Sort-Object EstCost -Descending | Select-Object -First $TopOperators

  $topSelfOps = $operatorRows | Where-Object { $_.EstSelfCost -ne $null } | Sort-Object EstSelfCost -Descending | Select-Object -First $TopOperators
  $readsHeavyOps = $operatorRows | Where-Object { $_.RowsRead -ne $null -and $_.RowsRead -gt 0 } | Sort-Object RowsRead -Descending | Select-Object -First $TopOperators

  $keyLookups = $operatorRows |
  Where-Object { $_.PhysicalOp -match "Key Lookup|RID Lookup" } |
  ForEach-Object {
    $calls = $null
    if ($_.ActRows -ne $null -and $_.Execs -ne $null) { $calls = $_.ActRows * $_.Execs }
    elseif ($_.EstRows -ne $null) { $calls = $_.EstRows }
    $_ | Add-Member -NotePropertyName LookupCalls -NotePropertyValue $calls -Force
    $_
  } | Sort-Object LookupCalls -Descending

  $sortOps = $operatorRows | Where-Object { $_.PhysicalOp -match "Sort" -or $_.LogicalOp -match "Sort" } | Sort-Object EstCost -Descending
  $parallelOps = $operatorRows | Where-Object { $_.PhysicalOp -match "Parallelism" }

  # -----------------------------
  # Cardinality issues (more useful + grouped summary)
  # -----------------------------
  $cardinalityIssues =
  $operatorRows |
  Where-Object {
    $_.ActRows -ne $null -and $_.EstRows -ne $null -and
    $_.Executed -ne $false -and
    $_.ActRows -gt 0 -and $_.EstRows -gt 0 -and
    $_.Ratio -ne $null -and
    $_.Ratio -ge $CEMismatchRatio -and
    ([math]::Max($_.ActRows,$_.EstRows) -ge $CEMinRows)
  } |
  ForEach-Object {
    $likely = @()
    if ($_.HasConvertImplicit) { $likely += "implicit conversion (CONVERT_IMPLICIT)" }
    if ($_.NonSargableHints) {
      $hints = $_.NonSargableHints -split ","
      foreach ($h in $hints) {
        switch ($h) {
          "leading-wildcard" { $likely += "leading-wildcard LIKE" }
          "date-fn" { $likely += "date function on column" }
          "string-fn" { $likely += "string function on column" }
          "case-fn" { $likely += "UPPER/LOWER on column" }
          default { $likely += "non-sargable ($h)" }
        }
      }
    }
    if ($_.Access -match "Scan") { $likely += "scan (predicate may not be seekable)" }
    if ($_.PhysicalOp -match "Nested Loops|Hash Match|Merge Join") { $likely += "join choice sensitive to CE" }
    if ($_.RowsRead -ne $null -and $_.ActRows -ne $null -and $_.RowsRead -gt ($_.ActRows * 10)) { $likely += "high read-to-return ratio" }
    if ($_.Rebinds -ne $null -and $_.Rebinds -gt 0) { $likely += "rebinds (possible repeated inner work)" }
    if ($_.Warnings) { $likely += "warnings ($($_.Warnings))" }

    $_ | Add-Member -NotePropertyName LikelyContributors -NotePropertyValue (($likely | Select-Object -Unique) -join "; ") -Force
    $_
  } |
  Sort-Object Severity -Descending |
  Select-Object -First 50

  $ceStatementSummary = $cardinalityIssues |
  Group-Object StatementId |
  ForEach-Object {
    $worst = $_.Group | Sort-Object Severity -Descending | Select-Object -First 1
    [pscustomobject]@{
      StatementId = $_.Name
      WorstNodeId = $worst.NodeId
      WorstRatio = $worst.Ratio
      WorstSeverity = $worst.Severity
      NodesFlagged = $_.Count
      StatementText = $worst.StatementText
    }
  } | Sort-Object WorstSeverity -Descending | Select-Object -First 10

  # -----------------------------
  # Missing index suggestions (robust XPath)
  # -----------------------------
  $missingIndexGroups = Select-Nodes "//sp:MissingIndexGroup"
  if (-not $missingIndexGroups -or @($missingIndexGroups).Count -eq 0) {
    # fallback (very defensive)
    $missingIndexGroups = Select-Nodes "//*[local-name()='MissingIndexGroup']"
  }

  $missingIndexes = @(
    foreach ($g in $missingIndexGroups) {
      $impact = To-DoubleOrNull ($g.GetAttribute("Impact"))
      $mi = $g.SelectSingleNode(".//sp:MissingIndex",$nsm)
      if (-not $mi) { continue }

      $db = $mi.GetAttribute("Database")
      $schema = $mi.GetAttribute("Schema")
      $table = $mi.GetAttribute("Table")
      $fullTable = (@($db,$schema,$table) | Where-Object { $_ -and $_ -ne "" }) -join "."

      $equalityCols = @()
      $inequalityCols = @()
      $includeCols = @()

      foreach ($cg in $mi.SelectNodes(".//sp:ColumnGroup",$nsm)) {
        $usage = $cg.GetAttribute("Usage")
        $cols = @()
        foreach ($c in $cg.SelectNodes(".//sp:Column",$nsm)) { $cols += ($c.GetAttribute("Name") -replace '^\[|\]$','') }
        switch ($usage) {
          "EQUALITY" { $equalityCols += $cols }
          "INEQUALITY" { $inequalityCols += $cols }
          "INCLUDE" { $includeCols += $cols }
        }
      }

      $keyCols = @()
      if ($equalityCols.Count -gt 0) { $keyCols += $equalityCols }
      if ($inequalityCols.Count -gt 0) { $keyCols += $inequalityCols }

      $safeTable = ($table -replace '[\[\]]','')
      $ixName = "IX_Tune_{0}_{1}" -f $safeTable,([guid]::NewGuid().ToString("N").Substring(0,8))

      $keySql = ($keyCols | ForEach-Object { "[{0}]" -f $_ }) -join ", "
      $incSql = ($includeCols | ForEach-Object { "[{0}]" -f $_ }) -join ", "

      $createSql = if ($includeCols.Count -gt 0 -and $incSql -ne "") {
        "CREATE INDEX [$ixName] ON $fullTable ($keySql) INCLUDE ($incSql);"
      } else {
        "CREATE INDEX [$ixName] ON $fullTable ($keySql);"
      }

      [pscustomobject]@{
        Impact = $impact
        FullTable = $fullTable
        EqualityCols = $equalityCols
        InequalityCols = $inequalityCols
        IncludeCols = $includeCols
        EqualityText = ($equalityCols | ForEach-Object { "[{0}]" -f $_ }) -join ", "
        InequalityText = ($inequalityCols | ForEach-Object { "[{0}]" -f $_ }) -join ", "
        IncludeText = ($includeCols | ForEach-Object { "[{0}]" -f $_ }) -join ", "
        SuggestedSql = $createSql
        Signature = "{0}|{1}|{2}|{3}" -f $fullTable,($equalityCols -join ","),($inequalityCols -join ","),($includeCols -join ",")
      }
    }
  )

  $missingIndexes = @($missingIndexes | Sort-Object Impact -Descending)

  # --- Smart Missing Index Merger ---
  # Groups recommendations by Table + Key Columns. If multiple recs have the same keys 
  # but different INCLUDEs, it merges them into a single superset index to prevent index bloat.
  $missingIndexDuplicates = @() # Kept empty so the legacy suggestions engine doesn't trip
  
  $mergedIndexes = @()
  $miGroups = $missingIndexes | Group-Object { $_.FullTable + "|" + ($_.EqualityCols -join ",") + "|" + ($_.InequalityCols -join ",") }

  foreach ($g in $miGroups) {
      $base = $g.Group[0]
      $mergedIncludes = New-Object System.Collections.Generic.HashSet[string]
      $maxImpact = 0

      foreach ($mi in $g.Group) {
          if ($mi.Impact -gt $maxImpact) { $maxImpact = $mi.Impact }
          if ($mi.IncludeCols) {
              foreach ($inc in $mi.IncludeCols) { [void]$mergedIncludes.Add($inc) }
          }
      }

      $incArray = @($mergedIncludes | Sort-Object)
      $incSql = ($incArray | ForEach-Object { "[{0}]" -f $_ }) -join ", "
      
      $safeTable = $base.FullTable.Split('.')[-1].Trim('[', ']')
      $ixName = "IX_Tune_{0}_{1}" -f $safeTable,([guid]::NewGuid().ToString("N").Substring(0,8))

      $keySql = @()
      if ($base.EqualityCols.Count -gt 0) { $keySql += ($base.EqualityCols | ForEach-Object { "[{0}]" -f $_ }) }
      if ($base.InequalityCols.Count -gt 0) { $keySql += ($base.InequalityCols | ForEach-Object { "[{0}]" -f $_ }) }
      $keySqlStr = ($keySql -join ", ")

      $createSql = if ($incArray.Count -gt 0) {
          "CREATE INDEX [$ixName] ON $($base.FullTable) ($keySqlStr) INCLUDE ($incSql);"
      } else {
          "CREATE INDEX [$ixName] ON $($base.FullTable) ($keySqlStr);"
      }

      $mergedIndexes += [pscustomobject]@{
          Impact = $maxImpact
          FullTable = $base.FullTable
          EqualityText = $base.EqualityText
          InequalityText = $base.InequalityText
          IncludeText = $incSql
          SuggestedSql = $createSql
          MergedCount = $g.Group.Count
      }
  }

  $missingIndexesUnique = $mergedIndexes | Sort-Object Impact -Descending
  $missingIndexesUniqueCount = @($missingIndexesUnique).Count


  # -----------------------------
  # DBA-focused heuristics
  # -----------------------------
  $joinChecks = @()
  foreach ($op in $operatorRows | Where-Object { $_.LogicalOp -match "Join" -or $_.PhysicalOp -match "Join|Nested Loops|Hash Match|Merge Join|Adaptive Join" }) {
    $rows = if ($op.ActRows -ne $null) { $op.ActRows } else { $op.EstRows }
    $execs = if ($op.Execs -ne $null -and $op.Execs -gt 0) { $op.Execs } else { $null }

    # Heuristic threshold: nested loops becomes suspicious when output is large.
    # 10k was noisy for OLTP-ish workloads; default to 100k to reduce false positives.
    if ($op.PhysicalOp -eq "Nested Loops" -and $rows -ne $null -and $rows -ge 100000) {
      $joinChecks += [pscustomobject]@{
        NodeId = $op.NodeId
        Join = $op.PhysicalOp
        Signal = "large join output"
        Rows = $rows
        Execs = $execs
        Object = $op.Object
        Detail = "Nested Loops with output rows $rows. If outer input is large, consider Hash/Merge join or indexing to support seeks."
      }
    }
    if ($op.PhysicalOp -eq "Merge Join" -and $sortOps.Count -gt 0) {
      $joinChecks += [pscustomobject]@{
        NodeId = $op.NodeId
        Join = $op.PhysicalOp
        Signal = "merge join + sort(s)"
        Rows = $rows
        Execs = $execs
        Object = $op.Object
        Detail = "Merge Join often wants ordered inputs. Sorts present; consider indexes on join keys to provide order and avoid sorts."
      }
    }
    if ($op.PhysicalOp -eq "Hash Match" -and $rows -ne $null -and $rows -ge 200000) {
      $joinChecks += [pscustomobject]@{
        NodeId = $op.NodeId
        Join = $op.PhysicalOp
        Signal = "large hash join"
        Rows = $rows
        Execs = $execs
        Object = $op.Object
        Detail = "Hash join with output rows $rows. Check memory grant, spills, and predicate selectivity."
      }
    }
    if ($op.PhysicalOp -eq "Adaptive Join") {
      $joinChecks += [pscustomobject]@{
        NodeId = $op.NodeId
        Join = $op.PhysicalOp
        Signal = "adaptive join"
        Rows = $rows
        Execs = $execs
        Object = $op.Object
        Detail = "Adaptive Join chosen. Ensure statistics are good; parameter sensitivity can push it to a suboptimal join type. If CE mismatches exist, the join algorithm can flip across executions."
      }
    }
  }
  $joinChecks = @($joinChecks | Sort-Object NodeId -Unique)



  # Many-to-many Merge Join detection (ShowPlan attribute)
  $manyToManyMergeSignals = @()
  try {
    $m2mMergeNodes = Select-Nodes (".//sp:RelOp[@PhysicalOp='Merge Join']//sp:Merge[@ManyToMany='true' or @ManyToMany='1']")
    foreach ($m in @($m2mMergeNodes)) {
      $rel = $m.SelectSingleNode("ancestor::sp:RelOp[1]",$nsm)
      if ($rel -ne $null) {
        $nid = [int]$rel.GetAttribute("NodeId")
        $manyToManyMergeSignals += [pscustomobject]@{
          NodeId = $nid
          Join = "Merge Join"
          Signal = "many-to-many merge join"
          Detail = "Many-to-many Merge Join detected. This can build worktables and amplify rows/memory. Consider join keys/order, indexes, and predicate selectivity."
        }
      }
    }
  } catch {}

  if (@($manyToManyMergeSignals).Count -gt 0) {
    # Add to joinChecks (avoid duplicates)
    $joinChecks += $manyToManyMergeSignals
    $joinChecks = @($joinChecks | Sort-Object NodeId -Unique)
  }

  # Spill detection (operator-level) from Warnings subtree with Quantification
  $spillSignals = @()
  try {
    $spillRelOps = Select-Nodes (".//sp:RelOp[.//sp:Warnings//*[contains(translate(local-name(),'SPILL','spill'),'spill')]]")
    foreach ($r in @($spillRelOps)) {
      $nid = [int]$r.GetAttribute("NodeId")
      $phys = $r.GetAttribute("PhysicalOp")
      if ($phys -notmatch '(?i)sort' -and $phys -notmatch '(?i)hash match') { continue }
      
      $warnNodes = $r.SelectNodes(".//sp:Warnings//*",$nsm)
      $names = @()
      $maxPages = 0
      $maxLevel = 0

      foreach ($wn in @($warnNodes)) {
        $ln = $wn.LocalName
        if ($ln -match "(?i)spill") { 
            $names += $ln 
            $p = To-DoubleOrNull ($wn.GetAttribute("SpilledPages"))
            $l = To-DoubleOrNull ($wn.GetAttribute("SpillLevel"))
            if ($p -gt $maxPages) { $maxPages = $p }
            if ($l -gt $maxLevel) { $maxLevel = $l }
        }
      }
      $names = @($names | Sort-Object -Unique)
      
      $detail = ""
      if ($maxPages -gt 0) {
          $mb = [math]::Round(($maxPages * 8) / 1024, 2)
          $detail = "Spilled $maxPages pages (~$mb MB) at Level $maxLevel. "
          
          if ($maxPages -gt 5000) {
              $detail += "SEVERE. Try OPTION (MIN_GRANT_PERCENT = 5) or higher to force a larger memory grant."
          } else {
              $detail += "Mild spill. Check if statistics are outdated causing an under-estimate."
          }
      } else {
          $detail = "Warnings: " + ($names -join ", ")
      }

      $spillSignals += [pscustomobject]@{
        NodeId = $nid
        Operator = $phys
        Signal = "spill to tempdb"
        Detail = $detail
      }
    }
  } catch {}

  # Parallelism skew detection (ActualRows imbalance across threads)
  $parallelSkewSignals = @()
  $skewScanned = 0
  try {
    $rtRelOps = Select-Nodes (".//sp:RelOp[sp:RunTimeInformation/sp:RunTimeCountersPerThread]")
    foreach ($r in @($rtRelOps)) {
      $threads = $r.SelectNodes("./sp:RunTimeInformation/sp:RunTimeCountersPerThread",$nsm)
      if ($threads -eq $null) { continue }
      $rows = @()
      foreach ($t in @($threads)) {
        $ar = $t.GetAttribute("ActualRows")
        if ($ar -ne $null -and $ar -ne "") { $rows += [double]$ar }
      }
      if ($rows.Count -lt 2) { continue }
      $skewScanned++
      $total = ($rows | Measure-Object -Sum).Sum
      if ($total -lt 5000) { continue } # ignore tiny ops
      $max = ($rows | Measure-Object -Maximum).Maximum
      $avg = $total / $rows.Count
      $ratio = if ($avg -gt 0) { $max / $avg } else { $null }
      if ($ratio -ne $null -and $ratio -ge 5) {
        $parallelSkewSignals += [pscustomobject]@{
          NodeId = [int]$r.GetAttribute("NodeId")
          Operator = $r.GetAttribute("PhysicalOp")
          Signal = "parallelism skew"
          Detail = ("Max thread rows {0}, Avg {1}, SkewRatio {2}" -f (F0 $max),(F0 $avg),(F2 $ratio))
          SkewRatio = $ratio
          TotalRows = $total
          Threads = $rows.Count
        }
      }
    }
  } catch {}

  # PlanAffectingConvert / implicit conversion signals
  $planAffectingConvertSignals = @()
  try {
    $pacs = Select-Nodes (".//sp:PlanAffectingConvert")
    foreach ($pac in @($pacs)) {
      $issue = $pac.GetAttribute("ConvertIssue")
      if ([string]::IsNullOrWhiteSpace($issue)) { $issue = $pac.GetAttribute("ConvertIssueType") }
      $expr = $pac.GetAttribute("Expression")
      if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $pac.InnerText }
      if ($expr -and $expr.Length -gt 220) { $expr = $expr.Substring(0,220) + "..." }
      $planAffectingConvertSignals += [pscustomobject]@{
        StatementId = $chosenStatementId
        Issue = $issue
        Expression = $expr
        Detail = "Plan-affecting convert detected. Check parameter/column datatypes and implicit conversions (can hurt seeks and cardinality estimation)."
      }
    }
  } catch {}

  # Operator red flags
  $operatorRedFlags = @()
  foreach ($op in $operatorRows) {
    $rows = if ($op.ActRows -ne $null) { $op.ActRows } else { $op.EstRows }

    if ($op.PhysicalOp -match "Sort" -and $rows -ne $null -and $rows -ge 100000) {
      $operatorRedFlags += [pscustomobject]@{ NodeId = $op.NodeId; Operator = $op.PhysicalOp; Signal = "large sort"; Rows = $rows; Execs = $op.Execs; Object = $op.Object; Detail = "Sort on ~$rows rows. Consider index/order, reduce rowset, or memory grant/spill review." }
    }
    if ($op.PhysicalOp -match "Spool") {
      $operatorRedFlags += [pscustomobject]@{ NodeId = $op.NodeId; Operator = $op.PhysicalOp; Signal = "spool"; Rows = $rows; Execs = $op.Execs; Object = $op.Object; Detail = "Spool can indicate repeated work. Check Execs, joins, correlated subqueries, and indexing." }
    }
    if ($op.PhysicalOp -match "Table Spool" -and $op.Execs -ne $null -and $op.Execs -ge 1000) {
      $operatorRedFlags += [pscustomobject]@{ NodeId = $op.NodeId; Operator = $op.PhysicalOp; Signal = "high rebinds"; Rows = $rows; Execs = $op.Execs; Object = $op.Object; Detail = "Table Spool with Execs=$($op.Execs). Investigate nested loops rebinds / parameter sensitivity." }
    }
    if ($op.PhysicalOp -match "Hash Match" -and $rows -ne $null -and $rows -ge 200000) {
      $operatorRedFlags += [pscustomobject]@{ NodeId = $op.NodeId; Operator = $op.PhysicalOp; Signal = "large hash"; Rows = $rows; Execs = $op.Execs; Object = $op.Object; Detail = "Hash operator on ~$rows rows. Check memory grant and spills/tempdb." }
    }
    if ($op.PhysicalOp -match "Bitmap") {
      $operatorRedFlags += [pscustomobject]@{ NodeId = $op.NodeId; Operator = $op.PhysicalOp; Signal = "bitmap / index intersection"; Rows = $rows; Execs = $op.Execs; Object = $op.Object; Detail = "Bitmap usage can indicate index intersection. A composite index may be better." }
    }
  }
  $operatorRedFlags = @($operatorRedFlags | Sort-Object NodeId)

  # Predicate SARGability checks (heuristic)
  $rawSargIssues = @()
  
  function Add-SargIssue ([int]$nodeId,[string]$kind,[string]$expr) {
    if ([string]::IsNullOrWhiteSpace($expr)) { return }
    $short = if ($expr.Length -gt 220) { $expr.Substring(0,220) + "..." } else { $expr }
    $cleanKind = $kind -replace "\s*\(non-SARGable\)\s*","" -replace "\s*\(often non-SARGable\)\s*","" -replace "\s*\(non-SARGable likely\)\s*",""
    $rawSargIssues += [pscustomobject]@{ NodeId = $nodeId; Issue = $cleanKind; Expression = $short }
  }

  foreach ($op in $operatorRows) {
    foreach ($field in @("SeekPredicate","Predicate","Residual")) {
      $expr = $op.$field
      if ([string]::IsNullOrWhiteSpace($expr)) { continue }

      if ($expr -match "CONVERT_IMPLICIT") { Add-SargIssue $op.NodeId "implicit conversion" $expr }
      if ($expr -match "(?i)\bLIKE\s+N?'%[^']*'") { Add-SargIssue $op.NodeId "leading wildcard LIKE" $expr }
      if ($expr -match "(?i)\b(UPPER|LOWER|SUBSTRING|LEFT|RIGHT|DATEADD|DATEDIFF|DATEPART|DATENAME|YEAR|MONTH|DAY|TRY_CONVERT|TRY_CAST|CONVERT|CAST)\s*\(") { Add-SargIssue $op.NodeId "function on column" $expr }
    }
  }

  $scalarOps = Select-Nodes ("//sp:ScalarOperator[@ScalarString]")
  $searchParam = @($parameterSensitivity | Where-Object { $_.Name -match "@Search" } | Select-Object -First 1)
  $searchVal = $null
  if ($searchParam) { $searchVal = if (-not [string]::IsNullOrWhiteSpace($searchParam.RuntimeValue)) { $searchParam.RuntimeValue } else { $searchParam.CompiledValue } }
  $searchLeadingWildcard = $false
  if ($searchVal -and ($searchVal -match "(?i)N?'%")) { $searchLeadingWildcard = $true }

  foreach ($soNode in $scalarOps) {
    $expr = $soNode.GetAttribute("ScalarString")
    if ([string]::IsNullOrWhiteSpace($expr)) { continue }

    $rel = $soNode.SelectSingleNode("ancestor::sp:RelOp[1]",$nsm)
    $nid = if ($rel) { To-IntOrNull ($rel.GetAttribute("NodeId")) } else { 0 }
    if ($nid -eq $null) { $nid = 0 }

    if ($expr -match "CONVERT_IMPLICIT") { Add-SargIssue $nid "implicit conversion" $expr }

    if (($expr -match "(?i)\blike\s*\[?@Search\]?") -and $searchLeadingWildcard) {
      Add-SargIssue $nid "leading wildcard LIKE via @Search" ($expr + "  -- @Search=" + $searchVal)
    }

    if ($expr -match "(?i)\bLIKE\s+N?'%[^']*'") {
      Add-SargIssue $nid "leading wildcard LIKE" $expr
    }

    $hasColRef = ($expr -match "\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\]" -or $expr -match "\[(?!@)[^\]]+\]\.\[[^\]]+\]")
    if ($hasColRef -and ($expr -match "(?i)\b(UPPER|LOWER|SUBSTRING|LEFT|RIGHT|DATEADD|DATEDIFF|DATEPART|DATENAME|YEAR|MONTH|DAY|TRY_CONVERT|TRY_CAST|CONVERT|CAST|ISNULL|COALESCE)\s*\(")) {
      if ($expr -match "(?i)\b(CONVERT|CAST)\s*\([^)]*(\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\]|\[(?!@)[^\]]+\]\.\[[^\]]+\])(\s+as\s+\[(?!@)[^\]]+\]\.\[[^\]]+\])?") {
        Add-SargIssue $nid "convert/cast on column" $expr
      } else {
        Add-SargIssue $nid "function on column" $expr
      }
    }
  }

  foreach ($op in $operatorRows) {
    if (-not [string]::IsNullOrWhiteSpace($op.NonSargableHints)) {
      $hints = $op.NonSargableHints -split ","
      foreach ($h in $hints) {
        $h = $h.Trim()
        if (-not [string]::IsNullOrWhiteSpace($h)) {
          $expr = $op.Predicate
          if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $op.Residual }
          if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $op.SeekPredicate }
          if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $op.JoinPredicate }

          # Prevent blank expressions from bubbling
          if ([string]::IsNullOrWhiteSpace($expr)) { continue }

          $short = if ($expr -and $expr.Length -gt 220) { $expr.Substring(0,220) + "..." } else { $expr }

          $issue = switch ($h) {
            "leading-wildcard" { "leading wildcard LIKE" }
            "date-fn" { "date function on column" }
            "string-fn" { "string function on column" }
            "case-fn" { "UPPER/LOWER on column" }
            "isnull()" { "ISNULL in WHERE clause" }
            "abs()" { "ABS() on column" }
            "try_convert()" { "TRY_CONVERT on column" }
            "try_cast()" { "TRY_CAST on column" }
            "convert()" { "CONVERT on column" }
            "convert_implicit" { "implicit conversion" }
            default { "non-sargable ($h)" }
          }
          
          $rawSargIssues += [pscustomobject]@{ NodeId = $op.NodeId; Issue = $issue; Expression = $short }
        }
      }
    }
  }

  # Smart Deduplication: Group by Expression to eliminate bubbling and combine overlapping issues
  $sargMerged = @()
  foreach ($g in ($rawSargIssues | Group-Object Expression)) {
      $nodes = ($g.Group | Select-Object -ExpandProperty NodeId | Sort-Object -Unique) -join ", "
      $issues = ($g.Group | Select-Object -ExpandProperty Issue | Sort-Object -Unique) -join "; "
      $sargMerged += [pscustomobject]@{
          NodeIds = $nodes
          Issues = $issues
          Expression = $g.Name
      }
  }
  $sargabilityIssues = $sargMerged | Sort-Object NodeIds

  # Index intersection signals (heuristic):
  $indexIntersectionSignals = @()
  foreach ($op in $operatorRows | Where-Object { $_.PhysicalOp -match "Bitmap" -or $_.LogicalOp -match "Bitmap" }) {
    $indexIntersectionSignals += [pscustomobject]@{ NodeId = $op.NodeId; Operator = $op.PhysicalOp; Detail = "Bitmap present; may indicate index intersection. Consider a composite index that matches predicates/join keys." }
  }
  # Also detect bitmap filters embedded under other operators (e.g., Hash Match with Bitmap)
  $bitmapRelOps = Select-Nodes ("//sp:Bitmap/ancestor::sp:RelOp[1]")
  foreach ($r in $bitmapRelOps) {
    $nid = To-IntOrNull ($r.GetAttribute("NodeId"))
    if ($nid -eq $null) { continue }
    if (-not ($indexIntersectionSignals | Where-Object { $_.NodeId -eq $nid })) {
      $opName = $r.GetAttribute("PhysicalOp")
      if ([string]::IsNullOrWhiteSpace($opName)) { $opName = $r.GetAttribute("LogicalOp") }
      $indexIntersectionSignals += [pscustomobject]@{ NodeId = $nid; Operator = $opName; Detail = "Bitmap filter detected under this operator; may indicate index intersection/bitmap filtering." }
    }
  }

  # Also detect bitmap filtering expressed as PROBE(Opt_Bitmap...) in ScalarString (common with Hash Match)
  $probeNodes = Select-Nodes ("//sp:ScalarOperator[@ScalarString]")
  foreach ($pNode in $probeNodes) {
    $expr = $pNode.GetAttribute("ScalarString")
    if ([string]::IsNullOrWhiteSpace($expr)) { continue }
    if ($expr -match "(?i)\bPROBE\s*\(" -or $expr -match "Opt_Bitmap") {
      $rel = $pNode.SelectSingleNode("ancestor::sp:RelOp[1]",$nsm)
      $nid = $null
      if ($rel) { $nid = To-IntOrNull ($rel.GetAttribute("NodeId")) }
      if ($nid -eq $null) { $nid = 0 }
      if (-not ($indexIntersectionSignals | Where-Object { $_.NodeId -eq $nid })) {
        $indexIntersectionSignals += [pscustomobject]@{ NodeId = $nid; Operator = "(scalar predicate)"; Detail = "PROBE()/Opt_Bitmap detected in predicate; indicates bitmap filtering / index intersection behavior." }
      }
    }
  }



  # Rewrite hints (DBA-friendly, but still heuristic)
  $rewriteHints = New-Object System.Collections.Generic.List[string]
  if ($operatorRows | Where-Object { $_.PhysicalOp -match "TopN Sort" }) { $rewriteHints.Add("TopN Sort detected. Consider an index that matches the ORDER BY (and filters) to avoid sorting.") }
  if ($operatorRows | Where-Object { $_.PhysicalOp -match "Window Spool|Sequence Project|Segment" }) { $rewriteHints.Add("Windowing pattern detected (Segment/Sequence Project/Spool). Consider indexing to support ORDER BY / PARTITION BY, or rewrite to reduce windowed rows.") }
  if ($operatorRows | Where-Object { $_.PhysicalOp -match "Table Spool|Index Spool" }) { $rewriteHints.Add("Spool detected. Often caused by nested loops rebinds/correlation. Consider rewriting correlated subqueries, adding supporting indexes, or forcing a better join strategy.") }
  if ($sargabilityIssues.Count -gt 0) { $rewriteHints.Add("Non-SARGable predicate signals found. Fix datatype mismatches, avoid functions on filtered columns, and avoid leading-wildcard LIKE when possible.") }
  if (@($indexIntersectionSignals).Count -gt 0) { $rewriteHints.Add("Bitmap/index-intersection signals found. A composite index may outperform intersection of multiple single-column indexes.") }
  if ($psIssues.Count -gt 0) { $rewriteHints.Add("Parameter sensitivity signals found. Consider (RECOMPILE), OPTIMIZE FOR, or alternative query shapes to reduce plan variance.") }

  # If we saw large sorts/hashes AND missing-index recommendations exist, tie them together for DBAs
  $largeSortFlag = @($operatorRedFlags | Where-Object { $_.Operator -match "^Sort$" -and $_.Signal -match "large sort" }).Count -gt 0
  if ($largeSortFlag -and @($missingIndexes).Count -gt 0) {
    $topMi = @($missingIndexes | Sort-Object Impact -Descending | Select-Object -First 2)
    $miSummary = ($topMi | ForEach-Object {
        $keys = @()
        if (-not [string]::IsNullOrWhiteSpace($_.EqualityText)) { $keys += $_.EqualityText }
        if (-not [string]::IsNullOrWhiteSpace($_.InequalityText)) { $keys += $_.InequalityText }
        $inc = if (-not [string]::IsNullOrWhiteSpace($_.IncludeText)) { " INCLUDE (" + $_.IncludeText + ")" } else { "" }
        ("{0} ON ({1}){2}" -f $_.FullTable,(($keys -join ", ") -replace "\s+"," "),$inc)
      }) -join "; "
    $rewriteHints.Add("Large sorts detected. Review missing-index recommendations that may eliminate sort/scan. Top candidates: $miSummary")
  }

  # -----------------------------
  # Suggestions
  # -----------------------------
  $suggestions = New-Object System.Collections.Generic.List[string]
  if (@($missingIndexes).Count -gt 0) {
    if (@($missingIndexes).Count -gt 1) { $suggestions.Add("Multiple missing-index suggestions found in XML ($(@($missingIndexes).Count)). Validate + de-dupe/merge before creating anything.") }
    else { $suggestions.Add("Missing index suggestion found in plan XML. Validate against workload + existing indexes (SSMS can hide/show inconsistently).") }
  }
  if (@($missingIndexDuplicates).Count -gt 0) { $suggestions.Add("Duplicate missing-index signatures found. Dedupe/merge before creating indexes.") }
  if ($cardinalityIssues.Count -gt 0) { $suggestions.Add("Cardinality mismatches found. Focus on highest Severity first; check stats, datatypes, predicates, and parameter sniffing patterns.") }
  if ($keyLookups.Count -gt 0) {
    $painful = $keyLookups | Where-Object { $_.LookupCalls -ne $null -and $_.LookupCalls -ge $LookupCallsThreshold }
    if ($painful.Count -gt 0) { $suggestions.Add("High-volume lookups detected (calls above threshold). Consider covering indexes or reducing selected columns.") }
    else { $suggestions.Add("Lookups detected. Review LookupCalls before changing indexes.") }
  }
  if ($sortOps.Count -gt 0) { $suggestions.Add("Sort operators present. Review SortKeys and ORDER BY; consider indexes/persisted computed columns, and memory grant/spills.") }
  if ($parallelOps.Count -gt 0) { $suggestions.Add("Parallelism present. If unstable, check skew/CXPACKET/CXCONSUMER, MAXDOP, and cost threshold.") }
  if ($memoryGrantInfo -and ($null -ne $memoryGrantInfo.RequestedKB -or $null -ne $memoryGrantInfo.GrantedKB -or $null -ne $memoryGrantInfo.UsedKB -or $null -ne $memoryGrantInfo.MaxUsedKB -or $null -ne $memoryGrantInfo.GrantWaitMS)) { $suggestions.Add("Memory grant info present. Compare Granted vs Used/MaxUsed to spot spills or wasted grants.") }
  if (@($spillSignals).Count -gt 0) { $suggestions.Add("Spills detected in plan warnings. Investigate Sort/Hash spills, memory grants, and tempdb pressure.") }
  if (@($parallelSkewSignals).Count -gt 0) { $suggestions.Add("Parallelism skew detected (thread row imbalance). Investigate data distribution, join keys, and exchanges; consider different join/order or filtered/composite indexes.") }
  if (@($manyToManyMergeSignals).Count -gt 0) { $suggestions.Add("Many-to-many Merge Join detected. Check join predicates and indexing; this can amplify work via worktables and large memory use.") }
  if (@($planAffectingConvertSignals).Count -gt 0) { $suggestions.Add("Plan-affecting implicit conversions detected. Align datatypes (parameters/columns) to avoid conversion, CE issues, and non-seekable predicates.") }
  if (-not $hasRuntimeStats) { $suggestions.Add("Plan appears to lack runtime statistics (ActualRows/RowsRead/Execs). If possible capture an actual execution plan for better diagnostics.") }
  if ($spillLikely.Count -gt 0) { $suggestions.Add("Spill markers detected in plan XML. Investigate memory grants, hash/sort inputs, and tempdb.") }
  if ($hasParameterSensitivePlan) { $suggestions.Add("Parameter Sensitive Plan optimization detected. If performance varies by parameter, review PSP behavior and plan variants.") }
  if ($psIssues.Count -gt 0) { $suggestions.Add("Parameter sensitivity signals found (compiled vs runtime values differ). Consider parameter sniffing mitigations (OPTION(RECOMPILE), OPTIMIZE FOR, plan guides, or rewriting).") }
  if (@($joinChecks).Count -gt 0) { $suggestions.Add("Join strategy checks flagged potential issues. Review join types, input sizes, and supporting indexes.") }
  if (@($operatorRedFlags).Count -gt 0) { $suggestions.Add("Operator red flags detected (heuristic). Review spools/sorts/hashes and rebind patterns.") }
  if (@($sargabilityIssues).Count -gt 0) { $suggestions.Add("Non-SARGable predicate signals detected. Review implicit conversions, functions on columns, and leading-wildcard LIKE patterns.") }
  if (@($indexIntersectionSignals).Count -gt 0) { $suggestions.Add("Bitmap/index-intersection signals detected. Consider composite indexes that match predicates/join keys.") }

  $predicateConvertCount = @($operatorRows | Where-Object { $_.HasConvertImplicit }).Count
  if ($predicateConvertCount -gt 0) {
    $suggestions.Add("Predicate implicit conversion(s) detected (CONVERT_IMPLICIT). These can prevent seeks and skew cardinality; align datatypes (parameters/columns) where possible.")
  }
  if (@($expressionConvertImplicit).Count -gt 0) {
    $suggestions.Add("Expression implicit conversions detected in Compute Scalar. Usually not a seek blocker, but can add CPU or indicate upstream datatype mismatches.")
  }

  # -----------------------------
  # Optional: InspectDatabase (indexes/usage)
  # -----------------------------
  $dbInspection = $null
  if ($InspectDatabase) {
    if ([string]::IsNullOrWhiteSpace($ServerInstance) -or [string]::IsNullOrWhiteSpace($Database)) {
      throw "When using -InspectDatabase, you must supply -ServerInstance and -Database."
    }

    # --- UPDATED: Connection via dbatools ---
    $connectParams = @{
        SqlInstance            = $ServerInstance
        Database               = $Database
        TrustServerCertificate = $true
        ErrorAction            = 'Stop'
    }

    # dbatools uses SqlCredential for SQL Authentication
    if ($SqlCredential) {
        $connectParams.SqlCredential = $SqlCredential
    }

    # Establish the SMO connection
    $smoServer = Connect-DbaInstance @connectParams

    # Extract the underlying .NET SqlConnection so the rest of your DataReader logic works unmodified
    $conn = $smoServer.ConnectionContext.SqlConnectionObject
    if ($conn.State -ne 'Open') { 
        $conn.Open() 
    }    try {
      # Determine target tables: from missing index recs + top expensive ops
      $targetTables = New-Object System.Collections.Generic.HashSet[string]
      foreach ($mi in $missingIndexes) {
        if ($mi.FullTable) { [void]$targetTables.Add($mi.FullTable) }
      }
      foreach ($op in ($topOps | Select-Object -First 25)) {
        if ($op.Object) {
          # object strings can be "db.schema.table (ix) | db.schema.table (ix)"
          $parts = $op.Object -split "\s+\|\s+"
          foreach ($p in $parts) {
            # strip "(index)" suffix
            $base = ($p -replace "\s+\(.*\)$","").Trim()
            if ($base -match "^\[.*\]\.\[.*\]\.\[.*\]$") { [void]$targetTables.Add($base) }
          }
        }
      }

      $tablesList = @($targetTables)
      $tableRows = @()

      if ($tablesList.Count -gt 0) {
        # Pass tables via table variable (avoid dynamic SQL & quoting headaches)
        $cmd = $conn.CreateCommand()
        $cmd.CommandTimeout = 120
        $cmd.CommandTimeout = 120

        $cmd.CommandText = @"
DECLARE @t TABLE (FullTable sysname NOT NULL);
-- filled by client

;WITH tgt AS (
  SELECT FullTable,
         DbName   = PARSENAME(FullTable,3),
         SchName  = PARSENAME(FullTable,2),
         ObjName  = PARSENAME(FullTable,1)
  FROM @t
),
obj AS (
  SELECT t.FullTable, o.object_id, s.name AS SchemaName, o.name AS TableName
  FROM tgt t
  JOIN sys.schemas s ON s.name = REPLACE(REPLACE(t.SchName,'[',''),']','')
  JOIN sys.objects o ON o.schema_id = s.schema_id AND o.name = REPLACE(REPLACE(t.ObjName,'[',''),']','')
  WHERE o.type = 'U'
)
SELECT
  o.FullTable,
  o.SchemaName,
  o.TableName,
  i.name  AS IndexName,
  i.index_id,
  i.type_desc,
  i.is_unique,
  i.is_primary_key,
  i.has_filter,
  i.filter_definition,

  kc.KeyCols,
  ic.IncludeCols,

  -- basic usage (optional; may be NULL if never used since last restart)
  us.user_seeks,
  us.user_scans,
  us.user_lookups,
  us.user_updates

FROM obj o
JOIN sys.indexes i
  ON i.object_id = o.object_id
 AND i.index_id > 0

OUTER APPLY
(
    SELECT
        KeyCols = STUFF((
            SELECT
                ', ' + QUOTENAME(c.name) + CASE WHEN ix.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
            FROM sys.index_columns AS ix
            JOIN sys.columns       AS c
              ON c.object_id = ix.object_id
             AND c.column_id = ix.column_id
            WHERE ix.object_id = i.object_id
              AND ix.index_id  = i.index_id
              AND ix.is_included_column = 0
              AND ix.key_ordinal > 0
            ORDER BY ix.key_ordinal
            FOR XML PATH(''), TYPE
        ).value('.','nvarchar(max)'), 1, 2, '')
) AS kc

OUTER APPLY
(
    SELECT
        IncludeCols = STUFF((
            SELECT
                ', ' + QUOTENAME(c.name)
            FROM sys.index_columns AS ix
            JOIN sys.columns       AS c
              ON c.object_id = ix.object_id
             AND c.column_id = ix.column_id
            WHERE ix.object_id = i.object_id
              AND ix.index_id  = i.index_id
              AND ix.is_included_column = 1
            ORDER BY ix.index_column_id
            FOR XML PATH(''), TYPE
        ).value('.','nvarchar(max)'), 1, 2, '')
) AS ic

LEFT JOIN sys.dm_db_index_usage_stats us
  ON us.database_id = DB_ID()
 AND us.object_id   = i.object_id
 AND us.index_id    = i.index_id

ORDER BY o.FullTable, i.index_id;
"@


        # Fill @t with parameters by sending a batch that inserts values then runs the query
        # Build client-side inserts safely using parameters
        $insertSql = "DECLARE @t TABLE (FullTable sysname NOT NULL);" + "`n"
        $pIndex = 0
        foreach ($t in $tablesList) {
          $pn = "@p$pIndex"
          $insertSql += "INSERT INTO @t(FullTable) VALUES ($pn);" + "`n"
          $p = $cmd.Parameters.Add($pn,[System.Data.SqlDbType]::NVarChar,512)
          $p.Value = $t
          $pIndex++
        }
        $cmd.CommandText = $insertSql + "`n" + ($cmd.CommandText -replace "DECLARE @t TABLE \(FullTable sysname NOT NULL\);\s*-- filled by client","")

        $r = $cmd.ExecuteReader()
        while ($r.Read()) {
          $tableRows += [pscustomobject]@{
            FullTable = $r["FullTable"]
            IndexName = $r["IndexName"]
            IndexId = $r["index_id"]
            TypeDesc = $r["type_desc"]
            IsUnique = $r["is_unique"]
            IsPrimaryKey = $r["is_primary_key"]
            HasFilter = $r["has_filter"]
            Filter = $r["filter_definition"]
            KeyCols = $r["KeyCols"]
            IncludeCols = $r["IncludeCols"]
            UserSeeks = $r["user_seeks"]
            UserScans = $r["user_scans"]
            UserLookups = $r["user_lookups"]
            UserUpdates = $r["user_updates"]
          }
        }
        $r.Close()
      }


      # -----------------------------
      # Stats inspection (staleness + definition) for target tables
      # -----------------------------
      $statsRows = @()
      $staleStats = @()
      $multiColStatsOpportunities = @()

      if ($tablesList.Count -gt 0) {
        $cmdStats = $conn.CreateCommand()
        $cmdStats.CommandTimeout = 120

        $cmdStats.CommandText = @"
DECLARE @t TABLE (FullTable sysname NOT NULL);
-- filled by client

;WITH tgt AS (
  SELECT FullTable,
         SchName  = PARSENAME(FullTable,2),
         ObjName  = PARSENAME(FullTable,1)
  FROM @t
),
obj AS (
  SELECT t.FullTable, o.object_id, s.name AS SchemaName, o.name AS TableName
  FROM tgt t
  JOIN sys.schemas s ON s.name = REPLACE(REPLACE(t.SchName,'[',''),']','')
  JOIN sys.objects o ON o.schema_id = s.schema_id AND o.name = REPLACE(REPLACE(t.ObjName,'[',''),']','')
  WHERE o.type = 'U'
)
SELECT
  o.FullTable,
  st.name AS StatsName,
  st.stats_id,
  st.auto_created,
  st.user_created,
  st.no_recompute,
  sp.last_updated,
  sp.[rows],
  sp.rows_sampled,
  sp.modification_counter,
  StatsCols =
    STUFF((
      SELECT ',' + QUOTENAME(c.name)
      FROM sys.stats_columns sc
      JOIN sys.columns c ON c.object_id = sc.object_id AND c.column_id = sc.column_id
      WHERE sc.object_id = st.object_id AND sc.stats_id = st.stats_id
      ORDER BY sc.stats_column_id
      FOR XML PATH(''), TYPE).value('.','nvarchar(max)')
    ,1,1,'')
FROM obj o
JOIN sys.stats st ON st.object_id = o.object_id
OUTER APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
ORDER BY o.FullTable, st.name;
"@

        # Fill @t (reuse same tables list)
        $insertSql2 = "DECLARE @t TABLE (FullTable sysname NOT NULL);" + "`n"
        $pIndex2 = 0
        foreach ($t in $tablesList) {
          $pn = "@p$pIndex2"
          $insertSql2 += "INSERT INTO @t(FullTable) VALUES ($pn);" + "`n"
          $p = $cmdStats.Parameters.Add($pn,[System.Data.SqlDbType]::NVarChar,512)
          $p.Value = $t
          $pIndex2++
        }
        $cmdStats.CommandText = $insertSql2 + "`n" + ($cmdStats.CommandText -replace "DECLARE @t TABLE \(FullTable sysname NOT NULL\);\s*-- filled by client","")

        $r2 = $cmdStats.ExecuteReader()
        while ($r2.Read()) {
          $statsRows += [pscustomobject]@{
            FullTable = $r2["FullTable"]
            StatsName = $r2["StatsName"]
            StatsId = $r2["stats_id"]
            AutoCreated = [bool]$r2["auto_created"]
            UserCreated = [bool]$r2["user_created"]
            NoRecompute = [bool]$r2["no_recompute"]
            LastUpdated = $r2["last_updated"]
            Rows = $r2["rows"]
            RowsSampled = $r2["rows_sampled"]
            ModificationCounter = $r2["modification_counter"]
            StatsCols = $r2["StatsCols"]
          }
        }
        $r2.Close()

        # Heuristic stale-stats detection
        foreach ($s in $statsRows) {
          $rows = To-DoubleOrNull ($s.Rows)
          $mods = To-DoubleOrNull ($s.ModificationCounter)
          $days = $null
          if ($s.LastUpdated -and $s.LastUpdated -is [datetime]) {
            $days = (New-TimeSpan -Start $s.LastUpdated -End (Get-Date)).TotalDays
          }

          $ratio = $null
          if ($rows -ne $null -and $rows -gt 0 -and $mods -ne $null) { $ratio = $mods / $rows }

          $isStale =
          (($mods -ne $null -and $mods -ge 500 -and $ratio -ne $null -and $ratio -ge 0.20) -or
            ($days -ne $null -and $days -ge 30 -and $mods -ne $null -and $mods -ge 1))

          if ($isStale) {
            $staleStats += [pscustomobject]@{
              FullTable = $s.FullTable
              StatsName = $s.StatsName
              StatsCols = $s.StatsCols
              LastUpdated = $s.LastUpdated
              Rows = $s.Rows
              ModificationCounter = $s.ModificationCounter
              ModRatio = $ratio
              DaysSinceUpdate = $days
            }
          }
        }

        # Multi-column stats opportunities (from predicate candidates) vs existing stats
        $existingStatsByTable = @{}
        foreach ($g in ($statsRows | Group-Object FullTable)) {
          $existingStatsByTable[$g.Name] = @($g.Group | ForEach-Object { $_.StatsCols })
        }

        foreach ($cand in ($multiColPredicateCandidates | Group-Object { $_.FullTable + "|" + $_.ColumnsText })) {
          $one = $cand.Group | Select-Object -First 1
          $tbl = $one.FullTable
          $need = $one.Columns

          $covered = $false
          if ($existingStatsByTable.ContainsKey($tbl)) {
            foreach ($colsText in $existingStatsByTable[$tbl]) {
              if ([string]::IsNullOrWhiteSpace($colsText)) { continue }
              $existingCols = $colsText -split "," | ForEach-Object { $_.Trim() }
              if ($existingCols.Count -ge $need.Count) {
                $prefix = $existingCols[0..($need.Count - 1)]
                if ((($prefix -join ",") -eq ($need -join ","))) { $covered = $true; break }
              }
            }
          }

          if (-not $covered) {
            $multiColStatsOpportunities += [pscustomobject]@{
              FullTable = $tbl
              Columns = ($need -join ", ")
              ExamplePredicate = $one.SamplePredicate
              ExampleNodeId = $one.NodeId
              StatementId = $one.StatementId
            }
          }
        }
      }


      if ($staleStats.Count -gt 0) {
        $suggestions.Add("Stale statistics candidates found on target tables. Consider updating stats before making plan/index changes.")
      }
      if ($multiColStatsOpportunities.Count -gt 0) {
        $suggestions.Add("Multi-column stats opportunities found (predicate column combos not covered by existing stats). Consider CREATE STATISTICS to improve CE.")
      }

      # Compare missing-index recs to existing indexes (simple overlap heuristic)
      $miCoverage = @()

      if (@($missingIndexes).Count -gt 0) {
        $miSigCounts = @{}
        foreach ($g in (@($missingIndexes) | Group-Object Signature)) {
          $miSigCounts[$g.Name] = $g.Count
        }

        $miSigSeen = @{}

        foreach ($mi in @($missingIndexes)) {
          $tbl = $mi.FullTable
          if (-not $tbl) { continue }

          $existing = $tableRows | Where-Object { $_.FullTable -eq $tbl }
          $needKeys = @()
          if ($mi.EqualityCols) { $needKeys += $mi.EqualityCols }
          if ($mi.InequalityCols) { $needKeys += $mi.InequalityCols }
          $needInc = @()
          if ($mi.IncludeCols) { $needInc += $mi.IncludeCols }

          $coveredBy = @()
          foreach ($ix in $existing) {
            $ixKeys = @()
            if ($ix.KeyCols) {
              # strip ASC/DESC and brackets
              $ixKeys = ($ix.KeyCols -split ",") | ForEach-Object { ($_ -replace "\s+(ASC|DESC)\s*$","").Trim() }
            }
            $ixInc = @()
            if ($ix.IncludeCols) { $ixInc = ($ix.IncludeCols -split ",") | ForEach-Object { $_.Trim() } }

            # naive check: needKeys is prefix of ixKeys (best case) OR subset (good enough)
            $needKeysQ = $needKeys | ForEach-Object { "[{0}]" -f $_ }
            $needIncQ = $needInc | ForEach-Object { "[{0}]" -f $_ }

            $keysSubset = $true
            foreach ($k in $needKeysQ) { if ($ixKeys -notcontains $k) { $keysSubset = $false; break } }

            $incSubset = $true
            foreach ($c in $needIncQ) { if ($ixInc -notcontains $c -and $ixKeys -notcontains $c) { $incSubset = $false; break } }

            if ($keysSubset -and $incSubset) { $coveredBy += $ix.IndexName }
          }

          $occ = 1
          if ($mi.Signature -and $miSigCounts.ContainsKey($mi.Signature)) { $occ = $miSigCounts[$mi.Signature] }

          if (-not $miSigSeen.ContainsKey($mi.Signature)) { $miSigSeen[$mi.Signature] = 0 }
          $miSigSeen[$mi.Signature]++
          $ord = $miSigSeen[$mi.Signature]
          $dupLabel = if ($occ -gt 1) { "dup #$ord" } else { "" }

          $miCoverage += [pscustomobject]@{
            FullTable = $tbl
            Impact = $mi.Impact
            Occurrences = $occ
            Duplicate = $dupLabel
            SuggestedKeys = (($needKeys | ForEach-Object { "[{0}]" -f $_ }) -join ", ")
            SuggestedIncludes = (($needInc | ForEach-Object { "[{0}]" -f $_ }) -join ", ")
            CoveredByExistingIndex = if ($coveredBy.Count -gt 0) { $coveredBy -join ", " } else { "" }
          }

        }

      }
      $dbInspection = [pscustomobject]@{
        ServerInstance = $ServerInstance
        Database = $Database
        TargetTables = $tablesList
        Indexes = $tableRows
        Stats = $statsRows
        StaleStats = $staleStats
        MultiColumnStatsOpportunities = $multiColStatsOpportunities
        MissingIndexCoverage = $miCoverage
      }
    }
    finally {
      $conn.Close()
      $conn.Dispose()
    }
  }

  # -----------------------------
  # Finalize sort/index suggestions now that we may have database metadata
  # -----------------------------
  if ($sortOrderNeeds -and $sortOrderNeeds.Count -gt 0) {
    foreach ($need in $sortOrderNeeds) {
      $k = $need.FullTable
      $cols = @($need.Columns)
      if (-not $k -or $cols.Count -eq 0) { continue }

      $coveringIndexNames = @()
      if ($InspectDatabase -and $dbInspection -and $dbInspection.Indexes) {
        $idxs = @($dbInspection.Indexes | Where-Object { $_.FullTable -eq $k })
        foreach ($ix in $idxs) {
          if (Index-KeyPrefixMatches $ix.KeyCols $cols) {
            $coveringIndexNames += $ix.IndexName
          }
        }
      }

      # Normalize/display covering indexes
      $coveringIndexNames = @($coveringIndexNames | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
      $covered = ($coveringIndexNames.Count -gt 0)
      $coveringIndexDisplay =
        if (-not $covered) { '(none)' }
        elseif ($coveringIndexNames.Count -le 3) { ($coveringIndexNames -join ', ') }
        else { ( ($coveringIndexNames | Select-Object -First 3) -join ', ' ) + (' (+' + ($coveringIndexNames.Count - 3) + ' more)') }

      $suggestedIndexSql = $null

      # Build a richer display of Sort NodeId(s) with cost/rows to make the impact obvious.
      $sortNodeDetailsDisplay = ''
      if ($need.SortNodeIds -and @($need.SortNodeIds).Count -gt 0) {
        $parts = @()
        foreach ($nid in @($need.SortNodeIds)) {
          $op = $null
          try { $op = @($sortOpsForHints | Where-Object { $_.NodeId -eq [int]$nid } | Select-Object -First 1) } catch { }
          if ($op) {
            $self = if ($op.EstSelfCost -ne $null) { "{0:N2}" -f [double]$op.EstSelfCost } else { "n/a" }

            $rowsVal = $null
            $rowsSrc = $null
            $act = $null
            $est = $null

            if ($op.ActRowsOut -ne $null -and $op.ActRowsOut -ne '') { $act = $op.ActRowsOut }
            if ($op.EstRows    -ne $null -and $op.EstRows    -ne '') { $est = $op.EstRows }

            if ($act -ne $null) { $rowsVal = $act; $rowsSrc = 'act' }
            elseif ($est -ne $null) { $rowsVal = $est; $rowsSrc = 'est' }

            # If we have both actual and estimated, show the estimate when it differs (helps spot CE issues at a glance).
            $rowsDisplay = "n/a"
            if ($rowsVal -ne $null) {
              $rowsDisplay = "{0} ({1})" -f $rowsVal,$rowsSrc
              if ($act -ne $null -and $est -ne $null) {
                try {
                  $a = [double]$act; $e = [double]$est
                  if ($a -ne 0 -and ([math]::Abs(($a-$e)/$a) -ge 0.10)) {
                    $rowsDisplay += (" (est={0})" -f $est)
                  }
                } catch {
                  if ($act.ToString() -ne $est.ToString()) { $rowsDisplay += (" (est={0})" -f $est) }
                }
              }
            }
            $parts += ("{0} (self(est)={1}, rows={2})" -f $nid,$self,$rowsDisplay)
          } else {
            $parts += ("{0}" -f $nid)
          }
        }
        $sortNodeDetailsDisplay = ($parts -join ', ')
      }

      # Best-effort INCLUDE candidates for this table (referenced columns excluding sort keys).
      $desiredIncludeCols = @()
      if ($refColsByTable -and $refColsByTable.ContainsKey($k)) { $desiredIncludeCols = @($refColsByTable[$k]) }
      $desiredIncludeCols = @($desiredIncludeCols | Where-Object { $cols -notcontains $_ } | Sort-Object -Unique)

      # If the order is already covered, warn if INCLUDE coverage differs (lookups may remain).
      $includeGapNote = ''
      if ($covered -and $InspectDatabase -and $dbInspection -and $dbInspection.Indexes -and $desiredIncludeCols.Count -gt 0) {
        $idxsForGap = @($dbInspection.Indexes | Where-Object { $_.FullTable -eq $k })
        $candidates = @($idxsForGap | Where-Object { Index-KeyPrefixMatches $_.KeyCols $cols })
        $bestMissing = $null
        $bestIxName = $null
        foreach ($ix in $candidates) {
          $avail = New-Object 'System.Collections.Generic.HashSet[string]'
          foreach ($seg in ($ix.KeyCols -split ',\s*')) {
            $mm = [regex]::Match($seg,'\[([^\]]+)\]')
            if ($mm.Success) { [void]$avail.Add($mm.Groups[1].Value) }
          }
          if ($ix.IncludeCols) {
            foreach ($seg in ($ix.IncludeCols -split ',\s*')) {
              $mm = [regex]::Match($seg,'\[([^\]]+)\]')
              if ($mm.Success) { [void]$avail.Add($mm.Groups[1].Value) }
            }
          }
          $missing = @()
          foreach ($c in $desiredIncludeCols) { if (-not $avail.Contains($c)) { $missing += $c } }
          if ($bestMissing -eq $null -or $missing.Count -lt $bestMissing.Count) {
            $bestMissing = $missing
            $bestIxName = $ix.IndexName
          }
        }
        if ($bestMissing -and $bestMissing.Count -gt 0) {
          $includeGapNote = ("Order is covered, but INCLUDE differs; may still see lookups for: {0} (e.g., on {1})." -f ($bestMissing -join ', '), $bestIxName)
        }
      }

if (-not $covered) {
  # Generate a readable, valid index name and a best-effort INCLUDE list based on referenced columns.
  $m = [regex]::Match($k,'\[(?<db>[^\]]+)\]\.\[(?<schema>[^\]]+)\]\.\[(?<table>[^\]]+)\]')
  $tableName = if ($m.Success) { $m.Groups['table'].Value } else { ($k -replace '.*\.\[','' -replace '\]$','') }
  $colSuffix = ($cols -join '_')
  $idxName = ("IX_{0}_{1}" -f $tableName,$colSuffix) -replace '[^A-Za-z0-9_]', '_' 
  $idxName = ($idxName -replace '_+','_').Trim('_')
  if ($idxName.Length -gt 120) { $idxName = $idxName.Substring(0,120) }

  $keyList = ($cols | ForEach-Object { '[' + $_ + '] ASC' }) -join ', '

  $includeCols = @($desiredIncludeCols)

  if ($includeCols.Count -gt 0) {
    $incList = ($includeCols | ForEach-Object { '[' + $_ + ']' }) -join ', '
    $suggestedIndexSql = ('CREATE INDEX {0} ON {1} ({2}) INCLUDE ({3});' -f $idxName,$k,$keyList,$incList)
  } else {
    $suggestedIndexSql = ('CREATE INDEX {0} ON {1} ({2});' -f $idxName,$k,$keyList)
  }
}

      $sortIndexSuggestions += [pscustomobject]@{
        FullTable = $k
        SortColumns = ($cols -join ', ')
        CoveredByExistingIndex = [bool]$covered
        CoveringIndexName = $coveringIndexDisplay
        SortNodeIds = @($need.SortNodeIds)
        SortNodeIdsDisplay =
          if ($need.SortNodeIds -and @($need.SortNodeIds).Count -gt 0) { (@($need.SortNodeIds) -join ', ') } else { '' }
        SortNodeDetailsDisplay = $sortNodeDetailsDisplay
        IncludeGapNote = $includeGapNote
        SuggestedCreateIndex = if ($suggestedIndexSql) { $suggestedIndexSql } else { '' }
      }
    }
  }

  # -----------------------------
  # Result object
  # -----------------------------
  $result = [pscustomobject]@{
    File = (Resolve-Path -LiteralPath $Path).Path
    NamespaceURI = $nsUri
    DegreeOfParallelism = $dop
    ObservedDegreeOfParallelism = $dopObserved
    ObservedDopParallelism = $dopObservedParallelism
    ObservedDopMaxRuntime = $dopObservedMaxRuntime
    MemoryGrantInfo = $memoryGrantInfo
    SpillSignalsFound = $spillLikely

    TopOperators = $topOps
    TopSelfOperators = $topSelfOps
    ReadsHeavyOps = $readsHeavyOps

    CardinalityIssues = $cardinalityIssues
    CEStatementSummary = $ceStatementSummary

    KeyLookups = $keyLookups
    SortOps = $sortOps
    SortIndexSuggestions = $sortIndexSuggestions
    ParallelismOps = $parallelOps

    MissingIndexes = $missingIndexes
    MissingIndexDuplicates = $missingIndexDuplicates

    ParameterSensitivity = $parameterSensitivity
    HasParameterSensitivePlan = $hasParameterSensitivePlan
    MultiColumnPredicateCandidates = $multiColPredicateCandidates
    JoinChecks = $joinChecks
    OperatorRedFlags = $operatorRedFlags
    SpillSignals = $spillSignals
    ParallelSkewSignals = $parallelSkewSignals
    ManyToManyMergeSignals = $manyToManyMergeSignals
    PlanAffectingConvertSignals = $planAffectingConvertSignals
    PredicateConvertImplicitCount = @($operatorRows | Where-Object { $_.HasConvertImplicit }).Count
    ExpressionConvertImplicit = $expressionConvertImplicit
    SargabilityIssues = $sargabilityIssues
    IndexIntersectionSignals = $indexIntersectionSignals
    RewriteHints = $rewriteHints.ToArray()


    Suggestions = $suggestions.ToArray()
    ReferenceLinks = $referenceLinks

    DbInspection = $dbInspection
  }

  if ($IncludeOperatorRows) {
    $result | Add-Member -NotePropertyName OperatorRows -NotePropertyValue $operatorRows -Force
  }

  # -----------------------------
  # Output
  # -----------------------------
  Write-Host ""
  Write-Host "=== SQL Plan Insights ===" -ForegroundColor Cyan
  Write-Host "File: $($result.File)"
  if ($chosenStmtSummary) {
    Write-Host ("Statement chosen: Id={0}" -f $chosenStmtSummary.StatementId) -ForegroundColor DarkCyan
    if ($chosenStmtSummary.StatementText) { Write-Host ("StatementText: {0}" -f $chosenStmtSummary.StatementText) -ForegroundColor DarkGray }
  }

  # --- NEW: Query Store Telemetry Bridge ---
  if ($ServerInstance -and $Database -and $queryHash -and $queryPlanHash) {
    try {
      $connectParams = @{
        SqlInstance            = $ServerInstance
        Database               = $Database
        TrustServerCertificate = $true
        ErrorAction            = 'Stop'
      }

      # dbatools uses SqlCredential for SQL Authentication
      if ($SqlCredential) {
          $connectParams.SqlCredential = $SqlCredential
      }

      # Establish the SMO connection
      $smoServer = Connect-DbaInstance @connectParams

      # Extract the underlying .NET SqlConnection so the rest of your DataReader logic works unmodified
      $conn = $smoServer.ConnectionContext.SqlConnectionObject
      if ($conn.State -ne 'Open') { 
          $conn.Open()
      }
      
      $cmd = $conn.CreateCommand()
      $cmd.CommandText = @"
      SELECT TOP 1
          rs.count_executions,
          CAST((rs.avg_duration / 1000.0) AS DECIMAL(18,2)) AS avg_duration_ms,
          CAST((rs.max_duration / 1000.0) AS DECIMAL(18,2)) AS max_duration_ms,
          CAST((rs.avg_cpu_time / 1000.0) AS DECIMAL(18,2)) AS avg_cpu_ms,
          rs.avg_logical_io_reads AS avg_logical_reads
      FROM sys.query_store_query qs
      JOIN sys.query_store_plan p ON qs.query_id = p.query_id
      JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
      WHERE qs.query_hash = CONVERT(VARBINARY(8), '$queryHash', 1)
        AND p.query_plan_hash = CONVERT(VARBINARY(8), '$queryPlanHash', 1)
      ORDER BY rs.last_execution_time DESC;
"@
      $rdr = $cmd.ExecuteReader()
      if ($rdr.Read()) {
          Write-Host ""
          Write-Host "Query Store Telemetry (Recent Context):" -ForegroundColor Green
          Write-Host " - QueryHash : $queryHash | PlanHash : $queryPlanHash" -ForegroundColor DarkGray
          Write-Host (" - Executions: {0:N0}" -f $rdr["count_executions"]) -ForegroundColor Gray
          Write-Host (" - Avg CPU   : {0:N2} ms" -f $rdr["avg_cpu_ms"]) -ForegroundColor Gray
          Write-Host (" - Avg Time  : {0:N2} ms (Max: {1:N2} ms)" -f $rdr["avg_duration_ms"], $rdr["max_duration_ms"]) -ForegroundColor Gray
          Write-Host (" - Avg Reads : {0:N0} pages" -f $rdr["avg_logical_reads"]) -ForegroundColor Gray
          
          # Severity heuristics
          $execs = [long]$rdr["count_executions"]
          $cpu = [decimal]$rdr["avg_cpu_ms"]
          if ($execs -gt 1000 -and $cpu -gt 250) {
              Write-Host " [!] HIGH IMPACT: This plan burns significant CPU and runs frequently. Tuning this is a priority." -ForegroundColor Red
          }

          $rdr.Close() # Close the first reader so we can run a second query

          # --- NEW: Automated Plan Regression Detection ---
          $cmdReg = $conn.CreateCommand()
          $cmdReg.CommandText = @"
          SELECT TOP 1
              p.plan_id,
              qs.query_id,
              CAST((rs.avg_cpu_time / 1000.0) AS DECIMAL(18,2)) AS hist_avg_cpu_ms
          FROM sys.query_store_query qs
          JOIN sys.query_store_plan p ON qs.query_id = p.query_id
          JOIN sys.query_store_runtime_stats rs ON p.plan_id = rs.plan_id
          WHERE qs.query_hash = CONVERT(VARBINARY(8), '$queryHash', 1)
            AND p.query_plan_hash <> CONVERT(VARBINARY(8), '$queryPlanHash', 1)
            AND (rs.avg_cpu_time / 1000.0) < ($cpu * 0.5) -- Look for plans at least 50% cheaper
          ORDER BY rs.avg_cpu_time ASC;
"@
          $rdrReg = $cmdReg.ExecuteReader()
          if ($rdrReg.Read()) {
              Write-Host ""
              Write-Host " [!] PLAN REGRESSION DETECTED (Query Store)" -ForegroundColor Magenta
              Write-Host (" - A historical plan (PlanId: {0}) averaged only {1:N2} ms of CPU (50%+ less than current)." -f $rdrReg["plan_id"], $rdrReg["hist_avg_cpu_ms"]) -ForegroundColor Gray
              Write-Host " - To instantly revert to the better historical plan, run:" -ForegroundColor DarkGray
              Write-Host ("   EXEC sp_query_store_force_plan @query_id = {0}, @plan_id = {1};" -f $rdrReg["query_id"], $rdrReg["plan_id"]) -ForegroundColor Cyan
          } else {
              Write-Host " - Plan Regression Check: No cheaper historical plans found (current plan is optimal or the only plan)." -ForegroundColor DarkGray
          }
          $rdrReg.Close()
          # ------------------------------------------------

      } else {
          Write-Host ""
          Write-Host "Query Store Telemetry: No runtime stats found for this specific Plan Hash." -ForegroundColor DarkGray
      }
      $conn.Close()
    } catch {
      Write-Host ""
      Write-Host "Query Store Telemetry: Failed to connect or query QDS ($($_.Exception.Message.Substring(0,[math]::Min($_.Exception.Message.Length, 60))))..." -ForegroundColor DarkGray
    }
  }
  if ($SanityCheck) {
    Write-Host ("SanityCheck: missingIndexGroups={0}, missingIndexes={1}, links={2}" -f `
         (@($missingIndexGroups).Count),(@($missingIndexes).Count),($referenceLinks.PSObject.Properties.Count)) -ForegroundColor DarkCyan
  }

$mismatches = $operatorRows | Where-Object { 
    $_.ActRows -ne $null -and 
    $_.Ratio -gt $CEMismatchRatio -and 
    $_.ActRows -gt $CEMinRows 
} | Sort-Object -Property Ratio -Descending | 
Select-Object -Property NodeId, 
                        PhysicalOp, 
                        EstRows, 
                        ActRows, 
                        @{ Name = 'Ratio'; Expression = { F2 $_.Ratio } }, 
                        Object

if ($mismatches) {
    Write-Host ""
    Write-Host "Significant Cardinality Estimate Mismatches Detected:" -ForegroundColor Yellow
    $mismatches | Format-Table -AutoSize
}
else { Write-Host ""
       Write-Host "No Cardinality Estimate Mismatches Detected" -ForegroundColor Yellow}

# wait stats
$waitNodes = $xml.SelectNodes("//sp:WaitStats/sp:Wait", $nsm)
$waitStatsReport = @()

if ($waitNodes.Count -gt 0) {
    foreach ($w in $waitNodes) {
        $waitStatsReport += [pscustomobject]@{
            WaitType    = $w.GetAttribute("WaitType")
            WaitTimeMs  = [double]$w.GetAttribute("WaitTimeMs")
            WaitCount   = [int]$w.GetAttribute("WaitCount")
            AvgWaitMs   = [math]::Round(([double]$w.GetAttribute("WaitTimeMs") / [int]$w.GetAttribute("WaitCount")), 3)
        }
    }
    
    # Sort by longest wait time
    $waitStatsReport = $waitStatsReport | Sort-Object WaitTimeMs -Descending

    Write-Host ""
    Write-Host "Query Wait Statistics (Actual Plan Only):" -ForegroundColor Yellow
    $waitStatsReport | Format-Table -AutoSize
}

  $dopPlannedText = if ($dop -ne $null -and $dop -gt 0) { $dop } else { "n/a" }
  $dopObservedText = if ($dopObserved -ne $null -and $dopObserved -gt 0) { $dopObserved } else { "n/a" }
  $dopParText = if ($dopObservedParallelism -ne $null -and $dopObservedParallelism -gt 0) { $dopObservedParallelism } else { "n/a" }
  $dopMaxText = if ($dopObservedMaxRuntime -ne $null -and $dopObservedMaxRuntime -gt 0) { $dopObservedMaxRuntime } else { "n/a" }

  Write-Host ""
  Write-Host ("DOP: planned={0} observed={1} (parallelism={2} maxRuntime={3})" -f $dopPlannedText,$dopObservedText,$dopParText,$dopMaxText)


  $hasMGNumbers = $memoryGrantInfo -and (
    $null -ne $memoryGrantInfo.RequestedKB -or $null -ne $memoryGrantInfo.GrantedKB -or
    $null -ne $memoryGrantInfo.UsedKB -or $null -ne $memoryGrantInfo.MaxUsedKB -or
    $null -ne $memoryGrantInfo.GrantWaitMS
  )

  function F3orNA ($v) { if ($null -eq $v -or [string]::IsNullOrWhiteSpace([string]$v)) { "n/a" } else { F3 $v } }
  $mgFeedback = if ($memoryGrantInfo -and -not [string]::IsNullOrWhiteSpace($memoryGrantInfo.FeedbackAdjusted)) { $memoryGrantInfo.FeedbackAdjusted } else { "n/a" }

  if ($hasMGNumbers) {
    Write-Host ("Memory grant KB: Requested={0} Granted={1} Used={2} MaxUsed={3} WaitMS={4} FeedbackAdjusted={5}" -f `
         (F3orNA $memoryGrantInfo.RequestedKB),
      (F3orNA $memoryGrantInfo.GrantedKB),
      (F3orNA $memoryGrantInfo.UsedKB),
      (F3orNA $memoryGrantInfo.MaxUsedKB),
      (F3orNA $memoryGrantInfo.GrantWaitMS),
      $mgFeedback) -ForegroundColor DarkCyan

    Write-Host ""
    # Memory grant analysis (heuristic)
    $mgGranted = $null; $mgMaxUsed = $null
    try { $mgGranted = [double]$memoryGrantInfo.GrantedKB } catch {}
    try { $mgMaxUsed = [double]$memoryGrantInfo.MaxUsedKB } catch {}
    if ($mgGranted -and $mgGranted -gt 0 -and $mgMaxUsed -ne $null) {
      $wastePct = (($mgGranted - $mgMaxUsed) / $mgGranted) * 100.0
      $underPct = 0.0
      if ($mgMaxUsed -gt $mgGranted) { $underPct = (($mgMaxUsed - $mgGranted) / $mgGranted) * 100.0 }
      $mgNotes = New-Object System.Collections.Generic.List[string]
      if ($wastePct -ge 50) { $mgNotes.Add("high grant waste") }
      if ($underPct -gt 0) { $mgNotes.Add("possible undergrant/spill risk") }
      if ($mgFeedback -ne "n/a" -and $mgFeedback -match "Adjust") { $mgNotes.Add("MG feedback active") }
      $noteText = if ($mgNotes.Count -gt 0) { (" (" + ($mgNotes -join "; ") + ")") } else { "" }
      Write-Host ("Memory grant analysis: WastePct={0}% UnderPct={1}%{2}" -f (F2 $wastePct),(F2 $underPct),$noteText)
    }
  }
  else {
    Write-Host ("Memory grant: n/a") -ForegroundColor DarkCyan
  }

  if (-not $hasRuntimeStats) {
    Write-Host ("Runtime stats: n/a (estimated plan or plan export without execution counters)") -ForegroundColor DarkCyan
  }

  if ($spillLikely.Count -gt 0) { Write-Host ("Spill markers in XML: {0}" -f ($spillLikely -join ", ")) -ForegroundColor DarkYellow }

  Write-Host ""
  Write-Host "Top operators by EstimatedTotalSubtreeCost:" -ForegroundColor Yellow
  $result.TopOperators | Sort-Object NodeId -Descending |
  Select-Object `
     @{ n = 'NodeId'; e = { $_.NodeId } },
  @{ n = 'PhysicalOp'; e = { $_.PhysicalOpDisplay } },
  @{ n = 'LogicalOp'; e = { $_.LogicalOp } },
  @{ n = 'EstCost'; e = { F2 $_.EstCost } },
  @{ n = 'SelfCost'; e = { if ($_.HasKids -and ($null -eq $_.EstSelfCost -or $_.EstSelfCost -eq '')) { '0.00' } else { F2 $_.EstSelfCost } } },
  @{ n = 'EstRows'; e = { F0 $_.EstRows } },
  @{ n = 'ActOut'; e = { if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '') { F0 $_.ActRowsOut } else { '' } } },
  @{ n = 'ActWork'; e = { if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '') { F0 $_.ActRowsWork } else { '' } } },
  @{ n = 'RatioOut'; e = { if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsOut) / ([double]$_.EstRows)) } else { '' } } },
  @{ n = 'RatioWork'; e = { if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsWork) / ([double]$_.EstRows)) } else { '' } } },
  @{ n = 'RowsRead'; e = { F0 $_.RowsRead } },
  @{ n = 'Execs'; e = { F0 $_.Execs } },
  @{ n = 'Access'; e = { $_.Access } },
  @{ n = 'Object'; e = { $_.Object } },
  @{ n = 'Warnings'; e = { $_.Warnings } } |
  Format-Table -AutoSize

  # --- NEW: Explanatory Blurb for Top Operators ---
  Write-Host "  Note: Operators highlighted with >> << (e.g., >> SORT <<, >> HASH MATCH <<) are heavy execution nodes" -ForegroundColor DarkGray
  Write-Host "  that frequently cause TempDB spills, CPU spikes, or memory grant issues. " -ForegroundColor DarkGray
  Write-Host "  This grid is filtered to the most expensive operators, excluding trivial/zero-cost nodes (like Compute Scalars) " -ForegroundColor DarkGray
  Write-Host "  and nodes that fall below the Top N cost threshold, focusing your attention on the true bottlenecks." -ForegroundColor DarkGray
  Write-Host ""

  Write-Host ""
  Write-Host "Top operators by EstimatedSelfCost (delta):" -ForegroundColor Yellow

  # Avoid duplicating the same operators already shown in the subtree-cost list.
  $topIds = @{}
  foreach ($o in $result.TopOperators) { $topIds[$o.NodeId] = $true }

  $selfFiltered = $result.TopSelfOperators | Where-Object { -not $topIds.ContainsKey($_.NodeId) }

  if (@($selfFiltered).Count -gt 0) {
    $selfFiltered |
    Select-Object `
    @{ n = 'NodeId'; e = { $_.NodeId } },
    @{ n = 'PhysicalOp'; e = { $_.PhysicalOpDisplay } },
    @{ n = 'LogicalOp'; e = { $_.LogicalOp } },
    @{ n = 'SelfCost'; e = { if ($_.HasKids -and ($null -eq $_.EstSelfCost -or $_.EstSelfCost -eq '')) { '0.00' } else { F2 $_.EstSelfCost } } },
    @{ n = 'EstCost'; e = { F2 $_.EstCost } },
    @{ n = 'EstRows'; e = { F0 $_.EstRows } },
    @{ n = 'ActOut'; e = { if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '') { F0 $_.ActRowsOut } else { '' } } },
    @{ n = 'ActWork'; e = { if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '') { F0 $_.ActRowsWork } else { '' } } },
    @{ n = 'RatioOut'; e = { if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsOut) / ([double]$_.EstRows)) } else { '' } } },
    @{ n = 'RatioWork'; e = { if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsWork) / ([double]$_.EstRows)) } else { '' } } },
    @{ n = 'RowsRead'; e = { F0 $_.RowsRead } },
    @{ n = 'Execs'; e = { F0 $_.Execs } },
    @{ n = 'Access'; e = { $_.Access } },
    @{ n = 'Object'; e = { $_.Object } },
    @{ n = 'Warnings'; e = { $_.Warnings } } |
    Format-Table -AutoSize
  } else {
    Write-Host " - (same operators as subtree-cost list; omitted)" -ForegroundColor DarkGray
  }

  # -----------------------------
  # DBA heuristics output
  # -----------------------------
  Write-Host ""

  # Build a quick NodeId -> operator lookup for richer heuristic summaries
  $opMap = @{}
  foreach ($r in $operatorRows) {
    if ($r.NodeId -ne $null) { $opMap["$($r.NodeId)"] = $r }
  }

  Write-Host "Join strategy sanity checks (heuristic):" -ForegroundColor Yellow
  if (@($joinChecks).Count -gt 0) {

    if (-not $ShowAllHeuristicMatches) {
      $groups = @($joinChecks | Group-Object Join,Signal)
      Write-Host (" - {0} matching operators grouped into {1} finding(s). Use -ShowAllHeuristicMatches to list each operator." -f @($joinChecks).Count,$groups.Count) -ForegroundColor DarkGray

      $joinChecksGrouped = foreach ($g in $groups) {
        $sample = $g.Group[0]
        $ids = @($g.Group | Select-Object -ExpandProperty NodeId | Sort-Object)
        $idText = ($ids -join ', ')
        if ($idText.Length -gt 60) { $idText = $idText.Substring(0,57) + '...' }

        $rowVals = @($g.Group | ForEach-Object { $_.Rows } | Where-Object { $_ -ne $null })
        $rowsMin = $null; $rowsMax = $null
        if ($rowVals.Count -gt 0) {
          $m = $rowVals | Measure-Object -Minimum -Maximum
          $rowsMin = $m.Minimum
          $rowsMax = $m.Maximum
        }

        $execVals = @($g.Group | ForEach-Object { $_.Execs } | Where-Object { $_ -ne $null })
        $execMax = $null
        if ($execVals.Count -gt 0) { $execMax = ($execVals | Measure-Object -Maximum).Maximum }

        $objs = @($g.Group | ForEach-Object { $_.Object } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
        $objText = ''
        if ($objs.Count -gt 0) {
          $objText = ($objs | Select-Object -First 2) -join ' | '
          if ($objs.Count -gt 2) { $objText += ' | ...' }
        }


        $opsForGroup = @($ids | ForEach-Object { $opMap["$_"] } | Where-Object { $_ -ne $null })

        $logOps = @($opsForGroup | ForEach-Object { $_.LogicalOp } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
        $logText = ''
        if ($logOps.Count -gt 0) {
          $logText = ($logOps | Select-Object -First 2) -join ' | '
          if ($logOps.Count -gt 2) { $logText += ' | ...' }
        }

        $costVals = @($opsForGroup | ForEach-Object { $_.EstCost } | Where-Object { $_ -ne $null })
        $costMax = $null
        if ($costVals.Count -gt 0) { $costMax = ($costVals | Measure-Object -Maximum).Maximum }

        $worstNode = ''
        if ($costVals.Count -gt 0) {
          $worst = $opsForGroup | Where-Object { $_.EstCost -ne $null } | Sort-Object EstCost -Descending | Select-Object -First 1
          if ($worst) { $worstNode = $worst.NodeId }
        }

        [pscustomobject]@{
          Count = $g.Count
          NodeIds = $idText
          Join = $sample.Join
          Signal = $sample.Signal
          RowsMin = $(if ($rowsMin -ne $null) { F0 $rowsMin } else { '' })
          RowsMax = $(if ($rowsMax -ne $null) { F0 $rowsMax } else { '' })
          ExecMax = $(if ($execMax -ne $null) { F0 $execMax } else { '' })
          EstCostMax = $(if ($costMax -ne $null) { F2 $costMax } else { '' })
          WorstNode = $worstNode
          LogicalOps = $logText
          Objects = $objText
          Detail = $sample.Detail
        }
      }

      $joinChecksGrouped |
      Sort-Object @{ Expression = 'Count'; Descending = $true },Join,Signal |
      Format-Table Count,NodeIds,Join,Signal,RowsMin,RowsMax,ExecMax,EstCostMax,WorstNode,LogicalOps,Objects -AutoSize
    } else {
      $joinChecks | Select-Object NodeId,Join,Signal,Rows,Execs,Object,Detail | Format-Table -AutoSize
    }

  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  Write-Host "Operator red flags (heuristic):" -ForegroundColor Yellow
  if (@($operatorRedFlags).Count -gt 0) {

    if (-not $ShowAllHeuristicMatches) {
      $groups = @($operatorRedFlags | Group-Object Operator,Signal)
      Write-Host (" - {0} matching operators grouped into {1} finding(s). Use -ShowAllHeuristicMatches to list each operator." -f @($operatorRedFlags).Count,$groups.Count) -ForegroundColor DarkGray

      $operatorRedFlagsGrouped = foreach ($g in $groups) {
        $sample = $g.Group[0]
        $ids = @($g.Group | Select-Object -ExpandProperty NodeId | Sort-Object)
        $idText = ($ids -join ', ')
        if ($idText.Length -gt 60) { $idText = $idText.Substring(0,57) + '...' }

        $rowVals = @($g.Group | ForEach-Object { $_.Rows } | Where-Object { $_ -ne $null })
        $rowsMax = $null
        if ($rowVals.Count -gt 0) { $rowsMax = ($rowVals | Measure-Object -Maximum).Maximum }

        $execVals = @($g.Group | ForEach-Object { $_.Execs } | Where-Object { $_ -ne $null })
        $execMax = $null
        if ($execVals.Count -gt 0) { $execMax = ($execVals | Measure-Object -Maximum).Maximum }

        $objs = @($g.Group | ForEach-Object { $_.Object } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
        $objText = ''
        if ($objs.Count -gt 0) {
          $objText = ($objs | Select-Object -First 2) -join ' | '
          if ($objs.Count -gt 2) { $objText += ' | ...' }
        }


        $opsForGroup = @($ids | ForEach-Object { $opMap["$_"] } | Where-Object { $_ -ne $null })

        $logOps = @($opsForGroup | ForEach-Object { $_.LogicalOp } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
        $logText = ''
        if ($logOps.Count -gt 0) {
          $logText = ($logOps | Select-Object -First 2) -join ' | '
          if ($logOps.Count -gt 2) { $logText += ' | ...' }
        }

        $costVals = @($opsForGroup | ForEach-Object { $_.EstCost } | Where-Object { $_ -ne $null })
        $costMax = $null
        if ($costVals.Count -gt 0) { $costMax = ($costVals | Measure-Object -Maximum).Maximum }

        $worstNode = ''
        if ($costVals.Count -gt 0) {
          $worst = $opsForGroup | Where-Object { $_.EstCost -ne $null } | Sort-Object EstCost -Descending | Select-Object -First 1
          if ($worst) { $worstNode = $worst.NodeId }
        }

        [pscustomobject]@{
          Count = $g.Count
          NodeIds = $idText
          Operator = $sample.Operator
          Signal = $sample.Signal
          RowsMax = $(if ($rowsMax -ne $null) { F0 $rowsMax } else { '' })
          ExecMax = $(if ($execMax -ne $null) { F0 $execMax } else { '' })
          EstCostMax = $(if ($costMax -ne $null) { F2 $costMax } else { '' })
          WorstNode = $worstNode
          LogicalOps = $logText
          Objects = $objText
          Detail = $sample.Detail
        }
      }

      $operatorRedFlagsGrouped | Sort-Object @{ Expression = 'Count'; Descending = $true },Operator,Signal | Format-Table -AutoSize

    } else {
      $operatorRedFlags | Select-Object NodeId,Operator,Signal,Rows,Execs,Object,Detail | Format-Table -AutoSize
    }

  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  Write-Host "Spill signals (from plan XML):" -ForegroundColor Yellow
  if (@($spillSignals).Count -gt 0) {
    $spillSignals | Select-Object NodeId,Operator,Signal,Detail | Sort-Object NodeId | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  Write-Host "Parallelism skew signals (heuristic):" -ForegroundColor Yellow
  if (@($parallelSkewSignals).Count -gt 0) {
    $parallelSkewSignals |
    Sort-Object SkewRatio -Descending |
    Select-Object NodeId,Operator,Threads,@{ n = 'TotalRows'; e = { F0 $_.TotalRows } },Signal,Detail |
    Format-Table -AutoSize
  } else {
    if ($skewScanned -gt 0) {
      Write-Host " - (none detected) Note: Scanned $skewScanned parallel operators; no high skew (ratio >= 5) found." -ForegroundColor DarkGray
    } else {
      Write-Host " - (none detected)" -ForegroundColor DarkGray
    }
  }

  Write-Host ""
  Write-Host "Many-to-many merge join signals:" -ForegroundColor Yellow
  if (@($manyToManyMergeSignals).Count -gt 0) {
    $manyToManyMergeSignals | Select-Object NodeId,Join,Signal,Detail | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  Write-Host "Implicit conversion signals (PlanAffectingConvert):" -ForegroundColor Yellow
  if (@($planAffectingConvertSignals).Count -gt 0) {
    $planAffectingConvertSignals | Select-Object StatementId,Issue,Expression | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected) Note: SQL Server only creates PlanAffectingConvert elements when implicit conversions significantly affect plan choice." -ForegroundColor DarkGray
  }

  

  Write-Host ""
  Write-Host "Predicate CONVERT_IMPLICIT signals (heuristic):" -ForegroundColor Yellow
  $predConvertNodes = $operatorRows | Where-Object { $_.HasConvertImplicit } | Select-Object NodeId,PhysicalOp,Object,Predicate,SeekPredicate,Residual
  if (@($predConvertNodes).Count -gt 0) {
    $predConvertNodes | Select-Object NodeId,PhysicalOp,Object | Format-Table -AutoSize
    Write-Host "Tip: confirm the conversion is happening on the column side of a predicate/seek; if so, align parameter/column datatypes to restore seeks." -ForegroundColor Gray
  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  Write-Host "Expression CONVERT_IMPLICIT in Compute Scalar:" -ForegroundColor Yellow
  if (@($expressionConvertImplicit).Count -gt 0) {
    $expressionConvertImplicit | Select-Object NodeId,Operator,Expression | Format-Table -AutoSize
    Write-Host "Note: compute-scalar conversions are often harmless for seekability, but can add CPU or hint at datatype mismatches upstream." -ForegroundColor Gray
  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

Write-Host ""
  Write-Host "Predicate SARGability signals (heuristic):" -ForegroundColor Yellow
  $predCount = @($operatorRows | Where-Object { -not [string]::IsNullOrWhiteSpace($_.SeekPredicate) -or -not [string]::IsNullOrWhiteSpace($_.Predicate) -or -not [string]::IsNullOrWhiteSpace($_.Residual) }).Count
  if (@($sargabilityIssues).Count -gt 0) {
    $sargabilityIssues | Select-Object NodeIds,Issues,Expression | Format-Table -AutoSize
  } else {
    if ($predCount -gt 0) {
      Write-Host " - (none detected) Note: Scanned $predCount predicate expressions; no non-SARGable patterns (CONVERT_IMPLICIT, functions on columns, leading wildcards) found." -ForegroundColor DarkGray
    } else {
      Write-Host " - (none detected) Note: No predicate expressions found to scan." -ForegroundColor DarkGray
    }
  }
  Write-Host ""
  Write-Host "Parameter sensitivity signals:" -ForegroundColor Yellow
  if ($hasParameterSensitivePlan) {
    Write-Host " - Parameter Sensitive Plan optimization detected in plan XML."
  }
  if (@($psIssues).Count -gt 0) {
    $psIssues | Select-Object Name,DataType,CompiledValue,RuntimeValue,@{ n = 'ValueRatio'; e = { F2 $_.ValueRatio } } | Format-Table -AutoSize
  } elseif (@($parameterSensitivity).Count -gt 0) {
    $parameterSensitivity | Select-Object Name,DataType,CompiledValue,RuntimeValue,@{ n = 'ValueRatio'; e = { if ($_.ValueRatio -ne $null) { F2 $_.ValueRatio } else { '' } } } | Format-Table -AutoSize
    $missingRuntime = @($parameterSensitivity | Where-Object { [string]::IsNullOrWhiteSpace($_.RuntimeValue) }).Count
    if ($missingRuntime -gt 0) {
      Write-Host " - Note: Some parameters lack ParameterRuntimeValue in this plan XML. If possible, save an *actual* plan (Include Actual Execution Plan) and re-export it." -ForegroundColor DarkGray
    } else {
      Write-Host " - Note: ValueRatio is calculated only for numeric parameters; blanks are normal for strings/dates or non-numeric formatting." -ForegroundColor DarkGray
    }

  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }
  Write-Host ""
  Write-Host "Index intersection signals (heuristic):" -ForegroundColor Yellow
  $bitmapCount = @($operatorRows | Where-Object { $_.PhysicalOp -match 'Bitmap' -or $_.LogicalOp -match 'Bitmap' }).Count
  if (@($indexIntersectionSignals).Count -gt 0) {
    $indexIntersectionSignals | Select-Object NodeId,Operator,Detail | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected) Note: No bitmap operators or PROBE()/Opt_Bitmap patterns detected in plan." -ForegroundColor DarkGray
  }
  Write-Host ""
  Write-Host "Rewrite / design hints (heuristic):" -ForegroundColor Yellow
  if ($rewriteHints.Count -gt 0) {
    $rewriteHints | ForEach-Object { Write-Host " - $_" }
  } else {
    Write-Host " - (none)" -ForegroundColor DarkGray
  }

  if ($result.SortIndexSuggestions -and @($result.SortIndexSuggestions).Count -gt 0) {
    Write-Host "" 
    Write-Host "Order / sort support opportunities (heuristic):" -ForegroundColor Yellow
    $result.SortIndexSuggestions |
    Select-Object FullTable,SortColumns,CoveredByExistingIndex,CoveringIndexName |
    Format-Table -AutoSize

    Write-Host "Note: CoveredByExistingIndex means an existing index has a key prefix matching the sort columns (in order). The optimizer may still choose a different access path." -ForegroundColor Gray

    $gapNotes = @($result.SortIndexSuggestions | Where-Object { $_.CoveredByExistingIndex -and -not [string]::IsNullOrWhiteSpace($_.IncludeGapNote) })
    if ($gapNotes.Count -gt 0) {
      Write-Host "Note: Even when order is covered, missing INCLUDE columns can still force lookups." -ForegroundColor Gray
      $gapNotes | Select-Object -First 3 | ForEach-Object { Write-Host (" - " + $_.IncludeGapNote) -ForegroundColor DarkGray }
    }

    $todo = @($result.SortIndexSuggestions | Where-Object { -not $_.CoveredByExistingIndex -and -not [string]::IsNullOrWhiteSpace($_.SuggestedCreateIndex) })
    if ($todo.Count -gt 0) {
      Write-Host "" 
      Write-Host "Example CREATE INDEX statements (validate first):" -ForegroundColor DarkYellow
      $todo | Select-Object -First 3 | ForEach-Object {
        Write-Host (" - " + $_.SuggestedCreateIndex)
        $sn = if (-not [string]::IsNullOrWhiteSpace($_.SortNodeDetailsDisplay)) { $_.SortNodeDetailsDisplay } else { $_.SortNodeIdsDisplay }
        if (-not [string]::IsNullOrWhiteSpace($sn)) {
          $keys = if (-not [string]::IsNullOrWhiteSpace($_.SortColumns)) { $_.SortColumns } else { '(unknown)' }
          Write-Host ("   Note: matches Sort operator NodeId(s): {0}; sort keys: {1}; if chosen, may remove/reduce that Sort." -f $sn,$keys) -ForegroundColor DarkGray

          # If the plan shows high memory-grant waste, sorts are common drivers; removing/reducing a Sort can reduce the required grant.
          try {
            $mgWaste = $null

            # Prefer the existing memory-grant analysis (if already computed), otherwise derive from Granted vs MaxUsed.
            if ($memoryGrantAnalysis -and $memoryGrantAnalysis.WastePct -ne $null) {
              $mgWaste = [double]$memoryGrantAnalysis.WastePct
            } else {
              $mgG = $null; $mgMU = $null
              if ($memoryGrantInfo -and $memoryGrantInfo.GrantedKB -ne $null -and $memoryGrantInfo.MaxUsedKB -ne $null) {
                $mgG = [double]$memoryGrantInfo.GrantedKB
                $mgMU = [double]$memoryGrantInfo.MaxUsedKB
              }
              if ($mgG -and $mgG -gt 0 -and $mgMU -ne $null) {
                $mgWaste = (($mgG - $mgMU) / $mgG) * 100.0
              }
            }

            # Optional extra notes for this sort opportunity:
            $snLocal = $null
            try { $snLocal = $_.SortNodeIds } catch { }
            if (-not $snLocal -and $_.PSObject.Properties.Match('SortNodeId').Count -gt 0 -and $_.SortNodeId) { $snLocal = @($_.SortNodeId) }

            # High memory-grant waste can be driven by Sorts; if this recommendation targets a Sort, call it out.
            if ($snLocal -and $mgWaste -ne $null -and $mgWaste -ge 50) {
                Write-Host ("   Also: Memory grant waste is high (WastePct={0:N2}%). If an order-supporting index is used and the Sort is avoided or reduced, the memory grant may reduce." -f $mgWaste) -ForegroundColor DarkGray
            }

            # If the base table is large, remind about size/write overhead of new indexes.
            if ($InspectDatabase -and $dbInspection -and $ft -and $dbInspection.Stats -and $dbInspection.Stats.ContainsKey($ft)) {
                try {
                    $approxRows2 = ($dbInspection.Stats[$ft] | Select-Object -First 1).Rows
                    if ($approxRows2 -ge 1000000) {
                        Write-Host ("   Also: Table is large (~{0:N0} rows). This index may be large; check space and write overhead before deploying." -f $approxRows2) -ForegroundColor DarkGray
                    }
                } catch { }
            }

            # Suggested index may overlap existing ones; that's not always bad, but it can increase maintenance.
            if ($InspectDatabase -and $dbInspection -and $ft -and $dbInspection.Indexes -and $dbInspection.Indexes.ContainsKey($ft)) {
                try {
                    $existing = $dbInspection.Indexes[$ft]
                    $suggestedCols = @($keyCols + $includeCols) | Where-Object { $_ } | ForEach-Object { $_.ToLowerInvariant() } | Select-Object -Unique
                    $overlaps = @()
                    foreach ($ex in $existing) {
                        $exCols = @()
                        if ($ex.KeyCols)    { $exCols += ($ex.KeyCols    -split ',') | ForEach-Object { $_.Trim().ToLowerInvariant() } }
                        if ($ex.IncludeCols){ $exCols += ($ex.IncludeCols -split ',') | ForEach-Object { $_.Trim().ToLowerInvariant() } }
                        $exCols = $exCols | Where-Object { $_ } | Select-Object -Unique
                        if ($exCols.Count -gt 0) {
                            $common = @($suggestedCols | Where-Object { $exCols -contains $_ })
                            if ($common.Count -ge 2) {
                                $overlaps += [pscustomobject]@{ IndexName=$ex.IndexName; Common=$common }
                            }
                        }
                    }
                    if ($overlaps.Count -gt 0) {
                        $msg = ($overlaps | ForEach-Object { "{0} (common: {1})" -f $_.IndexName, (($_.Common | Select-Object -First 5) -join ', ') }) -join '; '
                        Write-Host ("   Also: Suggested index overlaps columns with existing index(es): {0}. Overlap is column-level; key order may differ, so a new index can still be useful for ORDER BY support." -f $msg) -ForegroundColor DarkGray
                    }
                } catch { }
            }
          } catch { }
        }
      }
      Write-Host "Tip: consider INCLUDE columns used by the query to avoid lookups (e.g., projected or joined columns)." -ForegroundColor Gray
      Write-Host "Note: new indexes can be large and add write overhead; validate with your workload and test before deploying." -ForegroundColor Gray
    }
  }

  if (@($missingIndexes).Count -gt 0) {
    Write-Host ""
    if (@($missingIndexes).Count -gt 1) {
      Write-Host ("Missing index recommendations (from plan XML) - COUNT: {0} (Unique: {1})" -f @($missingIndexes).Count,$missingIndexesUniqueCount) -ForegroundColor Yellow
    } else {
      Write-Host "Missing index recommendations (from plan XML):" -ForegroundColor Yellow
    }

    $missingIndexesUnique |
    Select-Object `
       @{ n = 'Impact'; e = { F3 $_.Impact } },
    @{ n = 'FullTable'; e = { $_.FullTable } },
    @{ n = 'Equality'; e = { $_.EqualityText } },
    @{ n = 'Inequality'; e = { $_.InequalityText } },
    @{ n = 'Include'; e = { $_.IncludeText } } |
    Format-Table -AutoSize

    if (@($missingIndexDuplicates).Count -gt 0) {
      Write-Host ""
      Write-Host "Duplicate missing-index signatures:" -ForegroundColor DarkYellow
      $missingIndexDuplicates |
      Select-Object FullTable,Count,@{ n = 'MaxImpact'; e = { F3 $_.MaxImpact } } |
      Format-Table -AutoSize
    }

    Write-Host ""
    Write-Host "Example CREATE INDEX statements (validate first):" -ForegroundColor DarkYellow
    $missingIndexesUnique | Select-Object -First 3 | ForEach-Object { Write-Host (" - " + $_.SuggestedSql) }
  }

    if ($result.HasParameterSensitivePlan) {
      Write-Host ""
      Write-Host "Parameter Sensitive Plan optimization detected (SQL Server PSP):" -ForegroundColor DarkYellow
      Write-Host " - QueryPlan has ParameterSensitivePlan=true" -ForegroundColor Gray
    }

    $psIssues = $result.ParameterSensitivity |
    Where-Object { $_.RuntimeValue -and $_.CompiledValue -and $_.ValueRatio -ne $null -and $_.ValueRatio -ge 2 } |
    Sort-Object ValueRatio -Descending

    if ($psIssues.Count -gt 0) {
      Write-Host ""
  Write-Host "Parameter sensitivity signals:" -ForegroundColor Yellow
  
  if ($hasParameterSensitivePlan) {
    Write-Host " - Parameter Sensitive Plan optimization detected in plan XML." -ForegroundColor Gray
  }


  if (@($psIssues).Count -gt 0) {
    $psIssues | Select-Object Name,DataType,CompiledValue,RuntimeValue,@{ n = 'ValueRatio'; e = { F3 $_.ValueRatio } } | Format-Table -AutoSize
    Write-Host "Hint: large compiled/runtime differences can indicate parameter sniffing risk." -ForegroundColor Gray

    # --- Automated Parameter Sniffing Test Scripts ---
    Write-Host ""
    Write-Host "Automated Mitigation Testing Scripts (Parameter Sniffing):" -ForegroundColor Green
    Write-Host "Append one of these hints to the end of your query to test a fix:" -ForegroundColor DarkGray

    Write-Host ""
    Write-Host "-- Test 1: Generate a fresh plan for the specific runtime values (High CPU penalty if executed frequently)" -ForegroundColor Cyan
    Write-Host "OPTION (RECOMPILE);"

    Write-Host ""
    Write-Host "-- Test 2: Generate an 'average' plan using density vectors (Protects against edge cases, but ignores the histogram)" -ForegroundColor Cyan
    Write-Host "OPTION (OPTIMIZE FOR UNKNOWN);"

    Write-Host ""
    Write-Host "-- Test 3: Force the optimizer to use the original Compiled Values (Reproduces the original compiled plan behavior)" -ForegroundColor Cyan
    $optForList = @()
    foreach ($p in $psIssues) {
        $cleanC = $p.CompiledValue.Trim('(',')')
        $val = if ($cleanC -match "^[+-]?\d+(\.\d+)?$") { $cleanC } else { "N'$cleanC'" }
        $optForList += "$($p.Name) = $val"
    }
    Write-Host ("OPTION (OPTIMIZE FOR ({0}));" -f ($optForList -join ", "))
    
    Write-Host ""
    Write-Host "-- Test 4: Force the optimizer to use the current Runtime Values (To see if the new plan is definitively better)" -ForegroundColor Cyan
    $optForRuntimeList = @()
    foreach ($p in $psIssues) {
        $cleanR = $p.RuntimeValue.Trim('(',')')
        $val = if ($cleanR -match "^[+-]?\d+(\.\d+)?$") { $cleanR } else { "N'$cleanR'" }
        $optForRuntimeList += "$($p.Name) = $val"
    }
    Write-Host ("OPTION (OPTIMIZE FOR ({0}));" -f ($optForRuntimeList -join ", "))
    
  } else {
    # Check if this is an estimated plan (no runtime values) so we don't give a false negative
    $missingRuntime = @($parameterSensitivity | Where-Object { [string]::IsNullOrWhiteSpace($_.RuntimeValue) }).Count
    if ($missingRuntime -gt 0 -and @($parameterSensitivity).Count -gt 0) {
      Write-Host " - None found (Note: Plan lacks Runtime Values. Capture an Actual Execution Plan to verify)." -ForegroundColor DarkGray
    } else {
      Write-Host " - None found." -ForegroundColor DarkGray
    }
  }

  if ($parallelOps.Count -gt 0) {
    Write-Host ""
    Write-Host "Parallelism operators:" -ForegroundColor Yellow
    $result.ParallelismOps |
    Select-Object `
       @{ n = 'NodeId'; e = { $_.NodeId } },
    @{ n = 'PhysicalOp'; e = { $_.PhysicalOp } },
    @{ n = 'LogicalOp'; e = { $_.LogicalOp } },
    @{ n = 'EstCost'; e = { F2 $_.EstCost } },
    @{ n = 'EstRows'; e = { F0 $_.EstRows } },
    @{ n = 'ActOut'; e = { if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '') { F0 $_.ActRowsOut } else { '' } } },
    @{ n = 'ActWork'; e = { if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '') { F0 $_.ActRowsWork } else { '' } } },
    @{ n = 'Object'; e = { $_.Object } } |
    Format-Table -AutoSize
  }

  if ($cardinalityIssues.Count -gt 0) {
    Write-Host ""
    Write-Host "Inaccurate cardinality estimation (SSMS-style):" -ForegroundColor Yellow

    # Avoid repeating the same nodes already shown in the Top operators list.
    # (Top list includes EstRows/ActRows/Ratio now.)
    $ceFiltered = $cardinalityIssues | Where-Object { -not $topIds.ContainsKey($_.NodeId) }

    if (@($ceFiltered).Count -gt 0) {
      $ceFiltered |
      Select-Object -First 25 `
         @{ n = 'NodeId'; e = { $_.NodeId } },
      @{ n = 'PhysicalOp'; e = { $_.PhysicalOp } },
      @{ n = 'LogicalOp'; e = { $_.LogicalOp } },
      @{ n = 'EstRows'; e = { F0 $_.EstRows } },
      @{ n = 'ActOut'; e = { if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '') { F0 $_.ActRowsOut } else { '' } } },
      @{ n = 'ActWork'; e = { if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '') { F0 $_.ActRowsWork } else { '' } } },
      @{ n = 'RatioOut'; e = { if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsOut) / ([double]$_.EstRows)) } else { '' } } },
      @{ n = 'RatioWork'; e = { if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsWork) / ([double]$_.EstRows)) } else { '' } } },
      @{ n = 'RatioBest'; e = { if ($_.Ratio -eq [double]::PositiveInfinity) { "inf" } else { F3 $_.Ratio } } },
      @{ n = 'EstCost'; e = { F2 $_.EstCost } },
      @{ n = 'Access'; e = { $_.Access } },
      @{ n = 'Object'; e = { $_.Object } } |
      Format-Table -AutoSize
    } else {
      Write-Host " - (all CE-mismatch nodes are already listed above in Top operators; omitted)" -ForegroundColor DarkGray
    }

    Write-Host ""
    Write-Host "CE hot spots (grouped by statement):" -ForegroundColor DarkYellow
    $result.CEStatementSummary |
    Select-Object `
       StatementId,
    @{ n = 'WorstNode'; e = { $_.WorstNodeId } },
    @{ n = 'WorstRatio'; e = { if ($_.WorstRatio -eq [double]::PositiveInfinity) { "inf" } else { F3 $_.WorstRatio } } },
    @{ n = 'WorstSeverity'; e = { F3 $_.WorstSeverity } },
    NodesFlagged,
    StatementText |
    Format-Table -AutoSize

    Write-Host ""

    Write-Host "CE details (top 5 statements, top 3 nodes each):" -ForegroundColor DarkYellow
    Write-Host "  (ActRowsOut = trusted output rows where meaningful; ActRowsWork = summed worker rows)" -ForegroundColor DarkGray
    foreach ($stmt in ($result.CEStatementSummary | Select-Object -First 5)) {
      $isChosen = ($chosenStmtSummary -and $stmt.StatementId -eq $chosenStmtSummary.StatementId)
      if ($isChosen) {
        Write-Host ("StatementId {0}" -f $stmt.StatementId) -ForegroundColor Gray
      } else {
        Write-Host ("StatementId {0}: {1}" -f $stmt.StatementId,$stmt.StatementText) -ForegroundColor Gray
      }

      # De-dupe repeated detail lines (e.g., same predicate repeated on Top/Parallelism above the real scan/seek)
      $seen = @{}

      function Write-Once ([string]$label,[string]$value) {
        if ([string]::IsNullOrWhiteSpace($value)) { return }
        $k = "$label|$value"
        if (-not $seen.ContainsKey($k)) {
          Write-Host ("{0,-18}: {1}" -f $label,$value)
          $seen[$k] = $true
        }
      }

      $top = $cardinalityIssues |
      Where-Object { $_.StatementId -eq $stmt.StatementId } |
      Sort-Object Severity -Descending |
      Select-Object -First 3 |
      Sort-Object NodeId -Unique # safety: avoid duplicate NodeIds

      foreach ($n in $top) {
        Write-Host ""
        Write-Host ("NodeId             : {0}" -f $n.NodeId)
        Write-Host ("PhysicalOp         : {0}" -f $n.PhysicalOp)
        Write-Host ("EstRows            : {0}" -f (F0 $n.EstRows))
        $actOutTxt = if ($n.ActRowsOut -ne $null -and $n.ActRowsOut -ne '') { F0 $n.ActRowsOut } else { 'n/a' }
        $actWorkTxt = if ($n.ActRowsWork -ne $null -and $n.ActRowsWork -ne '') { F0 $n.ActRowsWork } else { 'n/a' }
        Write-Host ("ActRowsOut         : {0}" -f $actOutTxt)
        Write-Host ("ActRowsWork        : {0}" -f $actWorkTxt)

        # For parallel operators where output rows aren't a single counter, show per-thread ActualRows (readable + obvious)
        if ($n.ThreadRows -and @($n.ThreadRows).Count -gt 1) {
          $workers = $n.ThreadRows | Where-Object { $_.Thread -ne 0 }
          $wCount = @($workers).Count
          if ($wCount -gt 0) {
            $m = $workers | Measure-Object -Property ActualRows -Minimum -Maximum -Average
            $min = [double]$m.Minimum; $max = [double]$m.Maximum; $avg = [double]$m.Average
            $skew = if ($avg -gt 0) { F2 ($max / $avg) } else { 'n/a' }
            Write-Host ("Threads            : {0} workers (min={1}, avg={2}, max={3}, max/avg={4})" -f $wCount,(F0 $min),(F0 $avg),(F0 $max),$skew)

            # Compact per-thread list: show up to 8 busiest threads
            $topThr = $workers | Sort-Object ActualRows -Descending | Select-Object -First 8
            $pairs = @()
            foreach ($tr in $topThr) { $pairs += ("t{0}={1}" -f $tr.Thread,(F0 $tr.ActualRows)) }
            $more = $wCount - @($topThr).Count
            $suffix = if ($more -gt 0) { " ... +$more more" } else { "" }
            Write-Host ("PerThreadRows      : {0}{1}" -f ($pairs -join "  "),$suffix)
          }
        }

        $ratioOutTxt = if ($n.ActRowsOut -ne $null -and $n.EstRows -ne $null -and [double]$n.EstRows -ne 0) { F3 (([double]$n.ActRowsOut) / ([double]$n.EstRows)) } else { 'n/a' }
        $ratioWorkTxt = if ($n.ActRowsWork -ne $null -and $n.EstRows -ne $null -and [double]$n.EstRows -ne 0) { F3 (([double]$n.ActRowsWork) / ([double]$n.EstRows)) } else { 'n/a' }
        Write-Host ("RatioOut           : {0}" -f $ratioOutTxt)
        Write-Host ("RatioWork          : {0}" -f $ratioWorkTxt)

        if (-not [string]::IsNullOrWhiteSpace($n.LikelyContributors)) {
          Write-Host ("LikelyContributors : {0}" -f $n.LikelyContributors)
        }

        # Print predicates where they are most meaningful, and avoid repeated noise.
        $isAccess = ($n.PhysicalOp -match "Scan|Seek|Lookup")
        $isFilter = ($n.PhysicalOp -match "^Filter$")
        $isJoin = ($n.PhysicalOp -match "Nested Loops|Hash Match|Merge Join")

        if ($isAccess) {
          Write-Once "SeekPredicate" $n.SeekPredicate
          # Prefer Residual for scans/seeks; only show Predicate if it's distinct
          Write-Once "Residual" $n.Residual
          if (-not [string]::IsNullOrWhiteSpace($n.Predicate) -and $n.Predicate -ne $n.Residual) {
            Write-Once "Predicate" $n.Predicate
          }
        }
        elseif ($isFilter) {
          Write-Once "Predicate" $n.Predicate
        }
        elseif ($isJoin) {
          Write-Once "JoinPredicate" $n.JoinPredicate
        }
        # else: skip Predicate fields for wrapper ops (Top / Parallelism / etc.) to avoid duplication
      }

      Write-Host ""
    }
  }

  if ($result.Suggestions.Count -gt 0) {
    Write-Host ""
    Write-Host "Suggestions:" -ForegroundColor Green
    $result.Suggestions | ForEach-Object { Write-Host " - $_" }
  }

  if ($InspectDatabase -and $result.DbInspection) {
    Write-Host ""
    Write-Host ("Database inspection: {0}\{1}" -f $result.DbInspection.ServerInstance,$result.DbInspection.Database) -ForegroundColor Cyan

    if ($result.DbInspection.TargetTables.Count -gt 0) {
      Write-Host "Target tables:" -ForegroundColor DarkCyan
      $result.DbInspection.TargetTables | ForEach-Object { Write-Host (" - " + $_) }
    }

    if ($result.DbInspection.Indexes.Count -gt 0) {
      Write-Host ""
      Write-Host "Indexes (per target table):" -ForegroundColor Yellow
      $result.DbInspection.Indexes |
      Select-Object `
         FullTable,IndexId,IndexName,TypeDesc,IsUnique,IsPrimaryKey,HasFilter,
      @{ n = 'KeyCols'; e = { $_.KeyCols } },
      @{ n = 'IncludeCols'; e = { $_.IncludeCols } },
      UserSeeks,UserScans,UserLookups,UserUpdates |
      Format-Table -AutoSize
    }

    if ($result.DbInspection.Stats.Count -gt 0) {
      Write-Host ""
      Write-Host "Statistics (per target table):" -ForegroundColor Yellow
      $result.DbInspection.Stats |
      Select-Object `
         FullTable,StatsName,
      @{ n = 'LastUpdated'; e = { $_.LastUpdated } },
      @{ n = 'Rows'; e = { $_.Rows } },
      @{ n = 'Mods'; e = { $_.ModificationCounter } },
      @{ n = 'ModRatio'; e = {
          $rows = To-DoubleOrNull $_.Rows
          $mods = To-DoubleOrNull $_.ModificationCounter
          if ($rows -and $rows -gt 0 -and $mods -ne $null) { F3 ($mods / $rows) } else { "" }
        } },
      StatsCols,AutoCreated,UserCreated,NoRecompute |
      Format-Table -AutoSize
    }

    if ($result.DbInspection.StaleStats.Count -gt 0) {
      Write-Host ""
      Write-Host "Stale statistics candidates (heuristic):" -ForegroundColor DarkYellow
      $result.DbInspection.StaleStats |
      Select-Object `
         FullTable,StatsName,
      @{ n = 'LastUpdated'; e = { $_.LastUpdated } },
      @{ n = 'Rows'; e = { $_.Rows } },
      @{ n = 'Mods'; e = { $_.ModificationCounter } },
      @{ n = 'ModRatio'; e = { F3 $_.ModRatio } },
      @{ n = 'Days'; e = { F0 $_.DaysSinceUpdate } },
      StatsCols |
      Format-Table -AutoSize
      Write-Host "Tip: consider UPDATE STATISTICS (or sp_updatestats) for the highlighted stats after validating workload/maintenance windows." -ForegroundColor Gray
    }

    if ($result.DbInspection.MultiColumnStatsOpportunities.Count -gt 0) {
      Write-Host ""
      Write-Host "Multi-column statistics opportunities (from predicates; not covered by existing stats):" -ForegroundColor DarkYellow
      $result.DbInspection.MultiColumnStatsOpportunities |
      Select-Object FullTable,Columns,StatementId,ExampleNodeId,ExamplePredicate |
      Format-Table -AutoSize
      Write-Host "Tip: multi-column stats can help cardinality estimation when multiple columns are filtered together." -ForegroundColor Gray
    }

    if (@($result.DbInspection.MissingIndexCoverage).Count -gt 0) {
      Write-Host ""
      Write-Host "Missing-index coverage check (heuristic: does an existing index already cover it?):" -ForegroundColor Yellow
      $result.DbInspection.MissingIndexCoverage |
      Select-Object `
         FullTable,
      @{ n = 'Impact'; e = { F3 $_.Impact } },
      Occurrences,Duplicate,
      SuggestedKeys,SuggestedIncludes,CoveredByExistingIndex |
      Format-Table -AutoSize

    }
  }

  Write-Host ""
  Write-Host "Useful links:" -ForegroundColor Cyan
  foreach ($p in $result.ReferenceLinks.PSObject.Properties) {
    Write-Host (" - {0}: {1}" -f $p.Name,$p.Value)
  }

  # ---- make returned object display compact by default (but keep full data) ----
  $result.PSTypeNames.Insert(0,'SqlPlanInsights.Result')

  # Add some convenience count props for display (doesn't change underlying arrays)
  $result | Add-Member -NotePropertyName TopOperatorsCount -NotePropertyValue (@($result.TopOperators).Count) -Force
  $result | Add-Member -NotePropertyName CEIssuesCount -NotePropertyValue (@($result.CardinalityIssues).Count) -Force
  $result | Add-Member -NotePropertyName MissingIndexesCount -NotePropertyValue (@($result.MissingIndexes).Count) -Force
  $result | Add-Member -NotePropertyName JoinChecksCount -NotePropertyValue (@($result.JoinChecks).Count) -Force
  $result | Add-Member -NotePropertyName RedFlagsCount -NotePropertyValue (@($result.OperatorRedFlags).Count) -Force
  $result | Add-Member -NotePropertyName SargIssuesCount -NotePropertyValue (@($result.SargabilityIssues).Count) -Force
  $result | Add-Member -NotePropertyName IndexIntersectCount -NotePropertyValue (@($result.IndexIntersectionSignals).Count) -Force
  $result | Add-Member -NotePropertyName RewriteHintsCount -NotePropertyValue (@($result.RewriteHints).Count) -Force
  $result | Add-Member -NotePropertyName SpillOpsCount -NotePropertyValue (@($result.SpillSignals).Count) -Force
  $result | Add-Member -NotePropertyName ParallelSkewCount -NotePropertyValue (@($result.ParallelSkewSignals).Count) -Force
  $result | Add-Member -NotePropertyName ManyToManyMergeCount -NotePropertyValue (@($result.ManyToManyMergeSignals).Count) -Force
  $result | Add-Member -NotePropertyName PlanConvertCount -NotePropertyValue (@($result.PlanAffectingConvertSignals).Count) -Force
  $result | Add-Member -NotePropertyName KeyLookupsCount -NotePropertyValue (@($result.KeyLookups).Count) -Force
  $result | Add-Member -NotePropertyName SortOpsCount -NotePropertyValue (@($result.SortOps).Count) -Force
  $result | Add-Member -NotePropertyName ParallelismOpsCount -NotePropertyValue (@($result.ParallelismOps).Count) -Force
  $result | Add-Member -NotePropertyName SuggestionsCount -NotePropertyValue (@($result.Suggestions).Count) -Force
  $result | Add-Member -NotePropertyName InspectedDatabase -NotePropertyValue ([bool]($InspectDatabase -and $result.DbInspection)) -Force

  # Register a compact default view once per session
  if (-not (Get-TypeData -TypeName 'SqlPlanInsights.Result' -ErrorAction SilentlyContinue)) {
    Update-TypeData -TypeName 'SqlPlanInsights.Result' `
       -DefaultDisplayPropertySet @(
      'File','DegreeOfParallelism','TopOperatorsCount','CEIssuesCount',
      'MissingIndexesCount','KeyLookupsCount','SortOpsCount','ParallelismOpsCount',
      'JoinChecksCount','SpillOpsCount','ParallelSkewCount','ManyToManyMergeCount','PlanConvertCount',
      'SuggestionsCount','InspectedDatabase'
    )
  }


  return $result
}}
