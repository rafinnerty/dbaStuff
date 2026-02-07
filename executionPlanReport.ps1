<#
.SYNOPSIS
  Parse SQL Server ShowPlan XML (.sqlplan / .xml) and output performance insights.

.USAGE
  . .\Get-SqlPlanInsights.ps1
  Get-SqlPlanInsights -Path ".\Execution plan.xml"

  # More rows in top lists:
  Get-SqlPlanInsights -Path ".\Execution plan.xml" -TopOperators 25

  # Tweak CE mismatch sensitivity:
  Get-SqlPlanInsights -Path ".\Execution plan.xml" -CEMismatchRatio 5 -CEMinRows 50

  # Optional DB inspection (indexes, usage, overlap vs missing-index recs):
  Get-SqlPlanInsights -Path ".\Execution plan3.xml" -ServerInstance "rafinst\sql01" -Database "StackOverflow2013" -InspectDatabase

.NOTES
  Works with Estimated and Actual plans.
  Missing Index suggestions are taken from plan XML (even if SSMS doesn't show the green banner).
  All numeric output is formatted to 3 decimal places for alignment/readability.
#>

function Get-SqlPlanInsights {
  [CmdletBinding()]
  param(
    [Parameter(Position=0)]
    [string]$Path = 'C:\Users\dbsa\Documents\Execution plan.xml',

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

    # Diagnostics
    [switch]$IncludeOperatorRows,
    [switch]$SanityCheck,

    # Debug SARGability detection
    [switch]$DebugSargability
  )

  Write-Output 'v32-fixed13'
  if (-not (Test-Path -LiteralPath $Path)) { throw "File not found: $Path" }

  [xml]$xml = Get-Content -LiteralPath $Path -Raw
  $nsUri = $xml.DocumentElement.NamespaceURI
  if ([string]::IsNullOrWhiteSpace($nsUri)) { throw "Could not detect ShowPlan XML namespace. Is this a SQL Server plan file?" }

  $nsm = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
  $nsm.AddNamespace("sp", $nsUri)

  $rootForSelect = $xml

  function Normalize-XPath([string]$xpath) {
    if ([string]::IsNullOrWhiteSpace($xpath)) { return $xpath }
    # When we scope $rootForSelect to a <StmtSimple>, absolute XPaths (starting with / or //)
    # still evaluate from the document root. Rewrite them to be relative to the chosen statement.
    if ($xpath.StartsWith('//')) { return '.' + $xpath }
    if ($xpath.StartsWith('/'))  { return '.' + $xpath }
    return $xpath
  }

  function Select-Nodes([string]$xpath)  { $rootForSelect.SelectNodes((Normalize-XPath $xpath), $nsm) }
  function Select-Single([string]$xpath) { $rootForSelect.SelectSingleNode((Normalize-XPath $xpath), $nsm) }

  function To-DoubleOrNull([string]$s) {
  if ([string]::IsNullOrWhiteSpace($s)) { return $null }
  $t = $s.Trim()

  # ShowPlan often wraps numeric params in parentheses: (10000)
  $t = $t.Trim('(',')').Trim()

  # Strip quotes / N'...' (still wont parse datetimes, by design)
  if ($t -match "^[Nn]?'.*'$") {
    $t = $t -replace "^[Nn]?'", ""
    $t = $t.TrimEnd("'")
    $t = $t.Trim()
  }

  # Only parse if it looks numeric
  if ($t -notmatch '^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$') { return $null }

  try { return [double]$t } catch { return $null }
}

  function To-IntOrNull([string]$s)    { if ([string]::IsNullOrWhiteSpace($s)) { return $null }; try { [int]$s } catch { $null } }


# --- statement selection (when plan contains multiple statements) ---
$chosenStmt = $null
$chosenStmtSummary = $null
$stmtNodes = $xml.SelectNodes("//sp:StmtSimple", $nsm)
if ($stmtNodes -and @($stmtNodes).Count -gt 1) {
  $stmtScores = foreach ($s in $stmtNodes) {
    $sid = $s.GetAttribute("StatementId")
    $cost = To-DoubleOrNull ($s.GetAttribute("StatementSubTreeCost"))
    $pNodes = $s.SelectNodes(".//sp:ParameterList/sp:ColumnReference", $nsm)
    $runtimeCount = 0
    $mismatchCount = 0
    foreach ($p in $pNodes) {
      $cv = $p.GetAttribute("ParameterCompiledValue")
      $rv = $p.GetAttribute("ParameterRuntimeValue")
      if (-not [string]::IsNullOrWhiteSpace($rv)) { $runtimeCount++ }
      if (-not [string]::IsNullOrWhiteSpace($rv) -and -not [string]::IsNullOrWhiteSpace($cv) -and $rv -ne $cv) { $mismatchCount++ }
    }
    [pscustomobject]@{
      StatementId        = $sid
      Cost               = $cost
      RuntimeParamCount  = $runtimeCount
      ParamMismatchCount = $mismatchCount
      Node               = $s
    }
  }
  $chosen = $stmtScores |
    Sort-Object @{Expression='ParamMismatchCount';Descending=$true}, @{Expression='RuntimeParamCount';Descending=$true}, @{Expression='Cost';Descending=$true} |
    Select-Object -First 1
  $chosenStmt = $chosen.Node
} elseif ($stmtNodes -and @($stmtNodes).Count -eq 1) {
  $chosenStmt = $stmtNodes[0]
}

if ($chosenStmt) {
  $chosenStmtId = $chosenStmt.GetAttribute("StatementId")
  $chosenStmtText = $chosenStmt.GetAttribute("StatementText")
  if ($chosenStmtText -and $chosenStmtText.Length -gt 220) { $chosenStmtText = $chosenStmtText.Substring(0,220) + "..." }
  $chosenStmtSummary = [pscustomobject]@{ StatementId=$chosenStmtId; StatementText=$chosenStmtText }
  # Scope all subsequent XPath queries to the chosen statement
  $rootForSelect = $chosenStmt
}


  # --- numeric formatting helpers ---
  # Row counts: 0 decimal places
  function F0($v) {
    if ($null -eq $v) { return "" }
    if ($v -is [string] -and [string]::IsNullOrWhiteSpace($v)) { return "" }
    try { return ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, "{0:0}", [double]$v)) }
    catch { return "$v" }
  }

  # Non-row numeric values (costs, ratios, ms, KB, etc.): 2 decimal places
  function F2($v) {
    if ($null -eq $v) { return "" }
    if ($v -is [string] -and [string]::IsNullOrWhiteSpace($v)) { return "" }
    try { return ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, "{0:0.00}", [double]$v)) }
    catch { return "$v" }
  }

  # Back-compat: old name used throughout the script
  function F3($v) { return (F2 $v) }


  function Get-ScalarString($node) {
    if (-not $node) { return $null }
    $so = $node.SelectSingleNode(".//sp:ScalarOperator", $nsm)
    if ($so) {
      $ss = $so.GetAttribute("ScalarString")
      if (-not [string]::IsNullOrWhiteSpace($ss)) { return $ss }
    }
    return $null
  }

  function Get-WarningsText($relOpNode) {
    $warningsNode = $relOpNode.SelectSingleNode(".//sp:Warnings", $nsm)
    if (-not $warningsNode) { return $null }
    $warnFlags = @()
    foreach ($attr in $warningsNode.Attributes) {
      if ($attr.Value -eq "true") { $warnFlags += $attr.Name }
    }
    if ($warnFlags.Count -gt 0) { return ($warnFlags -join ",") }
    return $null
  }

  function Get-StatementInfo($relOpNode) {
    $stmt = $relOpNode.SelectSingleNode("ancestor::sp:StmtSimple[1]", $nsm)
    if (-not $stmt) { return $null }
    $text = $stmt.GetAttribute("StatementText")
    if ($text -and $text.Length -gt 220) { $text = $text.Substring(0,220) + "..." }
    return [pscustomobject]@{
      StatementId   = $stmt.GetAttribute("StatementId")
      StatementText = $text
    }
  }

  # More robust: return up to 2 distinct objects in the subtree (helps joins show both sides)
  function Get-ObjectNames($relOpNode, [int]$max = 2) {
    $objs = $relOpNode.SelectNodes(".//sp:Object", $nsm)
    if (-not $objs -or @($objs).Count -eq 0) { return "" }

    $names = New-Object System.Collections.Generic.List[string]
    foreach ($o in $objs) {
      $db     = $o.GetAttribute("Database")
      $schema = $o.GetAttribute("Schema")
      $table  = $o.GetAttribute("Table")
      $index  = $o.GetAttribute("Index")

      $base = (@($db,$schema,$table) | Where-Object { $_ -and $_ -ne "" }) -join "."
      if ([string]::IsNullOrWhiteSpace($base)) { continue }
      if ($index -and $index -ne "") { $base = "$base ($index)" }

      if (-not $names.Contains($base)) { $names.Add($base) }
      if ($names.Count -ge $max) { break }
    }

    return ($names -join " | ")
  }

  function Get-RunTimeSum($relOpNode) {
    $rt = $relOpNode.SelectNodes("./sp:RunTimeInformation/sp:RunTimeCountersPerThread", $nsm)
    if (-not $rt -or @($rt).Count -eq 0) { return $null }

    # Use arrays for keys to avoid "Collection was modified..." edge cases
    $sum = [ordered]@{
      ActualRows        = 0
      ActualExecutions  = 0
      ActualRowsRead    = 0
      ActualRebinds     = 0
      ActualRewinds     = 0
      ActualEndOfScans  = 0
    }
    $keys = @($sum.Keys)

    $has = @{}
    foreach ($k in $keys) { $has[$k] = $false }

    # Collect per-thread ActualRows for readability/skew analysis
    $threadRows = @()

    foreach ($t in $rt) {
      $thr = To-IntOrNull ($t.GetAttribute("Thread"))
      $ar  = To-DoubleOrNull ($t.GetAttribute("ActualRows"))
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
    $log  = $relOpNode.GetAttribute("LogicalOp")
    $par  = $relOpNode.GetAttribute("Parallel")

    $trustT0 = $false
    if ($par -eq "false") { $trustT0 = $true }
    elseif ($phys -eq "Parallelism" -and $log -match "Gather Streams") { $trustT0 = $true }

    $sumActualRowsWork = $sum.ActualRows
    $sumActualRowsOut  = $null
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
    $o['ActualRowsOut']  = $sumActualRowsOut

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

  function Get-NonSargableFlags([string]$s) {
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
function Get-SeekPredicatePretty($ixNode) {
  if (-not $ixNode) { return $null }

  function Format-ColumnRef($cr) {
    if (-not $cr) { return $null }
    $d = $cr.GetAttribute("Database")
    $s = $cr.GetAttribute("Schema")
    $t = $cr.GetAttribute("Table")
    $c = $cr.GetAttribute("Column")
    $parts = @($d,$s,$t,$c) | Where-Object { $_ -and $_ -ne "" }
    if ($parts.Count -eq 0) { return $null }
    return "[" + ($parts -join "].[") + "]"
  }

  $preds = $ixNode.SelectNodes(".//sp:SeekPredicateNew", $nsm)
  if (-not $preds -or @($preds).Count -eq 0) { return $null }

  $clauses = @()
  foreach ($p in @($preds)) {
    $sk = $p.SelectSingleNode(".//sp:SeekKeys", $nsm)
    if (-not $sk) { continue }

    $startCols  = $sk.SelectNodes(".//sp:StartRange/sp:RangeColumns/sp:ColumnReference", $nsm)
    $startExprs = $sk.SelectNodes(".//sp:StartRange/sp:RangeExpressions/sp:ScalarOperator", $nsm)
    $endCols    = $sk.SelectNodes(".//sp:EndRange/sp:RangeColumns/sp:ColumnReference", $nsm)
    $endExprs   = $sk.SelectNodes(".//sp:EndRange/sp:RangeExpressions/sp:ScalarOperator", $nsm)

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
              $clauses += ("{0} = {1}" -f $col, $start)
            } else {
              $clauses += ("{0} BETWEEN {1} AND {2}" -f $col, $start, $end)
            }
            continue
          }
        }
        $clauses += ("{0} >= {1}" -f $col, $start)
      }
    }
  }

  if ($clauses.Count -gt 0) { return ($clauses -join " AND ") }
  return $null
}
  function Get-AccessDetails($relOpNode) {
    $ixSeek  = $relOpNode.SelectSingleNode("./sp:IndexSeek", $nsm)
    $ixScan  = $relOpNode.SelectSingleNode("./sp:IndexScan", $nsm)
    $tblScan = $relOpNode.SelectSingleNode("./sp:TableScan", $nsm)

    $seekPred = $null
    $residualPred = $null
    $kind = $null

    if ($ixSeek) {
      $kind = "Index Seek"
      $seekPred     = Get-SeekPredicatePretty $ixSeek
      if (-not $seekPred) { $seekPred = Get-ScalarString ($ixSeek.SelectSingleNode(".//sp:SeekPredicates", $nsm)) }
      $residualPred = Get-ScalarString ($ixSeek.SelectSingleNode(".//sp:Predicate", $nsm))
    }
    elseif ($ixScan) {
      $kind = "Index Scan"
      $seekPred     = Get-SeekPredicatePretty $ixScan
      if (-not $seekPred) { $seekPred = Get-ScalarString ($ixScan.SelectSingleNode(".//sp:SeekPredicates", $nsm)) }
      $residualPred = Get-ScalarString ($ixScan.SelectSingleNode(".//sp:Predicate", $nsm))
    }
    elseif ($tblScan) {
      $kind = "Table Scan"
      $residualPred = Get-ScalarString ($tblScan.SelectSingleNode(".//sp:Predicate", $nsm))
    }

    return [pscustomobject]@{
      AccessType = $kind
      SeekPredicate = $seekPred
      Residual = $residualPred
    }
  }

  # For operators that don't have a seek/scan, give a useful category so "Access" isn't blank
  function Get-AccessCategory($physicalOp, $accessType) {
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

  function Get-JoinDetails($relOpNode) {
    $joinPred = $null
    $hash = $relOpNode.SelectSingleNode(".//sp:Hash", $nsm)
    if ($hash) {
      $joinPred = Get-ScalarString ($hash.SelectSingleNode(".//sp:ProbeResidual", $nsm))
      if (-not $joinPred) { $joinPred = Get-ScalarString ($hash.SelectSingleNode(".//sp:HashKeysBuild", $nsm)) }
      if (-not $joinPred) { $joinPred = Get-ScalarString ($hash.SelectSingleNode(".//sp:HashKeysProbe", $nsm)) }
    }
    $nl = $relOpNode.SelectSingleNode(".//sp:NestedLoops", $nsm)
    if ($nl -and -not $joinPred) { $joinPred = Get-ScalarString ($nl.SelectSingleNode(".//sp:Predicate", $nsm)) }
    $mj = $relOpNode.SelectSingleNode(".//sp:Merge", $nsm)
    if ($mj -and -not $joinPred) { $joinPred = Get-ScalarString ($mj.SelectSingleNode(".//sp:Residual", $nsm)) }
    return $joinPred
  }

  function Get-SortKeys($relOpNode) {
    $sort = $relOpNode.SelectSingleNode(".//sp:Sort", $nsm)
    if (-not $sort) { return $null }

    $keyRefs = $sort.SelectNodes(".//sp:OrderByColumn/sp:ColumnReference", $nsm)
    if (-not $keyRefs -or @($keyRefs).Count -eq 0) { $keyRefs = $sort.SelectNodes(".//sp:SortColumn/sp:ColumnReference", $nsm) }
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
  function Get-ThreadTeamSize([System.Xml.XmlNode]$relOp) {
    if (-not $relOp) { return $null }
    $threads = $relOp.SelectNodes("./sp:RunTimeInformation/sp:RunTimeCountersPerThread", $nsm)
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

  $runtimeOps = Select-Nodes("//sp:RelOp[sp:RunTimeInformation/sp:RunTimeCountersPerThread]")
  if ($runtimeOps -and @($runtimeOps).Count -gt 0) {
    $dopObservedMaxRuntime = 1
    foreach ($r in @($runtimeOps)) {
      $sz = Get-ThreadTeamSize $r
      if ($sz -ne $null -and $sz -gt $dopObservedMaxRuntime) { $dopObservedMaxRuntime = $sz }
    }

    $parOps = Select-Nodes("//sp:RelOp[@PhysicalOp='Parallelism' and sp:RunTimeInformation/sp:RunTimeCountersPerThread]")
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
      GrantedKB   = To-DoubleOrNull ($mg.GetAttribute("GrantedMemory"))
      UsedKB      = To-DoubleOrNull ($mg.GetAttribute("UsedMemory"))
      MaxUsedKB   = To-DoubleOrNull ($mg.GetAttribute("MaxUsedMemory"))
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

  # First pass: CONVERT_IMPLICIT markers (per-node)
  $scalarNodes = Select-Nodes "//sp:ScalarOperator"
  $conversionNodeIds = @{}
  foreach ($s in $scalarNodes) {
    $scalarString = $s.GetAttribute("ScalarString")
    $outer = $s.OuterXml
    if ($outer -match "CONVERT_IMPLICIT" -or $scalarString -match "CONVERT_IMPLICIT") {
      $relOp = $s.SelectSingleNode("ancestor::sp:RelOp[1]", $nsm)
      if ($relOp) {
        $nid = $relOp.GetAttribute("NodeId")
        if ($nid -ne "") { $conversionNodeIds["$nid"] = $true }
      }
    }
  }

  $operatorRows = @(
    foreach ($op in $relOps) {
      $nodeId   = To-IntOrNull ($op.GetAttribute("NodeId"))
      $physical = $op.GetAttribute("PhysicalOp")
      $logical  = $op.GetAttribute("LogicalOp")
      $estRows  = To-DoubleOrNull ($op.GetAttribute("EstimateRows"))
      $estCost  = To-DoubleOrNull ($op.GetAttribute("EstimatedTotalSubtreeCost"))

      $warn = Get-WarningsText $op
      $st = Get-StatementInfo $op

      $rt = Get-RunTimeSum $op
      $actRows = $null; $actRowsOut = $null; $actRowsWork = $null
      $threadRows = $null; $workerThreads = $null; $tMin = $null; $tMax = $null; $tAvg = $null; $tSkewMA = $null; $tSkewMM = $null
      $execs = $null; $rowsRead = $null; $rebinds = $null; $rewinds = $null
      if ($rt) {
        if ($rt.PSObject.Properties.Name -contains "ActualRows")       { $actRows = [double]$rt.ActualRows }
        if ($rt.PSObject.Properties.Name -contains "ActualRowsOut")    { $actRowsOut = if ($rt.ActualRowsOut -ne $null -and $rt.ActualRowsOut -ne '') { [double]$rt.ActualRowsOut } else { $null } }
        if ($rt.PSObject.Properties.Name -contains "ActualRowsWork")   { $actRowsWork = if ($rt.ActualRowsWork -ne $null -and $rt.ActualRowsWork -ne '') { [double]$rt.ActualRowsWork } else { $null } }
        if ($rt.PSObject.Properties.Name -contains "ThreadRows")       { $threadRows = $rt.ThreadRows }
        if ($rt.PSObject.Properties.Name -contains "WorkerThreadCount"){ $workerThreads = [int]$rt.WorkerThreadCount }
        if ($rt.PSObject.Properties.Name -contains "ThreadMinRows")    { $tMin = $rt.ThreadMinRows }
        if ($rt.PSObject.Properties.Name -contains "ThreadMaxRows")    { $tMax = $rt.ThreadMaxRows }
        if ($rt.PSObject.Properties.Name -contains "ThreadAvgRows")    { $tAvg = $rt.ThreadAvgRows }
        if ($rt.PSObject.Properties.Name -contains "ThreadSkewMaxAvg") { $tSkewMA = $rt.ThreadSkewMaxAvg }
        if ($rt.PSObject.Properties.Name -contains "ThreadSkewMaxMin") { $tSkewMM = $rt.ThreadSkewMaxMin }
        if ($rt.PSObject.Properties.Name -contains "ActualExecutions") { $execs   = [double]$rt.ActualExecutions }
        if ($rt.PSObject.Properties.Name -contains "ActualRowsRead")   { $rowsRead= [double]$rt.ActualRowsRead }
        if ($rt.PSObject.Properties.Name -contains "ActualRebinds")    { $rebinds = [double]$rt.ActualRebinds }
        if ($rt.PSObject.Properties.Name -contains "ActualRewinds")    { $rewinds = [double]$rt.ActualRewinds }
      }

      $executed = $null
      if ($execs -ne $null) { $executed = ($execs -gt 0) }

      $ratio = $null
      $direction = $null
      if ($estRows -ne $null -and $actRows -ne $null -and $estRows -ge 0 -and $actRows -ge 0) {
        if ($estRows -eq 0 -and $actRows -eq 0) { $ratio = 1.0 }
        elseif (($estRows -eq 0 -and $actRows -gt 0) -or ($actRows -eq 0 -and $estRows -gt 0)) { $ratio = [double]::PositiveInfinity }
        elseif ($estRows -gt 0 -and $actRows -gt 0) {
          $ratio = [math]::Max($estRows / $actRows, $actRows / $estRows)
          $direction = if ($actRows -gt $estRows) { "under-est" } else { "over-est" }
        }
      }

      $severity = $null
      if ($ratio -ne $null -and $ratio -ne [double]::PositiveInfinity -and $estRows -ne $null -and $actRows -ne $null -and $estCost -ne $null) {
        $scale = [math]::Log10([math]::Max($estRows, $actRows) + 1)
        $severity = ([math]::Log10($ratio) * ($scale + 1) * (1 + $estCost))
      }

      $access = Get-AccessDetails $op
      $predNode = $op.SelectSingleNode(
        "./sp:Predicate | ./sp:Filter/sp:Predicate | ./sp:IndexScan/sp:Predicate | ./sp:IndexSeek/sp:Predicate | ./sp:TableScan/sp:Predicate",
        $nsm
      )
      $predCtx = Get-ScalarString $predNode
      $nonSarg = Get-NonSargableFlags ($predCtx + " " + $access.SeekPredicate + " " + $access.Residual)

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
        NodeId        = $nodeId
        StatementId   = if ($st) { $st.StatementId } else { "" }
        StatementText = if ($st) { $st.StatementText } else { "" }

        PhysicalOp = $physical
        LogicalOp  = $logical

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
        Execs   = $execs
        Rebinds = $rebinds
        Rewinds = $rewinds
        Executed = $executed

        Ratio = $ratio
        Direction = $direction
        Severity = $severity

        EstCost = $estCost
        Object  = $objNames
        Warnings = $warn

        Access = $accessCategory
        SeekPredicate = $access.SeekPredicate
        Residual = $access.Residual
        Predicate = $predCtx
        NonSargableHints = $nonSarg

        HasConvertImplicit = if ($nodeId -ne $null -and $conversionNodeIds.ContainsKey("$nodeId")) { $true } else { $false }

        SortKeys = (Get-SortKeys $op)
        JoinPredicate = (Get-JoinDetails $op)
      }
    }
  )

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
    $k = ("{0}|{1}" -f [string]$r.StatementId, [string]$r.NodeId)
    $costByKey[$k] = [double]$r.EstCost
  }
}

function Get-NearestRelOpAncestor($node) {
  $p = $node.ParentNode
  while ($p -and $p.LocalName -ne 'RelOp') { $p = $p.ParentNode }
  return $p
}

function Get-DirectChildRelOps($relOpNode) {
  # Return RelOp descendants whose nearest RelOp ancestor is $relOpNode (i.e., direct children in the logical tree)
  $kids = @()
  $desc = $relOpNode.SelectNodes(".//*[local-name()='RelOp']", $nsm)
  if (-not $desc -or @($desc).Count -eq 0) { return $kids }

  foreach ($d in $desc) {
    # skip self (SelectNodes includes descendants only, but be safe)
    if ($d -eq $relOpNode) { continue }

    $nearest = Get-NearestRelOpAncestor $d
    if ($nearest -eq $relOpNode) { $kids += $d }
  }
  return $kids
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

  $pkTmp = ("{0}|{1}" -f $stmtIdKey, [string]$nodeIdKey)
  $hasKidsByKey[$pkTmp] = $true

  $maxChild = $null
  foreach ($k in $kids) {
    $cid = $k.GetAttribute("NodeId")
    if ([string]::IsNullOrWhiteSpace($cid)) { continue }

    $ck = ("{0}|{1}" -f $stmtIdKey, [string]$cid)
    if ($costByKey.ContainsKey($ck)) {
      $c = [double]$costByKey[$ck]
      if ($maxChild -eq $null -or $c -gt $maxChild) { $maxChild = $c }
    }
  }

  if ($maxChild -ne $null) {
    $pk = ("{0}|{1}" -f $stmtIdKey, [string]$nodeIdKey)
    $childMaxByKey[$pk] = $maxChild
  }
}

foreach ($r in $operatorRows) {
  if ($r.NodeId -eq $null -or $r.EstCost -eq $null) {
    $r | Add-Member -NotePropertyName HasKids -NotePropertyValue $false -Force
    $r | Add-Member -NotePropertyName EstSelfCost -NotePropertyValue $null -Force
    continue
  }

  $pk = ("{0}|{1}" -f [string]$r.StatementId, [string]$r.NodeId)
  $tot = [double]$r.EstCost
  $childMax = $null
  if ($childMaxByKey.ContainsKey($pk)) { $childMax = [double]$childMaxByKey[$pk] }

  $hasKids = $false
  if ($hasKidsByKey.ContainsKey($pk)) { $hasKids = [bool]$hasKidsByKey[$pk] }

  $self = if ($childMax -eq $null) {
    if ($hasKids) { 0.0 } else { $tot }
  } else {
    [math]::Max(0.0, ($tot - $childMax))
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
  $qp = Select-Single("//sp:QueryPlan[@ParameterSensitivePlan='true']")
  if ($qp) { $hasParameterSensitivePlan = $true }

  $paramNodes = Select-Nodes("//sp:ParameterList/sp:ColumnReference")
  foreach ($p in $paramNodes) {
    $name = $p.GetAttribute("Column")
    if ([string]::IsNullOrWhiteSpace($name)) { $name = $p.GetAttribute("Parameter") }
    $compiled = $p.GetAttribute("ParameterCompiledValue")
    $runtime  = $p.GetAttribute("ParameterRuntimeValue")
    $ptype    = $p.GetAttribute("ParameterDataType")
    if ([string]::IsNullOrWhiteSpace($ptype)) { $ptype = $p.GetAttribute("ParameterType") }

    $cNum = To-DoubleOrNull $compiled
    $rNum = To-DoubleOrNull $runtime

    $ratio = $null
    if ($cNum -ne $null -and $rNum -ne $null -and $cNum -ne 0) {
      $ratio = [math]::Abs($rNum / $cNum)
      if ($ratio -lt 1) { $ratio = 1 / $ratio }
    }

    $parameterSensitivity += [pscustomobject]@{
      Name          = $name
      DataType      = $ptype
      CompiledValue = $compiled
      RuntimeValue  = $runtime
      ValueRatio    = $ratio
    }
  }


  # High-risk parameter sensitivity candidates (needs both compiled + runtime values)
  $psIssues = @($parameterSensitivity | Where-Object {
    $_.RuntimeValue -and $_.CompiledValue -and $_.ValueRatio -ne $null -and $_.ValueRatio -ge 2
  })
  # Potential multi-column stats candidates from predicates (per table)
  function Get-ColumnRefsFromExpr([string]$expr) {
    $out = @()
    if ([string]::IsNullOrWhiteSpace($expr)) { return $out }
    $re = [regex]'\[(?<db>[^\]]+)\]\.\[(?<sch>[^\]]+)\]\.\[(?<tbl>[^\]]+)\]\.\[(?<col>[^\]]+)\]'
    foreach ($m in $re.Matches($expr)) {
      $fullTable = "[{0}].[{1}].[{2}]" -f $m.Groups["db"].Value, $m.Groups["sch"].Value, $m.Groups["tbl"].Value
      $col = "[{0}]" -f $m.Groups["col"].Value
      $out += [pscustomobject]@{ FullTable=$fullTable; Column=$col }
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
          Columns   = $cols
          ColumnsText = ($cols -join ", ")
          NodeId    = $op.NodeId
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
      ([math]::Max($_.ActRows, $_.EstRows) -ge $CEMinRows)
    } |
    ForEach-Object {
      $likely = @()
      if ($_.HasConvertImplicit) { $likely += "implicit conversion (CONVERT_IMPLICIT)" }
      if ($_.NonSargableHints)  { 
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
        StatementId   = $_.Name
        WorstNodeId   = $worst.NodeId
        WorstRatio    = $worst.Ratio
        WorstSeverity = $worst.Severity
        NodesFlagged  = $_.Count
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
      $mi = $g.SelectSingleNode(".//sp:MissingIndex", $nsm)
      if (-not $mi) { continue }

      $db     = $mi.GetAttribute("Database")
      $schema = $mi.GetAttribute("Schema")
      $table  = $mi.GetAttribute("Table")
      $fullTable = (@($db,$schema,$table) | Where-Object { $_ -and $_ -ne "" }) -join "."

      $equalityCols   = @()
      $inequalityCols = @()
      $includeCols    = @()

      foreach ($cg in $mi.SelectNodes(".//sp:ColumnGroup", $nsm)) {
        $usage = $cg.GetAttribute("Usage")
        $cols = @()
        foreach ($c in $cg.SelectNodes(".//sp:Column", $nsm)) { $cols += ($c.GetAttribute("Name") -replace '^\[|\]$','') }
        switch ($usage) {
          "EQUALITY"   { $equalityCols   += $cols }
          "INEQUALITY" { $inequalityCols += $cols }
          "INCLUDE"    { $includeCols    += $cols }
        }
      }

      $keyCols = @()
      if ($equalityCols.Count -gt 0)   { $keyCols += $equalityCols }
      if ($inequalityCols.Count -gt 0) { $keyCols += $inequalityCols }

      $safeTable = ($table -replace '[\[\]]','')
      $ixName = "IX_Tune_{0}_{1}" -f $safeTable, ([guid]::NewGuid().ToString("N").Substring(0,8))

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
        Signature = "{0}|{1}|{2}|{3}" -f $fullTable, ($equalityCols -join ","), ($inequalityCols -join ","), ($includeCols -join ",")
      }
    }
)

  $missingIndexes = @($missingIndexes | Sort-Object Impact -Descending)

  $missingIndexDuplicates = $missingIndexes |
    Group-Object Signature |
    Where-Object { $_.Count -gt 1 } |
    ForEach-Object {
      [pscustomobject]@{
        FullTable = $_.Group[0].FullTable
        Count     = $_.Count
        MaxImpact = ($_.Group | Measure-Object Impact -Maximum).Maximum
      }
    } | Sort-Object MaxImpact -Descending

  # A unique set of missing-index recommendations (deduped by Signature) for display / CREATE INDEX examples.
  # Keep $missingIndexes intact (it reflects raw plan XML, and duplicates are meaningful).
  $missingIndexesUnique = $missingIndexes |
    Group-Object Signature |
    ForEach-Object { $_.Group | Sort-Object Impact -Descending | Select-Object -First 1 } |
    Sort-Object Impact -Descending
  $missingIndexesUniqueCount = @($missingIndexesUnique).Count

  
  # -----------------------------
  # DBA-focused heuristics
  # -----------------------------
  $joinChecks = @()
  foreach ($op in $operatorRows | Where-Object { $_.LogicalOp -match "Join" -or $_.PhysicalOp -match "Join|Nested Loops|Hash Match|Merge Join|Adaptive Join" }) {
    $rows = if ($op.ActRows -ne $null) { $op.ActRows } else { $op.EstRows }
    $execs = if ($op.Execs -ne $null -and $op.Execs -gt 0) { $op.Execs } else { $null }

    if ($op.PhysicalOp -eq "Nested Loops" -and $rows -ne $null -and $rows -ge 10000) {
      $joinChecks += [pscustomobject]@{
        NodeId = $op.NodeId
        Join   = $op.PhysicalOp
        Signal = "large join output"
        Detail = "Nested Loops with output rows $rows. If outer input is large, consider Hash/Merge join or indexing to support seeks."
      }
    }
    if ($op.PhysicalOp -eq "Merge Join" -and $sortOps.Count -gt 0) {
      $joinChecks += [pscustomobject]@{
        NodeId = $op.NodeId
        Join   = $op.PhysicalOp
        Signal = "merge join + sort(s)"
        Detail = "Merge Join often wants ordered inputs. Sorts present; consider indexes on join keys to provide order and avoid sorts."
      }
    }
    if ($op.PhysicalOp -eq "Hash Match" -and $rows -ne $null -and $rows -ge 200000) {
      $joinChecks += [pscustomobject]@{
        NodeId = $op.NodeId
        Join   = $op.PhysicalOp
        Signal = "large hash join"
        Detail = "Hash join with output rows $rows. Check memory grant, spills, and predicate selectivity."
      }
    }
    if ($op.PhysicalOp -eq "Adaptive Join") {
      $joinChecks += [pscustomobject]@{
        NodeId = $op.NodeId
        Join   = $op.PhysicalOp
        Signal = "adaptive join"
        Detail = "Adaptive Join chosen. Ensure statistics are good; parameter sensitivity can push it to a suboptimal join type. If CE mismatches exist, the join algorithm can flip across executions."
      }
    }
  }
  $joinChecks = @($joinChecks | Sort-Object NodeId -Unique)

  

  # Many-to-many Merge Join detection (ShowPlan attribute)
  $manyToManyMergeSignals = @()
  try {
    $m2mMergeNodes = Select-Nodes(".//sp:RelOp[@PhysicalOp='Merge Join']//sp:Merge[@ManyToMany='true' or @ManyToMany='1']")
    foreach ($m in @($m2mMergeNodes)) {
      $rel = $m.SelectSingleNode("ancestor::sp:RelOp[1]", $nsm)
      if ($rel -ne $null) {
        $nid = [int]$rel.GetAttribute("NodeId")
        $manyToManyMergeSignals += [pscustomobject]@{
          NodeId = $nid
          Join   = "Merge Join"
          Signal = "many-to-many merge join"
          Detail = "Many-to-many Merge Join detected. This can build worktables and amplify rows/memory. Consider join keys/order, indexes, and predicate selectivity."
        }
      }
    }
  } catch { }

  if (@($manyToManyMergeSignals).Count -gt 0) {
    # Add to joinChecks (avoid duplicates)
    $joinChecks += $manyToManyMergeSignals
    $joinChecks = @($joinChecks | Sort-Object NodeId -Unique)
  }

  # Spill detection (operator-level) from Warnings subtree
  $spillSignals = @()
  try {
    $spillRelOps = Select-Nodes(".//sp:RelOp[.//sp:Warnings//*[contains(translate(local-name(),'SPILL','spill'),'spill')]]")
    foreach ($r in @($spillRelOps)) {
      $nid = [int]$r.GetAttribute("NodeId")
      $phys = $r.GetAttribute("PhysicalOp")
      if ($phys -notmatch '(?i)sort' -and $phys -notmatch '(?i)hash match') { continue }
      $warnNodes = $r.SelectNodes(".//sp:Warnings//*", $nsm)
      $names = @()
      foreach ($wn in @($warnNodes)) {
        $ln = $wn.LocalName
        if ($ln -match "(?i)spill") { $names += $ln }
      }
      $names = @($names | Sort-Object -Unique)
      $detail = if ($names.Count -gt 0) { "Warnings: " + ($names -join ", ") } else { "Spill warning detected under Warnings node." }
      $spillSignals += [pscustomobject]@{
        NodeId   = $nid
        Operator = $phys
        Signal   = "spill to tempdb"
        Detail   = $detail
      }
    }
  } catch { }

  # Parallelism skew detection (ActualRows imbalance across threads)
  $parallelSkewSignals = @()
  $skewScanned = 0
  try {
    $rtRelOps = Select-Nodes(".//sp:RelOp[sp:RunTimeInformation/sp:RunTimeCountersPerThread]")
    foreach ($r in @($rtRelOps)) {
      $threads = $r.SelectNodes("./sp:RunTimeInformation/sp:RunTimeCountersPerThread", $nsm)
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
          NodeId        = [int]$r.GetAttribute("NodeId")
          Operator      = $r.GetAttribute("PhysicalOp")
          Signal        = "parallelism skew"
          Detail        = ("Max thread rows {0}, Avg {1}, SkewRatio {2}" -f (F0 $max), (F0 $avg), (F2 $ratio))
          SkewRatio     = $ratio
          TotalRows     = $total
          Threads       = $rows.Count
        }
      }
    }
  } catch { }

    # PlanAffectingConvert / implicit conversion signals
  $planAffectingConvertSignals = @()
  try {
    $pacs = Select-Nodes(".//sp:PlanAffectingConvert")
    foreach ($pac in @($pacs)) {
      $issue = $pac.GetAttribute("ConvertIssue")
      if ([string]::IsNullOrWhiteSpace($issue)) { $issue = $pac.GetAttribute("ConvertIssueType") }
      $expr  = $pac.GetAttribute("Expression")
      if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $pac.InnerText }
      if ($expr -and $expr.Length -gt 220) { $expr = $expr.Substring(0,220) + "..." }
      $planAffectingConvertSignals += [pscustomobject]@{
        StatementId = $chosenStatementId
        Issue       = $issue
        Expression  = $expr
        Detail      = "Plan-affecting convert detected. Check parameter/column datatypes and implicit conversions (can hurt seeks and cardinality estimation)."
      }
    }
  } catch { }

# Operator red flags
  $operatorRedFlags = @()
  foreach ($op in $operatorRows) {
    $rows = if ($op.ActRows -ne $null) { $op.ActRows } else { $op.EstRows }

    if ($op.PhysicalOp -match "Sort" -and $rows -ne $null -and $rows -ge 100000) {
      $operatorRedFlags += [pscustomobject]@{ NodeId=$op.NodeId; Operator=$op.PhysicalOp; Signal="large sort"; Detail="Sort on ~$rows rows. Consider index/order, reduce rowset, or memory grant/spill review." }
    }
    if ($op.PhysicalOp -match "Spool") {
      $operatorRedFlags += [pscustomobject]@{ NodeId=$op.NodeId; Operator=$op.PhysicalOp; Signal="spool"; Detail="Spool can indicate repeated work. Check Execs, joins, correlated subqueries, and indexing." }
    }
    if ($op.PhysicalOp -match "Table Spool" -and $op.Execs -ne $null -and $op.Execs -ge 1000) {
      $operatorRedFlags += [pscustomobject]@{ NodeId=$op.NodeId; Operator=$op.PhysicalOp; Signal="high rebinds"; Detail="Table Spool with Execs=$($op.Execs). Investigate nested loops rebinds / parameter sensitivity." }
    }
    if ($op.PhysicalOp -match "Hash Match" -and $rows -ne $null -and $rows -ge 200000) {
      $operatorRedFlags += [pscustomobject]@{ NodeId=$op.NodeId; Operator=$op.PhysicalOp; Signal="large hash"; Detail="Hash operator on ~$rows rows. Check memory grant and spills/tempdb." }
    }
    if ($op.PhysicalOp -match "Bitmap") {
      $operatorRedFlags += [pscustomobject]@{ NodeId=$op.NodeId; Operator=$op.PhysicalOp; Signal="bitmap / index intersection"; Detail="Bitmap usage can indicate index intersection. A composite index may be better." }
    }
  }
  $operatorRedFlags = @($operatorRedFlags | Sort-Object NodeId)

  # Predicate SARGability checks (heuristic)
  $sargabilityIssues = @()
  function Add-SargIssue([int]$nodeId, [string]$kind, [string]$expr) {
    if ([string]::IsNullOrWhiteSpace($expr)) { return }
    $short = if ($expr.Length -gt 220) { $expr.Substring(0,220) + "..." } else { $expr }
    $sargabilityIssues += [pscustomobject]@{ NodeId=$nodeId; Issue=$kind; Expression=$short }
  }

  foreach ($op in $operatorRows) {
    foreach ($field in @("SeekPredicate","Predicate","Residual")) {
      $expr = $op.$field
      if ([string]::IsNullOrWhiteSpace($expr)) { continue }

      if ($expr -match "CONVERT_IMPLICIT") { Add-SargIssue $op.NodeId "implicit conversion (non-SARGable)" $expr }
      if ($expr -match "(?i)\bLIKE\s+N?'%[^']*'") { Add-SargIssue $op.NodeId "leading wildcard LIKE (non-SARGable)" $expr }
      if ($expr -match "(?i)\b(UPPER|LOWER|SUBSTRING|LEFT|RIGHT|DATEADD|DATEDIFF|DATEPART|DATENAME|YEAR|MONTH|DAY|TRY_CONVERT|TRY_CAST|CONVERT|CAST)\s*\(") { Add-SargIssue $op.NodeId "function on column (often non-SARGable)" $expr }
    }
  }
  $sargabilityIssues = @($sargabilityIssues | Sort-Object NodeId, Issue -Unique)


# Additional SARGability scan across all ScalarOperator ScalarString nodes
$scalarOps = Select-Nodes("//sp:ScalarOperator[@ScalarString]")

# If a LIKE predicate uses a parameter, infer leading-wildcard from runtime/compiled value
$searchParam = @($parameterSensitivity | Where-Object { $_.Name -match "@Search" } | Select-Object -First 1)
$searchVal = $null
if ($searchParam) { $searchVal = if (-not [string]::IsNullOrWhiteSpace($searchParam.RuntimeValue)) { $searchParam.RuntimeValue } else { $searchParam.CompiledValue } }
$searchLeadingWildcard = $false
if ($searchVal -and ($searchVal -match "(?i)N?'%")) { $searchLeadingWildcard = $true }

foreach ($soNode in $scalarOps) {
  $expr = $soNode.GetAttribute("ScalarString")
  if ([string]::IsNullOrWhiteSpace($expr)) { continue }

  # pick nearest RelOp NodeId if available
  $rel = $soNode.SelectSingleNode("ancestor::sp:RelOp[1]", $nsm)
  $nid = $null
  if ($rel) { $nid = To-IntOrNull ($rel.GetAttribute("NodeId")) }
  if ($nid -eq $null) { $nid = 0 }

  if ($expr -match "CONVERT_IMPLICIT") { Add-SargIssue $nid "implicit conversion (non-SARGable)" $expr }

  # Parameter-based LIKE (e.g., "Title like [@Search]") â€“ infer leading wildcard from param value if present
  if (($expr -match "(?i)\blike\s*\[?@Search\]?") -and $searchLeadingWildcard) {
    Add-SargIssue $nid "leading wildcard LIKE via @Search (non-SARGable)" ($expr + "  -- @Search=" + $searchVal)
  }

  # Explicit literal leading wildcard - FIXED REGEX
  if ($expr -match "(?i)\bLIKE\s+N?'%[^']*'") { 
    Add-SargIssue $nid "leading wildcard LIKE (non-SARGable)" $expr 
  }

  # Function-on-column (only flag when the expression actually contains a 4-part column reference)
  $hasColRef = ($expr -match "\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\]" -or $expr -match "\[(?!@)[^\]]+\]\.\[[^\]]+\]")
  if ($hasColRef -and ($expr -match "(?i)\b(UPPER|LOWER|SUBSTRING|LEFT|RIGHT|DATEADD|DATEDIFF|DATEPART|DATENAME|YEAR|MONTH|DAY|TRY_CONVERT|TRY_CAST|CONVERT|CAST|ISNULL|COALESCE)\s*\(")) {
    # More specific: CONVERT/CAST with a column argument (allow ShowPlan aliasing "... as [p].[Col]")
    if ($expr -match "(?i)\b(CONVERT|CAST)\s*\([^)]*(\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\]|\[(?!@)[^\]]+\]\.\[[^\]]+\])(\s+as\s+\[(?!@)[^\]]+\]\.\[[^\]]+\])?") {
      Add-SargIssue $nid "convert/cast on column (non-SARGable likely)" $expr
    } else {
      Add-SargIssue $nid "function on column (often non-SARGable)" $expr
    }
  }
}

# FIX: Also check the NonSargableHints field that's already populated by Get-NonSargableFlags
foreach ($op in $operatorRows) {
    if (-not [string]::IsNullOrWhiteSpace($op.NonSargableHints)) {
        $hints = $op.NonSargableHints -split ","
        foreach ($h in $hints) {
            $h = $h.Trim()
            if (-not [string]::IsNullOrWhiteSpace($h)) {
                $expr = $op.Predicate
                if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $op.Residual }
                if ([string]::IsNullOrWhiteSpace($expr)) { $expr = $op.SeekPredicate }
                
                $short = if ($expr -and $expr.Length -gt 220) { $expr.Substring(0,220) + "..." } else { $expr }
                
                # Map hint to user-friendly description
                $issue = switch ($h) {
                    "leading-wildcard" { "leading wildcard LIKE (non-SARGable)" }
                    "date-fn"          { "date function on column (non-SARGable)" }
                    "string-fn"        { "string function on column (non-SARGable)" }
                    "case-fn"          { "UPPER/LOWER on column (non-SARGable)" }
                    "isnull()"         { "ISNULL in WHERE clause (non-SARGable)" }
                    "abs()"            { "ABS() on column (non-SARGable)" }
                    "try_convert()"    { "TRY_CONVERT on column (non-SARGable)" }
                    "try_cast()"       { "TRY_CAST on column (non-SARGable)" }
                    "convert()"        { "CONVERT on column (non-SARGable)" }
                    "convert_implicit" { "implicit conversion (CONVERT_IMPLICIT)" }
                    default            { "non-sargable ($h)" }
                }
                
                # Check if this issue already exists for this node
                $exists = $sargabilityIssues | Where-Object { 
                    $_.NodeId -eq $op.NodeId -and $_.Issue -eq $issue 
                }
                
                if (-not $exists) {
                    $sargabilityIssues += [pscustomobject]@{ 
                        NodeId=$op.NodeId; 
                        Issue=$issue; 
                        Expression=$short 
                    }
                }
            }
        }
    }
}

$sargabilityIssues = @($sargabilityIssues | Sort-Object NodeId, Issue, Expression -Unique)

  # Index intersection detector (very heuristic)
  $indexIntersectionSignals = @()
  foreach ($op in $operatorRows | Where-Object { $_.PhysicalOp -match "Bitmap" -or $_.LogicalOp -match "Bitmap" }) {
    $indexIntersectionSignals += [pscustomobject]@{ NodeId=$op.NodeId; Operator=$op.PhysicalOp; Detail="Bitmap present; may indicate index intersection. Consider a composite index that matches predicates/join keys." }
  }
  # Also detect bitmap filters embedded under other operators (e.g., Hash Match with Bitmap)
  $bitmapRelOps = Select-Nodes("//sp:RelOp[.//sp:Bitmap]")
  foreach ($r in $bitmapRelOps) {
    $nid = To-IntOrNull ($r.GetAttribute("NodeId"))
    if ($nid -eq $null) { continue }
    if (-not ($indexIntersectionSignals | Where-Object { $_.NodeId -eq $nid })) {
      $opName = $r.GetAttribute("PhysicalOp")
      if ([string]::IsNullOrWhiteSpace($opName)) { $opName = $r.GetAttribute("LogicalOp") }
      $indexIntersectionSignals += [pscustomobject]@{ NodeId=$nid; Operator=$opName; Detail="Bitmap filter detected under this operator; may indicate index intersection/bitmap filtering." }
    }
  }

# Also detect bitmap filtering expressed as PROBE(Opt_Bitmap...) in ScalarString (common with Hash Match)
$probeNodes = Select-Nodes("//sp:ScalarOperator[@ScalarString]")
foreach ($pNode in $probeNodes) {
  $expr = $pNode.GetAttribute("ScalarString")
  if ([string]::IsNullOrWhiteSpace($expr)) { continue }
  if ($expr -match "(?i)\bPROBE\s*\(" -or $expr -match "Opt_Bitmap") {
    $rel = $pNode.SelectSingleNode("ancestor::sp:RelOp[1]", $nsm)
    $nid = $null
    if ($rel) { $nid = To-IntOrNull ($rel.GetAttribute("NodeId")) }
    if ($nid -eq $null) { $nid = 0 }
    if (-not ($indexIntersectionSignals | Where-Object { $_.NodeId -eq $nid })) {
      $indexIntersectionSignals += [pscustomobject]@{ NodeId=$nid; Operator="(scalar predicate)"; Detail="PROBE()/Opt_Bitmap detected in predicate; indicates bitmap filtering / index intersection behavior." }
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
      ("{0} ON ({1}){2}" -f $_.FullTable, (($keys -join ", ") -replace "\s+"," "), $inc)
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


  if ($conversionNodeIds.Count -gt 0) { $suggestions.Add("Implicit conversion(s) detected (CONVERT_IMPLICIT). These can prevent seeks and skew cardinality; fix datatype mismatches where possible.") }

  # -----------------------------
  # Optional: InspectDatabase (indexes/usage)
  # -----------------------------
  $dbInspection = $null
  if ($InspectDatabase) {
    if ([string]::IsNullOrWhiteSpace($ServerInstance) -or [string]::IsNullOrWhiteSpace($Database)) {
      throw "When using -InspectDatabase, you must supply -ServerInstance and -Database."
    }

    $connString = "Server=$ServerInstance;Database=$Database;Integrated Security=True;Application Name=Get-SqlPlanInsights;"
    $conn = New-Object System.Data.SqlClient.SqlConnection $connString
    $conn.Open()

    try {
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
          $p = $cmd.Parameters.Add($pn, [System.Data.SqlDbType]::NVarChar, 512)
          $p.Value = $t
          $pIndex++
        }
        $cmd.CommandText = $insertSql + "`n" + ($cmd.CommandText -replace "DECLARE @t TABLE \(FullTable sysname NOT NULL\);\s*-- filled by client", "")

        $r = $cmd.ExecuteReader()
        while ($r.Read()) {
          $tableRows += [pscustomobject]@{
            FullTable     = $r["FullTable"]
            IndexName     = $r["IndexName"]
            IndexId       = $r["index_id"]
            TypeDesc      = $r["type_desc"]
            IsUnique      = $r["is_unique"]
            IsPrimaryKey  = $r["is_primary_key"]
            HasFilter     = $r["has_filter"]
            Filter        = $r["filter_definition"]
            KeyCols       = $r["KeyCols"]
            IncludeCols   = $r["IncludeCols"]
            UserSeeks     = $r["user_seeks"]
            UserScans     = $r["user_scans"]
            UserLookups   = $r["user_lookups"]
            UserUpdates   = $r["user_updates"]
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
          $p = $cmdStats.Parameters.Add($pn, [System.Data.SqlDbType]::NVarChar, 512)
          $p.Value = $t
          $pIndex2++
        }
        $cmdStats.CommandText = $insertSql2 + "`n" + ($cmdStats.CommandText -replace "DECLARE @t TABLE \(FullTable sysname NOT NULL\);\s*-- filled by client","")

        $r2 = $cmdStats.ExecuteReader()
        while ($r2.Read()) {
          $statsRows += [pscustomobject]@{
            FullTable = $r2["FullTable"]
            StatsName = $r2["StatsName"]
            StatsId   = $r2["stats_id"]
            AutoCreated = [bool]$r2["auto_created"]
            UserCreated = [bool]$r2["user_created"]
            NoRecompute = [bool]$r2["no_recompute"]
            LastUpdated = $r2["last_updated"]
            Rows        = $r2["rows"]
            RowsSampled = $r2["rows_sampled"]
            ModificationCounter = $r2["modification_counter"]
            StatsCols   = $r2["StatsCols"]
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
                $prefix = $existingCols[0..($need.Count-1)]
                if ((($prefix -join ",") -eq ($need -join ","))) { $covered = $true; break }
              }
            }
          }

          if (-not $covered) {
            $multiColStatsOpportunities += [pscustomobject]@{
              FullTable = $tbl
              Columns   = ($need -join ", ")
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
        if ($mi.EqualityCols)   { $needKeys += $mi.EqualityCols }
        if ($mi.InequalityCols) { $needKeys += $mi.InequalityCols }
        $needInc = @()
        if ($mi.IncludeCols)    { $needInc += $mi.IncludeCols }

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
          $needIncQ  = $needInc  | ForEach-Object { "[{0}]" -f $_ }

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
  Impact    = $mi.Impact
  Occurrences = $occ
  Duplicate   = $dupLabel
  SuggestedKeys = (($needKeys | ForEach-Object { "[{0}]" -f $_ }) -join ", ")
  SuggestedIncludes = (($needInc | ForEach-Object { "[{0}]" -f $_ }) -join ", ")
  CoveredByExistingIndex = if ($coveredBy.Count -gt 0) { $coveredBy -join ", " } else { "" }
}

      }

      }
$dbInspection = [pscustomobject]@{
        ServerInstance = $ServerInstance
        Database       = $Database
        TargetTables   = $tablesList
        Indexes        = $tableRows
        Stats          = $statsRows
        StaleStats     = $staleStats
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
  # Result object
  # -----------------------------
  $result = [pscustomobject]@{
    File = (Resolve-Path -LiteralPath $Path).Path
    NamespaceUri = $nsUri
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
  if ($SanityCheck) {
    Write-Host ("SanityCheck: missingIndexGroups={0}, missingIndexes={1}, links={2}" -f `
      (@($missingIndexGroups).Count), (@($missingIndexes).Count), ($referenceLinks.PSObject.Properties.Count)) -ForegroundColor DarkCyan
  }
  $dopPlannedText = if ($dop -ne $null -and $dop -gt 0) { $dop } else { "n/a" }
  $dopObservedText = if ($dopObserved -ne $null -and $dopObserved -gt 0) { $dopObserved } else { "n/a" }
  $dopParText = if ($dopObservedParallelism -ne $null -and $dopObservedParallelism -gt 0) { $dopObservedParallelism } else { "n/a" }
  $dopMaxText = if ($dopObservedMaxRuntime -ne $null -and $dopObservedMaxRuntime -gt 0) { $dopObservedMaxRuntime } else { "n/a" }
  Write-Host ("DOP: planned={0} observed={1} (parallelism={2} maxRuntime={3})" -f $dopPlannedText, $dopObservedText, $dopParText, $dopMaxText)


  $hasMGNumbers = $memoryGrantInfo -and (
    $null -ne $memoryGrantInfo.RequestedKB -or $null -ne $memoryGrantInfo.GrantedKB -or
    $null -ne $memoryGrantInfo.UsedKB -or $null -ne $memoryGrantInfo.MaxUsedKB -or
    $null -ne $memoryGrantInfo.GrantWaitMS
  )

  function F3orNA($v) { if ($null -eq $v -or [string]::IsNullOrWhiteSpace([string]$v)) { "n/a" } else { F3 $v } }
  $mgFeedback = if ($memoryGrantInfo -and -not [string]::IsNullOrWhiteSpace($memoryGrantInfo.FeedbackAdjusted)) { $memoryGrantInfo.FeedbackAdjusted } else { "n/a" }

  if ($hasMGNumbers) {
    Write-Host ("Memory grant KB: Requested={0} Granted={1} Used={2} MaxUsed={3} WaitMS={4} FeedbackAdjusted={5}" -f `
      (F3orNA $memoryGrantInfo.RequestedKB),
      (F3orNA $memoryGrantInfo.GrantedKB),
      (F3orNA $memoryGrantInfo.UsedKB),
      (F3orNA $memoryGrantInfo.MaxUsedKB),
      (F3orNA $memoryGrantInfo.GrantWaitMS),
      $mgFeedback) -ForegroundColor DarkCyan

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
      Write-Host ("Memory grant analysis: WastePct={0}% UnderPct={1}%{2}" -f (F2 $wastePct), (F2 $underPct), $noteText)
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
  $result.TopOperators |
    Select-Object `
      @{n='NodeId';e={$_.NodeId}},
      @{n='PhysicalOp';e={$_.PhysicalOp}},
      @{n='LogicalOp';e={$_.LogicalOp}},
      @{n='EstCost';e={F2 $_.EstCost}},
      @{n='SelfCost';e={ if ($_.HasKids -and ($null -eq $_.EstSelfCost -or $_.EstSelfCost -eq '')) { '0.00' } else { F2 $_.EstSelfCost } }},
      @{n='EstRows';e={F0 $_.EstRows}},
      @{n='ActOut';e={ if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '') { F0 $_.ActRowsOut } else { '' } }},
      @{n='ActWork';e={ if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '') { F0 $_.ActRowsWork } else { '' } }},
      @{n='RatioOut';e={ if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsOut)/([double]$_.EstRows)) } else { '' } }},
      @{n='RatioWork';e={ if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsWork)/([double]$_.EstRows)) } else { '' } }},
      @{n='RowsRead';e={F0 $_.RowsRead}},
      @{n='Execs';e={F0 $_.Execs}},
      @{n='Access';e={$_.Access}},
      @{n='Object';e={$_.Object}},
      @{n='Warnings';e={$_.Warnings}} |
    Format-Table -AutoSize

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
      @{n='NodeId';e={$_.NodeId}},
      @{n='PhysicalOp';e={$_.PhysicalOp}},
      @{n='LogicalOp';e={$_.LogicalOp}},
      @{n='SelfCost';e={ if ($_.HasKids -and ($null -eq $_.EstSelfCost -or $_.EstSelfCost -eq '')) { '0.00' } else { F2 $_.EstSelfCost } }},
      @{n='EstCost';e={F2 $_.EstCost}},
      @{n='EstRows';e={F0 $_.EstRows}},
      @{n='ActOut';e={ if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '') { F0 $_.ActRowsOut } else { '' } }},
      @{n='ActWork';e={ if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '') { F0 $_.ActRowsWork } else { '' } }},
      @{n='RatioOut';e={ if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsOut)/([double]$_.EstRows)) } else { '' } }},
      @{n='RatioWork';e={ if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsWork)/([double]$_.EstRows)) } else { '' } }},
      @{n='RowsRead';e={F0 $_.RowsRead}},
      @{n='Execs';e={F0 $_.Execs}},
      @{n='Access';e={$_.Access}},
      @{n='Object';e={$_.Object}},
      @{n='Warnings';e={$_.Warnings}} |
    Format-Table -AutoSize
  } else {
    Write-Host " - (same operators as subtree-cost list; omitted)" -ForegroundColor DarkGray
  }

# -----------------------------
  # DBA heuristics output
  # -----------------------------
  Write-Host ""
  Write-Host "Join strategy sanity checks (heuristic):" -ForegroundColor Yellow
  if (@($joinChecks).Count -gt 0) {
    $joinChecks | Select-Object NodeId, Join, Signal, Detail | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  Write-Host "Operator red flags (heuristic):" -ForegroundColor Yellow
  if (@($operatorRedFlags).Count -gt 0) {
    $operatorRedFlags | Select-Object NodeId, Operator, Signal, Detail | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  
  Write-Host ""
  Write-Host "Spill signals (from plan XML):" -ForegroundColor Yellow
  if (@($spillSignals).Count -gt 0) {
    $spillSignals | Select-Object NodeId, Operator, Signal, Detail | Sort-Object NodeId | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  Write-Host "Parallelism skew signals (heuristic):" -ForegroundColor Yellow
  if (@($parallelSkewSignals).Count -gt 0) {
    $parallelSkewSignals |
      Sort-Object SkewRatio -Descending |
      Select-Object NodeId, Operator, Threads, @{n='TotalRows';e={F0 $_.TotalRows}}, Signal, Detail |
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
    $manyToManyMergeSignals | Select-Object NodeId, Join, Signal, Detail | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected)" -ForegroundColor DarkGray
  }

  Write-Host ""
  Write-Host "Implicit conversion signals (PlanAffectingConvert):" -ForegroundColor Yellow
  if (@($planAffectingConvertSignals).Count -gt 0) {
    $planAffectingConvertSignals | Select-Object StatementId, Issue, Expression | Format-Table -AutoSize
  } else {
    Write-Host " - (none detected) Note: SQL Server only creates PlanAffectingConvert elements when implicit conversions significantly affect plan choice." -ForegroundColor DarkGray
  }

Write-Host ""
Write-Host "Predicate SARGability signals (heuristic):" -ForegroundColor Yellow
  $predCount = @($operatorRows | Where-Object { -not [string]::IsNullOrWhiteSpace($_.SeekPredicate) -or -not [string]::IsNullOrWhiteSpace($_.Predicate) -or -not [string]::IsNullOrWhiteSpace($_.Residual) }).Count
  if (@($sargabilityIssues).Count -gt 0) {
    $sargabilityIssues | Select-Object NodeId, Issue, Expression | Format-Table -AutoSize
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
    $psIssues | Select-Object Name, DataType, CompiledValue, RuntimeValue, @{n='ValueRatio';e={F2 $_.ValueRatio}} | Format-Table -AutoSize
  } elseif (@($parameterSensitivity).Count -gt 0) {
    $parameterSensitivity | Select-Object Name, DataType, CompiledValue, RuntimeValue, @{n='ValueRatio';e={ if ($_.ValueRatio -ne $null) { F2 $_.ValueRatio } else { '' } }} | Format-Table -AutoSize
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
    $indexIntersectionSignals | Select-Object NodeId, Operator, Detail | Format-Table -AutoSize
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

  if (@($missingIndexes).Count -gt 0) {
    Write-Host ""
    if (@($missingIndexes).Count -gt 1) {
      Write-Host ("Missing index recommendations (from plan XML) - COUNT: {0} (Unique: {1})" -f @($missingIndexes).Count, $missingIndexesUniqueCount) -ForegroundColor Yellow
    } else {
      Write-Host "Missing index recommendations (from plan XML):" -ForegroundColor Yellow
    }

    $missingIndexesUnique |
      Select-Object `
        @{n='Impact';e={F3 $_.Impact}},
        @{n='FullTable';e={$_.FullTable}},
        @{n='Equality';e={$_.EqualityText}},
        @{n='Inequality';e={$_.InequalityText}},
        @{n='Include';e={$_.IncludeText}} |
      Format-Table -AutoSize

    if (@($missingIndexDuplicates).Count -gt 0) {
      Write-Host ""
      Write-Host "Duplicate missing-index signatures:" -ForegroundColor DarkYellow
      $missingIndexDuplicates |
        Select-Object FullTable, Count, @{n='MaxImpact';e={F3 $_.MaxImpact}} |
        Format-Table -AutoSize
    }

    Write-Host ""
    Write-Host "Example CREATE INDEX statements (validate first):" -ForegroundColor DarkYellow
    $missingIndexesUnique | Select-Object -First 3 | ForEach-Object { Write-Host (" - " + $_.SuggestedSql) }

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
      Write-Host "Parameter sensitivity signals (compiled vs runtime values differ):" -ForegroundColor DarkYellow
      $psIssues |
        Select-Object Name, DataType, CompiledValue, RuntimeValue, @{n='ValueRatio';e={F3 $_.ValueRatio}} |
        Format-Table -AutoSize
      Write-Host "Hint: large compiled/runtime differences can indicate parameter sniffing risk." -ForegroundColor Gray
    }

  }
  else {
    Write-Host ""
    Write-Host "No missing index recommendations found in XML." -ForegroundColor DarkGray
  }

  if ($parallelOps.Count -gt 0) {
    Write-Host ""
    Write-Host "Parallelism operators:" -ForegroundColor Yellow
    $result.ParallelismOps |
      Select-Object `
        @{n='NodeId';e={$_.NodeId}},
        @{n='PhysicalOp';e={$_.PhysicalOp}},
        @{n='LogicalOp';e={$_.LogicalOp}},
        @{n='EstCost';e={F2 $_.EstCost}},
        @{n='EstRows';e={F0 $_.EstRows}},
        @{n='ActOut';e={ if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '') { F0 $_.ActRowsOut } else { '' } }},
        @{n='ActWork';e={ if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '') { F0 $_.ActRowsWork } else { '' } }},
        @{n='Object';e={$_.Object}} |
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
          @{n='NodeId';e={$_.NodeId}},
          @{n='PhysicalOp';e={$_.PhysicalOp}},
          @{n='LogicalOp';e={$_.LogicalOp}},
          @{n='EstRows';e={F0 $_.EstRows}},
          @{n='ActOut';e={ if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '') { F0 $_.ActRowsOut } else { '' } }},
          @{n='ActWork';e={ if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '') { F0 $_.ActRowsWork } else { '' } }},
          @{n='RatioOut';e={ if ($_.ActRowsOut -ne $null -and $_.ActRowsOut -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsOut)/([double]$_.EstRows)) } else { '' } }},
          @{n='RatioWork';e={ if ($_.ActRowsWork -ne $null -and $_.ActRowsWork -ne '' -and $_.EstRows -ne $null -and $_.EstRows -ne '' -and [double]$_.EstRows -ne 0) { F3 (([double]$_.ActRowsWork)/([double]$_.EstRows)) } else { '' } }},
          @{n='RatioBest';e={ if ($_.Ratio -eq [double]::PositiveInfinity) { "inf" } else { F3 $_.Ratio } }},
          @{n='EstCost';e={F2 $_.EstCost}},
          @{n='Access';e={$_.Access}},
          @{n='Object';e={$_.Object}} |
        Format-Table -AutoSize
    } else {
      Write-Host " - (all CE-mismatch nodes are already listed above in Top operators; omitted)" -ForegroundColor DarkGray
    }

    Write-Host ""
    Write-Host "CE hot spots (grouped by statement):" -ForegroundColor DarkYellow
    $result.CEStatementSummary |
      Select-Object `
        StatementId,
        @{n='WorstNode';e={$_.WorstNodeId}},
        @{n='WorstRatio';e={ if ($_.WorstRatio -eq [double]::PositiveInfinity) { "inf" } else { F3 $_.WorstRatio } }},
        @{n='WorstSeverity';e={F3 $_.WorstSeverity}},
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
    Write-Host ("StatementId {0}: {1}" -f $stmt.StatementId, $stmt.StatementText) -ForegroundColor Gray
  }

  # De-dupe repeated detail lines (e.g., same predicate repeated on Top/Parallelism above the real scan/seek)
  $seen = @{}

  function Write-Once([string]$label, [string]$value) {
    if ([string]::IsNullOrWhiteSpace($value)) { return }
    $k = "$label|$value"
    if (-not $seen.ContainsKey($k)) {
      Write-Host ("{0,-18}: {1}" -f $label, $value)
      $seen[$k] = $true
    }
  }

  $top = $cardinalityIssues |
    Where-Object { $_.StatementId -eq $stmt.StatementId } |
    Sort-Object Severity -Descending |
    Select-Object -First 3 |
    Sort-Object NodeId -Unique  # safety: avoid duplicate NodeIds

  foreach ($n in $top) {
    Write-Host ""
    Write-Host ("NodeId             : {0}" -f $n.NodeId)
    Write-Host ("PhysicalOp         : {0}" -f $n.PhysicalOp)
    Write-Host ("EstRows            : {0}" -f (F0 $n.EstRows))
    $actOutTxt  = if ($n.ActRowsOut -ne $null -and $n.ActRowsOut -ne '') { F0 $n.ActRowsOut } else { 'n/a' }
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
        Write-Host ("Threads            : {0} workers (min={1}, avg={2}, max={3}, max/avg={4})" -f $wCount, (F0 $min), (F0 $avg), (F0 $max), $skew)

        # Compact per-thread list: show up to 8 busiest threads
        $topThr = $workers | Sort-Object ActualRows -Descending | Select-Object -First 8
        $pairs = @()
        foreach ($tr in $topThr) { $pairs += ("t{0}={1}" -f $tr.Thread, (F0 $tr.ActualRows)) }
        $more = $wCount - @($topThr).Count
        $suffix = if ($more -gt 0) { " ... +$more more" } else { "" }
        Write-Host ("PerThreadRows      : {0}{1}" -f ($pairs -join "  "), $suffix)
      }
    }

    $ratioOutTxt  = if ($n.ActRowsOut -ne $null -and $n.EstRows -ne $null -and [double]$n.EstRows -ne 0) { F3 (([double]$n.ActRowsOut)/([double]$n.EstRows)) } else { 'n/a' }
    $ratioWorkTxt = if ($n.ActRowsWork -ne $null -and $n.EstRows -ne $null -and [double]$n.EstRows -ne 0) { F3 (([double]$n.ActRowsWork)/([double]$n.EstRows)) } else { 'n/a' }
    Write-Host ("RatioOut           : {0}" -f $ratioOutTxt)
    Write-Host ("RatioWork          : {0}" -f $ratioWorkTxt)

    if (-not [string]::IsNullOrWhiteSpace($n.LikelyContributors)) {
      Write-Host ("LikelyContributors : {0}" -f $n.LikelyContributors)
    }

    # Print predicates where they are most meaningful, and avoid repeated noise.
    $isAccess = ($n.PhysicalOp -match "Scan|Seek|Lookup")
    $isFilter = ($n.PhysicalOp -match "^Filter$")
    $isJoin   = ($n.PhysicalOp -match "Nested Loops|Hash Match|Merge Join")

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
    Write-Host ("Database inspection: {0}\{1}" -f $result.DbInspection.ServerInstance, $result.DbInspection.Database) -ForegroundColor Cyan

    if ($result.DbInspection.TargetTables.Count -gt 0) {
      Write-Host "Target tables:" -ForegroundColor DarkCyan
      $result.DbInspection.TargetTables | ForEach-Object { Write-Host (" - " + $_) }
    }

    if ($result.DbInspection.Indexes.Count -gt 0) {
      Write-Host ""
      Write-Host "Indexes (per target table):" -ForegroundColor Yellow
      $result.DbInspection.Indexes |
        Select-Object `
          FullTable, IndexId, IndexName, TypeDesc, IsUnique, IsPrimaryKey, HasFilter,
          @{n='KeyCols';e={$_.KeyCols}},
          @{n='IncludeCols';e={$_.IncludeCols}},
          UserSeeks, UserScans, UserLookups, UserUpdates |
        Format-Table -AutoSize
    }

    if ($result.DbInspection.Stats.Count -gt 0) {
      Write-Host ""
      Write-Host "Statistics (per target table):" -ForegroundColor Yellow
      $result.DbInspection.Stats |
        Select-Object `
          FullTable, StatsName,
          @{n='LastUpdated';e={$_.LastUpdated}},
          @{n='Rows';e={$_.Rows}},
          @{n='Mods';e={$_.ModificationCounter}},
          @{n='ModRatio';e={
            $rows = To-DoubleOrNull $_.Rows
            $mods = To-DoubleOrNull $_.ModificationCounter
            if ($rows -and $rows -gt 0 -and $mods -ne $null) { F3 ($mods/$rows) } else { "" }
          }},
          StatsCols, AutoCreated, UserCreated, NoRecompute |
        Format-Table -AutoSize
    }

    if ($result.DbInspection.StaleStats.Count -gt 0) {
      Write-Host ""
      Write-Host "Stale statistics candidates (heuristic):" -ForegroundColor DarkYellow
      $result.DbInspection.StaleStats |
        Select-Object `
          FullTable, StatsName,
          @{n='LastUpdated';e={$_.LastUpdated}},
          @{n='Rows';e={$_.Rows}},
          @{n='Mods';e={$_.ModificationCounter}},
          @{n='ModRatio';e={F3 $_.ModRatio}},
          @{n='Days';e={F0 $_.DaysSinceUpdate}},
          StatsCols |
        Format-Table -AutoSize
      Write-Host "Tip: consider UPDATE STATISTICS (or sp_updatestats) for the highlighted stats after validating workload/maintenance windows." -ForegroundColor Gray
    }

    if ($result.DbInspection.MultiColumnStatsOpportunities.Count -gt 0) {
      Write-Host ""
      Write-Host "Multi-column statistics opportunities (from predicates; not covered by existing stats):" -ForegroundColor DarkYellow
      $result.DbInspection.MultiColumnStatsOpportunities |
        Select-Object FullTable, Columns, StatementId, ExampleNodeId, ExamplePredicate |
        Format-Table -AutoSize
      Write-Host "Tip: multi-column stats can help cardinality estimation when multiple columns are filtered together." -ForegroundColor Gray
    }

    if (@($result.DbInspection.MissingIndexCoverage).Count -gt 0) {
      Write-Host ""
      Write-Host "Missing-index coverage check (heuristic: does an existing index already cover it?):" -ForegroundColor Yellow
      $result.DbInspection.MissingIndexCoverage |
  Select-Object `
    FullTable,
    @{n='Impact';e={F3 $_.Impact}},
    Occurrences, Duplicate,
    SuggestedKeys, SuggestedIncludes, CoveredByExistingIndex |
  Format-Table -AutoSize

    }
  }

  Write-Host ""
  Write-Host "Useful links:" -ForegroundColor Cyan
  foreach ($p in $result.ReferenceLinks.PSObject.Properties) {
    Write-Host (" - {0}: {1}" -f $p.Name, $p.Value)
  }

  # ---- make returned object display compact by default (but keep full data) ----
  $result.PSTypeNames.Insert(0, 'SqlPlanInsights.Result')

  # Add some convenience count props for display (doesn't change underlying arrays)
  $result | Add-Member -NotePropertyName TopOperatorsCount    -NotePropertyValue (@($result.TopOperators).Count) -Force
  $result | Add-Member -NotePropertyName CEIssuesCount        -NotePropertyValue (@($result.CardinalityIssues).Count) -Force
  $result | Add-Member -NotePropertyName MissingIndexesCount  -NotePropertyValue (@($result.MissingIndexes).Count) -Force
    $result | Add-Member -NotePropertyName JoinChecksCount     -NotePropertyValue (@($result.JoinChecks).Count) -Force
  $result | Add-Member -NotePropertyName RedFlagsCount        -NotePropertyValue (@($result.OperatorRedFlags).Count) -Force
  $result | Add-Member -NotePropertyName SargIssuesCount      -NotePropertyValue (@($result.SargabilityIssues).Count) -Force
  $result | Add-Member -NotePropertyName IndexIntersectCount  -NotePropertyValue (@($result.IndexIntersectionSignals).Count) -Force
  $result | Add-Member -NotePropertyName RewriteHintsCount    -NotePropertyValue (@($result.RewriteHints).Count) -Force
  $result | Add-Member -NotePropertyName SpillOpsCount        -NotePropertyValue (@($result.SpillSignals).Count) -Force
  $result | Add-Member -NotePropertyName ParallelSkewCount    -NotePropertyValue (@($result.ParallelSkewSignals).Count) -Force
  $result | Add-Member -NotePropertyName ManyToManyMergeCount -NotePropertyValue (@($result.ManyToManyMergeSignals).Count) -Force
    $result | Add-Member -NotePropertyName PlanConvertCount     -NotePropertyValue (@($result.PlanAffectingConvertSignals).Count) -Force
$result | Add-Member -NotePropertyName KeyLookupsCount      -NotePropertyValue (@($result.KeyLookups).Count) -Force
  $result | Add-Member -NotePropertyName SortOpsCount         -NotePropertyValue (@($result.SortOps).Count) -Force
  $result | Add-Member -NotePropertyName ParallelismOpsCount  -NotePropertyValue (@($result.ParallelismOps).Count) -Force
  $result | Add-Member -NotePropertyName SuggestionsCount     -NotePropertyValue (@($result.Suggestions).Count) -Force
  $result | Add-Member -NotePropertyName InspectedDatabase    -NotePropertyValue ([bool]($InspectDatabase -and $result.DbInspection)) -Force

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
}
