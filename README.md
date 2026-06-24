# dbaStuff

A small toolbox of production-minded SQL Server utilities I've built and used as a DBA / Database Reliability Engineer. The headline pieces are an **execution-plan analysis engine** (PowerShell) and a **self-contained HTML plan visualiser** that work together as a two-stage tuning workflow — but everything here is designed to be picked up and run on its own.

Two principles run through all of it:

- **Nothing leaves your machine.** The visualiser is a single HTML file with no network calls — your execution plans (which can leak schema, object names and literal parameter values) are never uploaded anywhere. The PowerShell tools only ever talk to the instances you point them at.
- **Read before you run.** The scripts that change things default to *script-only / dry-run*. You get reviewable T-SQL out; nothing touches the database until you explicitly ask it to. The diagnostic tools (estate checker, plan analyzer) are read-only and never change anything at all.

> **Licence:** MIT — see [`LICENSE`](LICENSE).

---

## Contents

| Folder | Tool | What it does |
| --- | --- | --- |
| [`Plan Visualizer`](Plan%20Visualizer) | `plan_visualizer_V3.html` | Single-file, offline execution-plan viewer. Drag in a `.sqlplan` or a JSON bundle from the analyzer and get an interactive diagram, cost/cardinality grid, and a tuning-insights panel. |
| [`execution-plans`](execution-plans) | `executionPlanReport_v7.ps1` (`Get-SqlPlanInsights`) | Parses ShowPlan XML like an automated tuning consultant — CE mismatches, TempDB spills, SARGability violations, implicit conversions, parameter-sniffing risk — and can augment it with live Query Store / index telemetry. |
| [`checker`](checker) | `Invoke-SqlEstateChecks_v2.ps1` | **(WIP)** Read-only estate-wide health check across a list of instances — configuration drift, backup/integrity currency, capacity risks — rolled up into a single self-contained HTML report. |
| [`move-database-objects`](move-database-objects) | `shrinkDbObjects.ps1` | Relocates tables, heaps and indexes (including LOB data) onto a target filegroup, preserving constraints. Used to reclaim space after a shrink, or to move objects between filegroups safely. |
| [`permissions`](permissions) | `sqlPermissions.ps1` | Exports instance-level role memberships and database-level permissions across a list of instances to a filtered, auto-width Excel workbook, then zips it. |
| [`troubleshooting`](troubleshooting) | `Troubleshooting_Guide.html` | Offline HTML incident runbook for triaging SQL Server under pressure — blocking, deadlocks, tempdb, high CPU, plan regressions, log growth, AG health. |

---

## The headline workflow: analyze → bundle → visualise

The PowerShell analyzer and the HTML visualiser are designed to hand off to each other, so you get both a scriptable, CI-friendly report *and* a rich visual you can hand to a colleague — without ever uploading a plan to a third-party site.

```powershell
# 1. Analyse a saved plan, augment it with live database telemetry,
#    and emit a self-contained JSON bundle.
Get-SqlPlanInsights `
    -Path        ".\Execution plan.sqlplan" `
    -ServerInstance "PROD-SQL-01" `
    -Database    "StackOverflow2013" `
    -InspectDatabase `
    -OutFile     ".\plan.json"
```

```text
2. Open plan_visualizer_V3.html in any browser.
3. Drag plan.json (or any raw .sqlplan) onto the page.
```

The bundle carries the plan XML *plus* the analyzer's findings, live index/statistics inventory, stale-stats candidates and Query Store regressions — so the visualiser's insights panel lights up with everything the engine found, all rendered locally.

---

## Plan Visualizer (`plan_visualizer_V3.html`)

A single, dependency-free HTML file. Open it in any modern browser — no install, no build step, no server. It reads either a raw `.sqlplan` / `SHOWPLAN_XML` document or a `sqlplan-insights` JSON bundle produced by `Get-SqlPlanInsights`.

**What you get**

- **Interactive operator diagram** — operators coloured by cost (green → amber → red), arrow thickness proportional to row counts, cardinality-skew warnings flagged inline. Click any node to inspect its costs, cardinality, predicates and warnings.
- **Cost / cardinality grid** — every operator with estimated vs. actual rows, execution counts and the estimate ratio, sortable, with per-statement chips.
- **Tuning insights panel** — TempDB spills, SARGability signals, merged missing-index suggestions, and (when a JSON bundle is supplied) live statistics, stale-stats candidates, multi-column statistics opportunities and Query Store regressions.

Get a plan out of SSMS with right-click → **Save Execution Plan As…**.

**Everything runs locally — the page never connects to a database and nothing is uploaded.**
---

## Execution Plan Analyzer — `Get-SqlPlanInsights` (`executionPlanReport_v7.ps1`)

An automated query-tuning consultant in a function. It parses ShowPlan XML and surfaces the things that actually matter for tuning: cardinality-estimation mismatches, TempDB spills, non-SARGable predicates, implicit conversions and parameter-sniffing risk.

**Two modes**

1. **Offline file parsing** — analyse a static `.xml` / `.sqlplan` file with no connection.
2. **Telemetry & regression** — connect to the instance to append real-time Query Store metrics, inspect existing indexes / statistics staleness / missing-index coverage, and auto-generate plan-forcing scripts where regressions are found.

**Usage**

```powershell
# Standard offline analysis
Get-SqlPlanInsights -Path ".\Execution plan.xml"

# File analysis + Query Store telemetry + live index/coverage check
Get-SqlPlanInsights -Path ".\Execution plan.xml" `
    -ServerInstance "PROD-SQL-01" -Database "StackOverflow2013" -InspectDatabase

# ...and emit a JSON bundle to open in the Plan Visualizer
Get-SqlPlanInsights -Path ".\Execution plan.xml" `
    -ServerInstance "PROD-SQL-01" -Database "StackOverflow2013" -InspectDatabase `
    -OutFile ".\plan.json"

# Connect with SQL authentication (via dbatools)
$cred = Get-Credential
Get-SqlPlanInsights -ServerInstance "192.168.1.195" -Database "StackOverflow" -SqlCredential $cred

# Tighten CE-mismatch sensitivity
Get-SqlPlanInsights -Path ".\Execution plan.xml" -CEMismatchRatio 5 -CEMinRows 50
```

**Notes**

- Requires the **dbatools** module only when connecting to a database for telemetry/inspection; offline parsing has no dependencies.
- Missing-index suggestions are merged to prevent bloat; parameter-sniffing test scripts (`OPTION (RECOMPILE)`, `OPTIMIZE FOR`) are generated automatically.
- **CE severity scoring** is runtime-evidence-only (revised per feedback from Joe Chang): severity is a function of the CE mismatch ratio, CPU-volume weight (`RowsRead`) and a waste multiplier penalising operators that scan many rows but pass few — the optimizer's page-weighted cost estimate is deliberately *not* used.

---

## SQL Estate Health Check — `Invoke-SqlEstateChecks_v2.ps1`  *(work in progress)*

A **read-only** health check that sweeps an entire estate in one pass. Point it at a list of instances and it inspects each one for the configuration drift, capacity risks and best-practice violations that tend to surface at the worst possible moment — then rolls the findings up into a single self-contained HTML report you can hand to a colleague, attach to a change review, or keep as a point-in-time baseline.

It only ever *reads*. There is no `-Execute` switch and nothing to undo — the script makes no changes to any instance or database.

**Typical checks** *(evolving — see the script for the current set)*

- **Instance configuration** — max server memory, MAXDOP, cost threshold for parallelism, optimise-for-ad-hoc and other server settings that drift away from sensible defaults.
- **Database settings** — recovery model vs. backup reality, page verify (`CHECKSUM`), auto-shrink, auto-close, compatibility level and owner.
- **Backup currency** — last full / differential / log backup per database, flagging anything outside policy.
- **Integrity** — most recent successful `DBCC CHECKDB`, flagging databases overdue for a consistency check.
- **Capacity** — data/log file free space and growth settings, drive headroom.
- **Agent & jobs** — recently failed jobs and jobs with no failure notification.

Findings are graded so the report leads with what actually needs attention rather than a flat wall of green ticks.

```powershell
# From an instances.txt file (one instance per line; # comments allowed)
.\Invoke-SqlEstateChecks_v2.ps1 -InstancesFile .\instances.txt -OutputDir .\out

# Or pass instances inline
.\Invoke-SqlEstateChecks_v2.ps1 -Instances "SQL01","SQL02\PROD" -OutputDir .\out
```

Requires **dbatools**, and read access (typically `VIEW SERVER STATE` plus database-level read) on each instance. The file `Invoke-SqlEstateChecks.ps1--SUPERSEDED-BY-V2` is the previous version, kept for reference only — run **v2**.

---

## Move Database Objects (`shrinkDbObjects.ps1`)

Shrinks rowstore tables, heaps and indexes in a single database by relocating them onto a target filegroup — **including LOB / BLOB (`TEXTIMAGE` / `LOB_DATA`) data** — while preserving PK / UNIQUE / FK / CHECK / DEFAULT constraints. The classic use case is reclaiming space after the file-level fragmentation a `DBCC SHRINKFILE` leaves behind, by genuinely rewriting allocation units onto a clean filegroup.

For each object it generates the correct DDL:

- **Clustered tables** → `CREATE ... CLUSTERED INDEX ... WITH (DROP_EXISTING = ON) ON [TargetFG]` (constraints preserved in place).
- **Clustered tables with LOB** → the partition-scheme technique (credit: Alex Yumashev / Jitbit) to rewrite *every* allocation unit, LOB included, then de-partition.
- **Heaps** → temporary clustered index on the target FG, then `DROP ... WITH (MOVE TO [TargetFG])`.
- **Nonclustered indexes** → recreated on the target FG, reproducing filtered predicates, included columns, uniqueness and key sort direction.

`SORT_IN_TEMPDB = ON` throughout; compression configurable; `ONLINE` optional (Enterprise / Developer).

**By design the default is script-only.** Nothing runs until you pass `-Execute`. Review the T-SQL, take a backup, and run in a maintenance window with adequate free space and log headroom.

```powershell
# Dry run — generate a reviewable script, change nothing
.\shrinkDbObjects.ps1 -SqlInstance SQL01 -Database Sales `
    -TargetFileGroup DATA_FG2 -OutputScriptPath .\move_sales.sql

# Classify and size every object, emit no DDL
.\shrinkDbObjects.ps1 -SqlInstance SQL01 -Database Sales `
    -TargetFileGroup DATA_FG2 -ReportOnly -ReportCsvPath .\report.csv

# Execute, forcing PAGE compression, online
.\shrinkDbObjects.ps1 -SqlInstance SQL01 -Database Sales `
    -TargetFileGroup DATA_FG2 -Compression Page -Online -Execute
```

Columnstore indexes are relocated only with `-IncludeColumnstore`. Partitioned, XML / spatial / full-text / in-memory / FILESTREAM structures are skipped with a warning by design — they need bespoke handling. Prefers **dbatools**, falls back to the **SqlServer** module, then raw `SqlClient`.


---

## Permissions Export (`sqlPermissions.ps1`)

Given a list of SQL Server instances, produces a single Excel workbook with:

1. An **All Instance Perms** worksheet — every instance-level role membership.
2. An **All DBs Perms** worksheet — every database-level permission.

The workbook is auto-width with filters on every column, then zipped (the unzipped copy is removed). Handy for access reviews, audits and handover documentation.

```powershell
# From an instances.txt file (one instance per line; # comments allowed)
.\sqlPermissions.ps1 -InstancesFile .\instances.txt -OutputDir .\out

# Or pass instances inline
.\sqlPermissions.ps1 -Instances "SQL01","SQL02\PROD" -OutputDir .\out
```

Noise-reduction switches let you include/exclude policy logins, blank logins and `NT*` service principals. Requires **dbatools** and **ImportExcel**, plus sysadmin on each instance. (The permission-enumeration T-SQL is adapted from a well-known [DBA StackExchange answer](https://dba.stackexchange.com/questions/36618/list-all-permissions-for-a-given-role).)


---

## Troubleshooting Guide (`Troubleshooting_Guide.html`)

A single, offline HTML runbook for triaging a SQL Server incident while it's happening — the structured first-response reference you'd want open during a major incident, rather than half-remembering which DMV to hit at 2 a.m. Open it in any browser: no install, no build step, no network calls.

**Covers** *(see the guide for the current set)*

- **Blocking & lock waits** — finding the head blocker, walking the blocking chain, and seeing what's holding what.
- **Deadlocks** — pulling and reading the deadlock graph, and the usual resolution paths.
- **tempdb contention** — allocation (PFS / GAM / SGAM) and metadata contention, spills, version-store growth.
- **High CPU** — separating a plan problem from a workload problem, and the queries to confirm which.
- **Plan regressions** — spotting a regressed plan and the Query Store route to forcing a known-good one.
- **Runaway log growth** — what's holding `log_reuse_wait_desc`, and how to recover headroom safely.
- **Availability Groups** — synchronisation state, redo queue and failover health.

Each section pairs a short "what you're looking at" with the diagnostic queries to run and a decision path through the fix.

---

## Requirements

- **PowerShell** 5.1+ (the scripts are Windows PowerShell / PowerShell 7 compatible).
- **[dbatools](https://dbatools.io/)** — used by the analyzer (when connecting), the estate checker, the move-objects script and the permissions export.
- **[ImportExcel](https://github.com/dfinke/ImportExcel)** — used by the permissions export.
- A modern browser for the Plan Visualizer and the Troubleshooting Guide. No other dependencies — each is one self-contained HTML file.

```powershell
Install-Module dbatools, ImportExcel -Scope CurrentUser
```

---

## Acknowledgements

- **LOB-move technique** — Alex Yumashev / Jitbit, *"Moving SQL table text/image to a new filegroup."*
- **CE severity model** — refined following feedback from Joe Chang.
- **Permissions T-SQL** — adapted from DBA StackExchange.
- **SSMS hides missing indexes** — Brent Ozar.

---

## Licence

MIT. See [`LICENSE`](LICENSE).
