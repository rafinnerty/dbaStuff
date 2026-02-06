# 🚀 SQL Plan Insights (PowerShell)

A powerful PowerShell script that transforms raw SQL Server execution plan XML into **actionable performance insights**, revealing problems that SQL Server Management Studio (SSMS) often hides or buries.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![PowerShell Version](https://img.shields.io/badge/PowerShell-5.1+-blue.svg)

## 🎯 Why This Exists

As a DBA/developer, you save an execution plan and see a "Missing Index" suggestion. You come back later, and it's gone. You see a slow operator but can't easily tell *why* SQL Server chose it. This script solves that by providing **consistent, detailed, and prioritized analysis** of any `.sqlplan` file.

## ✨ What It Does (Better Than SSMS)

This script parses SQL Server ShowPlan XML and automatically surfaces:

| Insight | What It Finds | Why It Matters |
| :--- | :--- | :--- |
| **🔍 Hidden Missing Indexes** | Index suggestions embedded in the XML, even when SSMS doesn't show the green bang. | SSMS filters them out; this script shows you *everything*. |
| **📈 Cardinality Mismatches** | Where estimated rows vs. actual rows are wildly different (bad guesses). | Bad cardinality leads to bad join types, memory grants, and overall plan choices. |
| **🎯 Parameter Sensitivity** | Differences between compiled and runtime parameter values (sniffing). | Explains why a query is fast sometimes and slow other times. |
| **🚫 SARGability Issues** | Flags predicates that can't use indexes (implicit conversions, functions, wildcards). | Directs you to query rewrites that will enable seeks. |
| **⚠️ Operational Red Flags** | Spills to tempdb, parallelism skew, expensive key lookups, and large sorts. | Identifies memory, disk, and CPU bottlenecks in the plan. |

## 🚦 Quick Start

### 1. Get the Script
```powershell
# Clone the repository
git clone https://github.com/rafinnerty/dbaStuff.git
cd dbaStuff

# Or just download the script:
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/rafinnerty/dbaStuff/main/Get-SqlPlanInsights.ps1" -OutFile "Get-SqlPlanInsights.ps1"
