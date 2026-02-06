# **SQL Plan Insights (PowerShell)**

A powerful PowerShell script that transforms raw SQL Server execution plan XML into performance insights, revealing problems quickly.

## **Why This Exists**

As a DBA/developer, you save an execution plan and see a "Missing Index" suggestion. You come back later, and it's gone. You see a slow operator but can't easily tell why SQL Server chose it. This script solves that by providing consistent, detailed, and prioritized analysis of any execution plan XML file.

## **What It Does**

This script parses SQL Server ShowPlan XML and automatically surfaces:

-   ****Hidden Missing Indexes****: Index suggestions embedded in the XML, even when SSMS doesn't show the green bang.
    
-   ****Cardinality Mismatches****: Where estimated rows vs. actual rows are wildly different (bad guesses).
    
-   ****Parameter Sensitivity****: Differences between compiled and runtime parameter values (sniffing).
    
-   ****SARGability Issues****: Flags predicates that can't use indexes (implicit conversions, functions, wildcards).
    
-   ****Operational Red Flags****: Spills to tempdb, parallelism skew, expensive key lookups, and large sorts.
    

## **Quick Start**

### **1\. Get the Script**

text

\# Clone the repository
git clone https://github.com/rafinnerty/dbaStuff.git
cd dbaStuff

\# Or just download the script:
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/rafinnerty/dbaStuff/main/Get-SqlPlanInsights.ps1" -OutFile "Get-SqlPlanInsights.ps1"

### **2\. Run Your First Analysis**

text

\# Source the function into your session
. .\\Get-SqlPlanInsights.ps1

\# Analyze a saved execution plan
Get-SqlPlanInsights -Path "C:\\temp\\slow\_query.sqlplan"

****Pro Tip:**** In SSMS, after running a query with "Include Actual Execution Plan" (Ctrl+M), right-click the plan and select "Show Execution Plan XML...". Save that XML file and feed it to this script.

## **Detailed Usage**

### **Basic Analysis**

text

Get-SqlPlanInsights -Path ".\\Execution plan.xml"

### **Deep Dive Analysis**

text

\# Get more operator details and check for index coverage
Get-SqlPlanInsights -Path ".\\plan.xml" -TopOperators 25 -InspectDatabase -ServerInstance ".\\SQL2019" -Database "AdventureWorks"

### **Output Control**

text

\# Send the full report to a text file for sharing
Get-SqlPlanInsights -Path ".\\plan.xml" -OutputTo File -OutputFilePath ".\\analysis\_report.txt"

## **Sample Output Walkthrough**

The script generates a structured console report. Here's what you'll see:

text

\=== SQL Plan Insights ===
File: C:\\plans\\query.sqlplan
DOP: 4
Memory grant KB: Requested=102,400 Granted=102,400 Used=12,288 MaxUsed=24,576

Top operators by EstimatedTotalSubtreeCost:
NodeId PhysicalOp          EstCost EstRows ActRows Access      Object
58     Clustered Index Scan 45.32  1       100,000 Scan        \[Sales\].\[OrderDetails\](PK\_OrderDetails)

****Key sections in the full output:****

1.  Plan Overview: Degree of Parallelism (DOP), memory grant efficiency
    
2.  Top Expensive Operators: The "hot spots" in your plan
    
3.  Cardinality Issues: Sorted by severity, with likely root causes
    
4.  Missing Index Suggestions: With ready-to-use CREATE INDEX statements
    
5.  DBA Heuristics: Join warnings, spill detection, parallelism skew
    
6.  Actionable Suggestions: A prioritized to-do list
    

## **Full Parameter Reference**

-   ****\-Path**** (string): Path to the .sqlplan or .xml file. Required.
    
-   ****\-TopOperators**** (int): Number of costly operators to display. Default: 15
    
-   ****\-CEMismatchRatio**** (double): Threshold to flag cardinality mismatches. Default: 10
    
-   ****\-InspectDatabase**** (switch): Connect to the source DB to check existing indexes and stats.
    
-   ****\-ServerInstance**** (string): Server name (required with -InspectDatabase).
    
-   ****\-Database**** (string): Database name (required with -InspectDatabase).
    
-   ****\-OutputTo**** (string): Output destination: Screen, File, or Both. Default: Screen
    
-   ****\-OutputFilePath**** (string): Path for the output file (used with -OutputTo File/Both).
    

## **FAQ**

****Q: Does this work with both Estimated and Actual execution plans?****  
A: Yes! It works with any valid SQL Server ShowPlan XML. For the richest insights (actual rows, runtime parameters), use an Actual Execution Plan.

****Q: Does the script connect to or modify my database?****  
A: No, unless you explicitly use the -InspectDatabase switch. By default, it only reads the XML file.

****Q: I see duplicate missing index suggestions. Why?****  
A: The plan XML can contain duplicate entries. The script shows them but also deduplicates for the final CREATE INDEX examples.

****Q: The script mentions "SARGability." What does that mean?****  
A: It stands for Search ARGument-able. A predicate is "sargable" if SQL Server can use an index seek for it. Functions like CONVERT, UPPER, or WHERE Column LIKE '%value' often break sargability.

****Q: How do I know which cardinality mismatches to fix first?****  
A: The script calculates a Severity score that considers both the ratio and the operator's cost. Focus on high-severity issues near the top of the "Cardinality Issues" list.

## **How It Works (For Contributors)**

The script uses PowerShell's XML capabilities to navigate the ShowPlan schema. Key functions:

-   Select-Nodes: Queries the XML with proper namespace handling
    
-   Get-RunTimeSum: Aggregates runtime counters across parallel threads
    
-   Get-NonSargableFlags: Regex-based detection of anti-patterns in predicates
    

Interested in improving the heuristics or adding new detectors? Check out CONTRIBUTING.md.

## **License**

Distributed under the MIT License. See LICENSE for more information.

## **Acknowledgments & Inspiration**

This tool stands on the shoulders of giants in the SQL Server community:

-   Brent Ozar & Team for pioneering sp\_BlitzCache and making plan analysis accessible
    
-   Erik Darling for deep dives into cardinality estimation and SARGability
    
-   The SQL Server community on Twitter, Stack Overflow, and beyond for sharing knowledge freely
    

* * *

****Found it useful?**** Please consider giving the repo a star! It helps others find it.

****Have a success story?**** Open an issue and tell us how this script helped you fix a performance problem!
