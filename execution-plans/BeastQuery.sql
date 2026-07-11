/* ============================================================================
   BEAST QUERY - Get-SqlPlanInsights v9 full-coverage exercise
   Target: StackOverflow2013 at COMPATIBILITY_LEVEL 160, SQL Server 2022
   Assumes IX_Posts_OwnerUserId already exists (created in act three).

   Every sin is deliberate. Scorecard of expected v9 sections at the bottom.
   Runtime expectation: 1-4 minutes on the Z640. Result set: likely near-empty
   (the Legendary-badge filter sees to that) - the PLAN is the product.
   ============================================================================ */

USE StackOverflow2013;
GO

/* ---- 1. Setup: supporting indexes (idempotent) ----------------------------
   IX_Users_Reputation: enables the seek + key-lookup + parameter-sniffing trap.
   IX_Badges_UserId:    feeds the merge joins ordered input (and M2M detection). */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Users_Reputation' AND object_id = OBJECT_ID(N'dbo.Users'))
    CREATE INDEX IX_Users_Reputation ON dbo.Users (Reputation);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Badges_UserId' AND object_id = OBJECT_ID(N'dbo.Badges'))
    CREATE INDEX IX_Badges_UserId ON dbo.Badges (UserId) INCLUDE (Name);
GO

/* ---- 2. The beast --------------------------------------------------------- */
CREATE OR ALTER PROCEDURE dbo.BeastQuery
    @RepLo int,
    @RepHi int,
    @Loc   nvarchar(100),
    @ViewCountText nvarchar(20)   -- deliberately mistyped: the column is int
AS
BEGIN
    SELECT TOP (5000)
        u.DisplayName,
        u.Reputation * 1.1  AS AdjustedRep,     -- SIN 1: int * numeric -> expression CONVERT_IMPLICIT in Compute Scalar
        p.Title,
        p.Score,
        p.CreationDate,
        bl.Name             AS RareBadge,
        b2.Name             AS OtherBadge
    FROM dbo.Users AS u
    INNER JOIN dbo.Posts AS p                    -- join left free (no hint) - can go loops/hash/adaptive
        ON p.OwnerUserId = u.Id
       AND p.ViewCount >= @ViewCountText         -- SIN 2: int column vs nvarchar PARAMETER -> PlanAffectingConvert + predicate CONVERT_IMPLICIT
                                                    --        (v1 used a literal N'10', which constant-folding quietly converted at
                                                    --         compile time - no runtime conversion, no warning. Parameters can't fold.)
    INNER MERGE JOIN dbo.Badges AS bl            -- SIN 3: inline MERGE hint (forces algorithm AND join order, statement-wide)
        ON bl.UserId = u.Id
       AND bl.Name = N'Legendary'                -- rare badge: keeps the M2M blast radius survivable
    INNER MERGE JOIN dbo.Badges AS b2            -- SIN 4: badges-to-badges on UserId - BOTH sides non-unique -> many-to-many merge
        ON b2.UserId = bl.UserId
    WHERE u.Reputation BETWEEN @RepLo AND @RepHi -- SIN 5: sniffable range predicate on the new index -> key lookups for DisplayName/Location
      AND UPPER(u.Location) LIKE @Loc            -- SIN 6: function-on-column + (runtime) leading wildcard -> two SARGability signals
    ORDER BY p.CreationDate DESC                 -- SIN 7: sort on unindexed column -> SortOps + sort-index suggestion + spill fuel
    OPTION (
        MAXDOP 8,                                -- SIN 8: DOP pinned
        MAX_GRANT_PERCENT = 0.1,                 -- SIN 9: strangled memory grant -> guaranteed sort/hash spills; exercises GRANT PERCENT hint detection
        USE HINT('FORCE_LEGACY_CARDINALITY_ESTIMATION')  -- SIN 10: CE 70 at compat 160 -> v9 narrows the source to THIS hint
    );
END;
GO

/* ---- 3. Execution protocol (order matters for parameter sensitivity) ------

   STEP A - prime the cache with pathological compiled values (returns nothing,
            compiles a plan optimized for an empty range):   */
EXEC dbo.BeastQuery @RepLo = 1000000, @RepHi = 1000001, @Loc = N'x', @ViewCountText = N'10';

/* STEP B - in SSMS: enable Actual Execution Plan (Ctrl+M), then run with wide
            runtime values against the CACHED plan. Compiled values (step A)
            now differ wildly from runtime values -> parameter sensitivity
            signals + key-lookup explosion + spills:          */
EXEC dbo.BeastQuery @RepLo = 100, @RepHi = 50000, @Loc = N'%united%', @ViewCountText = N'10';

/* STEP C - save the actual plan as beast.sqlplan, then:

   Get-SqlPlanInsights -Path .\beast.sqlplan -ServerInstance . `
       -Database StackOverflow2013 -InspectDatabase -OutFile .\beast.json `
       -ShowAllHeuristicMatches

   (-ShowAllHeuristicMatches earns its keep for once - the grouped view will
    be busy enough that the ungrouped listing is worth seeing.)

   ---- 4. Expected v9 scorecard ---------------------------------------------

   SHOULD FIRE (and why):
   - CE mismatches ................. legacy CE + conversion + sniffed range = garbage estimates
   - Wait statistics ............... CXPACKET + spill IO at minimum
   - Memory grant analysis ......... 0.1% grant; expect UnderPct > 0 for the first time in the series
   - Spill signals ................. sort feeding the merge join, under a starved grant
   - Key lookups ................... rep-range seek returns tens of thousands; Location is a lookup residual (LookupCallsThreshold 10000 should trip)
   - Sort ops + sort-index sugg .... ORDER BY CreationDate DESC, no supporting index
   - Join strategy checks .......... merge (M2M) + whatever the free Posts join picks + high-exec loops
   - Many-to-many merge ............ badges-to-badges on UserId, both inputs non-unique
   - Operator red flags ............ sorts/hashes/rebind patterns from the loops side
   - PlanAffectingConvert .......... ViewCount vs @ViewCountText (nvarchar param cannot be constant-folded)
   - Predicate CONVERT_IMPLICIT .... same conversion, heuristic scan
   - Key/RID lookups section ....... expect 2 operators: Users ~470k calls, Posts ~1.48M calls (v9 lookup fix validation)
   - Expression CONVERT_IMPLICIT ... Reputation * 1.1 in a Compute Scalar
   - SARGability signals ........... UPPER(Location) + leading-wildcard LIKE
   - Parameter sensitivity ......... compiled (1000000-1000001, 'x') vs runtime (100-50000, '%united%')
   - CE model ...................... 70 at compat 160 -> the note should point at the USE HINT
   - Hint signals .................. MAXDOP + USE HINT + MAX_GRANT_PERCENT (first outing for that pattern) + TWO inline MERGE JOIN rows
   - Suggestions ................... should be the longest list of the series, incl. the join-hint neutraliser
   - DB inspection ................. three tables (Users, Posts, Badges), fresh stats on both new indexes

   MIGHT FIRE (report either way):
   - Missing indexes ............... optimizer may recommend on Posts(ViewCount) or Users; conversions can suppress recs
   - Index intersection / bitmap ... depends whether the free join goes parallel hash (bitmap) or loops
   - Parallelism skew .............. reputation ranges are uneven; ratio >= 5 is a high bar

   SHOULD NOT FIRE (negative tests):
   - PSP optimization .............. Parameter Sensitive Plan requires CE 160; the legacy hint disables it - a good teaching note
   - Adaptive join ................. inline merge hints remove the freedom adaptive joins need on those joins
   - Plan regression check ......... new query hash; nothing historical to compare

   ---- 5. Cleanup (when done) ------------------------------------------------

   DROP PROCEDURE IF EXISTS dbo.BeastQuery;
   DROP INDEX IF EXISTS IX_Users_Reputation ON dbo.Users;
   DROP INDEX IF EXISTS IX_Badges_UserId ON dbo.Badges;
   -- IX_Posts_OwnerUserId: keep - it belongs to the act-three story
   ============================================================================ */