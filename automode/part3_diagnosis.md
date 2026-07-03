# Part 3 — Scalability & Diagnosis

> All root causes and remediation steps in this document are grounded in the specific evidence provided in `scale_profile.md`. Generic troubleshooting checklists are excluded.

---

## 3a. Diagnosing the `agg_monthly_pnl_by_instrument` SLA Breach

The query runs every 3 hours and currently takes **47 minutes** against a **30-minute SLA** — a 57% overrun.

### The Query Under Investigation

```sql
-- Production query — currently 47 min, SLA 30 min
SELECT
  DATE_TRUNC(trade_date, MONTH)  AS trade_month,
  instrument,
  direction,
  SUM(pnl_usd)                   AS total_pnl,
  COUNT(*)                       AS trade_count,
  AVG(volume_lots)               AS avg_volume
FROM `deriv-warehouse.trading.fct_trade`
WHERE trade_status = 'closed'
GROUP BY 1, 2, 3
```

---

### Step 1: What to Look at First

Before changing anything, gather signals from BigQuery's observability surfaces:

```sql
-- 1. Recent job history for this query
SELECT
    job_id
  , creation_time
  , total_bytes_processed
  , total_slot_ms
  , TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_seconds
  , cache_hit
FROM `region-asia-southeast1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE statement_type = 'SELECT'
  AND query LIKE '%agg_monthly_pnl_by_instrument%'
  AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY creation_time DESC
LIMIT 20;
```

```sql
-- 2. Partition state of fct_trade
SELECT
    table_name
  , partition_id
  , total_rows
  , total_logical_bytes
FROM `deriv-warehouse.trading`.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'fct_trade'
ORDER BY partition_id;
-- Expected: __NULL__ or __UNPARTITIONED__ partition containing all 9B rows
-- If partitioned: multiple partitions, each ~1 month of data
```

```sql
-- 3. Slot utilisation timeline during the query
SELECT
    period_start
  , period_slot_ms
FROM `region-asia-southeast1`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_PROJECT
WHERE job_id = '<job_id_from_above>'
ORDER BY period_start;
-- Look for: long tail at end (skew stall), uniform high usage (compute-bound), or gaps (shuffle wait)
```

Also check: **Query Execution Details** in the BigQuery UI — specifically "Stages" tab. Look for a final aggregation stage with dramatically unequal input rows per worker (skew signal).

---

### Step 2: What the Evidence Points To

Every data point below comes directly from `scale_profile.md`.

#### Root Cause 1: `fct_trade` is not partitioned and not clustered

> *"fct_trade — not partitioned, not clustered. Every query reads the full 720 GB regardless of the date range or filter applied."*

This is the primary cause of the 47-minute runtime. Without partitioning, BigQuery cannot prune the table before scanning. The query aggregates by `DATE_TRUNC(trade_date, MONTH)` — but since there is no partition on `trade_date`, BigQuery has no metadata to consult before doing a full table scan.

Contrast with `fct_deposit`, which **is** partitioned by `deposit_date (MONTH)` and queries it at ~160 MB per run — demonstrating the magnitude of the difference partitioning makes on this platform.

**Every run of this query bills ~$4.50** (720 GB × $6.25/TB ÷ 1000 = $4.50), regardless of what month range is actually needed.

#### Root Cause 2: The `trade_status = 'closed'` filter eliminates only 3% of rows

> *"The WHERE trade_status = 'closed' predicate is evaluated post-scan and eliminates only ~3% of rows."*

This filter is applied **after** BigQuery has already scanned all 720 GB. It provides zero partition pruning benefit. In a non-partitioned table, filters on non-cluster columns are always evaluated post-scan. The result: 97% of the 720 GB scan (≈699 GB) is wasted work producing rows that are then discarded.

`trade_status` is a poor partition key candidate: it has only a few distinct values and does not correlate with how the query actually slices the data (by month/instrument/direction).

#### Root Cause 3: Instrument skew — Gold accounts for 41% of all rows

> *"instrument: Gold — 41% of fct_trade rows (~20.5M rows/day)"*

> *"Gold instrument skew impact: Instrument-level aggregates complete quickly for all other instruments; the Gold partition completes last and determines total job runtime."*

The `GROUP BY instrument` in the final aggregation stage creates one output partition per instrument value. The Gold partition receives 41% of the 9 billion rows — approximately 3.7 billion rows. All other instruments complete in parallel; the entire query waits for Gold to finish. This is the **wall time bottleneck** even after fixing the full-table-scan issue, because Gold's partition is 7× larger than what a balanced distribution would produce (41% vs 1/5 = 20% for 5 instruments).

This is visible in the Stages tab: the final GROUP BY stage will show one worker completing dramatically later than the rest.

#### Root Cause 4: Full historical rebuild on every run

The query rebuilds ALL months of history (180 days of data = 9B rows) on **every 3-hour run**. Only the current month's data changes between runs — yet the query re-aggregates January through the current month each time. This is an architectural inefficiency compounding Root Cause 1: if partitioning were in place, the query could rebuild only the current month's partition (~4 GB) instead of 720 GB.

**Current cost: 8 runs/day × $4.50/run = $36/day = ~$1,080/month** for this single job alone — confirmed by the scale profile's cost reference.

---

### Step 3: Remediation Plan (Prioritised)

#### Fix 1 — Partition `fct_trade` by `trade_date` (MONTH) [Highest Impact]

```sql
-- Step 1: Create replacement table with partitioning
CREATE TABLE `deriv-warehouse.trading.fct_trade_v2`
PARTITION BY DATE_TRUNC(trade_date, MONTH)
CLUSTER BY instrument, direction
AS SELECT * FROM `deriv-warehouse.trading.fct_trade`;

-- Step 2: Validate row counts match
SELECT COUNT(*) FROM fct_trade_v2;

-- Step 3: Swap atomically (rename operations in BigQuery via bq CLI)
-- bq cp --no_clobber trading.fct_trade trading.fct_trade_backup
-- bq cp trading.fct_trade_v2 trading.fct_trade (after cutover window)
```

**Expected impact:**

| Scenario | Bytes scanned | Cost per run | Duration estimate |
|----------|--------------|-------------|-------------------|
| Before (no partition) | 720 GB | $4.50 | 47 min |
| After — full 180-day rebuild | 720 GB × (180/180) = 720 GB | $4.50 | ~45 min (same) |
| After — current month only | ~4 GB (1 partition) | $0.025 | ~2-3 min |
| After — last 3 months | ~48 GB (3 partitions) | $0.30 | ~4-5 min |

The key insight: **partition the rebuild strategy** (Fix 3) alongside Fix 1 to realise the full savings. Partition alone without incremental logic still scans all 180 months on a full rebuild.

The clustering by `(instrument, direction)` adds a secondary benefit: within each month partition, BigQuery skips blocks that don't match the `instrument` or `direction` values in the GROUP BY — reducing intra-partition scan further.

**Do not partition by `instrument`** — with Gold at 41%, an instrument-based partition would create a severely skewed Gold partition (~0.66 TB) and tiny partitions for other instruments. BigQuery's partition best practice is to avoid partitions with very unequal data distribution.

#### Fix 2 — Incremental Rebuild (Current Month Only) [High Impact for Frequency]

Replace the full historical rebuild with a dbt incremental model:

```sql
-- dbt model: agg_monthly_pnl_by_instrument.sql
{{ config(
    materialized='incremental',
    unique_key=['trade_month', 'instrument', 'direction'],
    partition_by={'field': 'trade_month', 'data_type': 'date', 'granularity': 'month'},
    incremental_strategy='merge'
) }}

SELECT
  DATE_TRUNC(trade_date, MONTH)  AS trade_month
, instrument
, direction
, SUM(pnl_usd)                   AS total_pnl
, COUNT(*)                       AS trade_count
, AVG(volume_lots)               AS avg_volume
FROM `deriv-warehouse.trading.fct_trade`
WHERE trade_status = 'closed'

{% if is_incremental() %}
  -- Only rebuild months that have received new/updated rows since last run.
  -- Use MAX(trade_month) from the target table itself — NOT _PARTITIONTIME.
  -- _PARTITIONTIME only works for ingestion-time partitioned tables; fct_trade
  -- is column-partitioned (PARTITION BY DATE_TRUNC(trade_date, MONTH)) after Fix 1,
  -- so _PARTITIONTIME is not a valid pseudo-column here.
  AND DATE_TRUNC(trade_date, MONTH) >= (
      SELECT DATE_TRUNC(MAX(trade_month), MONTH)
        FROM {{ this }}
  )
{% endif %}

GROUP BY 1, 2, 3
```

For most 3-hour cycles, only the current month's partition contains new rows — scan drops from 720 GB to ~4 GB.

**Revised cost estimate after Fix 1 + Fix 2:**

| | Before | After (Fix 1+2) |
|-|--------|-----------------|
| Bytes per run | 720 GB | ~4 GB (current month) |
| Cost per run | $4.50 | $0.025 |
| Runs/day | 8 | 8 |
| Daily cost | $36.00 | $0.20 |
| Monthly cost | ~$1,080 | ~$6 |
| **Savings** | | **~$1,074/month** |

#### Fix 3 — CL019 Skew Mitigation (for `GROUP BY client_id` queries)

> *"CL019 skew impact: the CL019 partition stalls the query for 18–25 min during its daily batch window (09:00–10:00 SGT)."*

For the `agg_monthly_pnl_by_instrument` query specifically, CL019 skew is **not the bottleneck** — the query groups by month/instrument/direction, not by client_id. The Gold instrument skew (41%) is the bottleneck here.

However, for queries that DO group by `client_id` (e.g. the BI dashboard joining `fct_trade + dim_client` for daily P&L by country), CL019's 34% share causes one shuffle partition to hold ~17M rows/day. Mitigation:

1. **Short-term**: Add slot autoscaling (BigQuery reservations) with a burst allowance during 09:00–10:00 SGT to absorb the CL019 batch window.
2. **Medium-term**: For GROUP BY client_id queries, use salting to distribute CL019 across multiple slots:

```sql
-- Salted aggregation: distribute CL019 across 4 buckets
SELECT
  client_id
, SUM(pnl_usd) AS total_pnl
FROM (
  SELECT
    CASE
      WHEN client_id = 'CL019'
      THEN CONCAT(client_id, '_', CAST(MOD(FARM_FINGERPRINT(trade_id), 4) AS STRING))
      ELSE client_id
    END AS client_id_salted
  , pnl_usd
  FROM fct_trade
)
GROUP BY client_id_salted
-- Then aggregate the 4 CL019_0..3 buckets back into CL019 in an outer query
```

#### Fix 4 — Address Other SLA Breaches (Context from Scale Profile)

**BI dashboard P&L refresh (12 min vs 5 min SLA):** This query joins `fct_trade` (720 GB full scan) + `dim_client` on `client_id`, filters `trade_date >= CURRENT_DATE - 30`, groups by country. Root cause: same as above — `fct_trade` has no partition, so `trade_date >= CURRENT_DATE - 30` filter has no effect. After Fix 1 (partition by trade_date), this filter will prune to ~30 days of data ≈ 24 GB instead of 720 GB. Additionally: create a BI-facing materialized view pre-aggregating `(trade_date, country, total_pnl)` refreshed every 30 min. Dashboard queries the materialized view (~18,000 rows) not the raw fact.

**Client deposit-to-trade lag report (38 min vs 20 min SLA):** The query has **no date filter** — full scan on both `fct_trade` (720 GB) and `fct_deposit` (28 GB). `fct_deposit` is partitioned but the filter doesn't use the partition key. Fix: (a) Pre-materialise `first_deposit_date` and `first_trade_date` per client in a `dim_client_activity` table updated incrementally; (b) The lag report then queries this pre-aggregated table (2.4M rows) instead of scanning both fact tables. Sub-1-minute runtime.

**Compliance snapshot — `dim_client_scd` 12M row full scan:** The point-in-time SCD join (`trade_date BETWEEN effective_from AND effective_to`) scans all 12M SCD rows regardless of the trade date range. Fix: cluster `dim_client_scd` by `client_id`. The SCD lookup for a set of specific client_ids then only reads the relevant blocks (collocated by client_id), not the full 12M rows.

---

### Step 4: Summary of Root Causes

| Root cause | Evidence from scale_profile.md | Impact | Fix |
|------------|-------------------------------|--------|-----|
| `fct_trade` not partitioned | "not partitioned, not clustered. Every query reads the full 720 GB" | Full 720 GB scan every run | Partition by trade_date MONTH |
| `trade_status` filter useless | "eliminates only ~3% of rows" | 697 GB scanned wastefully post-filter | Partition; trade_status is not a useful filter column |
| Gold instrument skew at 41% | "Gold — 41% (~20.5M rows/day)" | Final GROUP BY stage stalls on Gold; determines total runtime | Cluster by instrument; incremental rebuild limits exposure |
| Full historical rebuild | 8 runs/day × 720 GB | $36/day for one job; most data is unchanged between runs | Incremental dbt model; rebuild only changed month partitions |
| Cost compounding | "$4.50 per rebuild × 8/day = $36/day" | ~$1,080/month for one aggregation job | All fixes combined → ~$6/month |
