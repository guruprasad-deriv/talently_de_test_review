# Part 3 — Scalability & Diagnosis

## 3a. Diagnosis: `agg_monthly_pnl_by_instrument` — 47 min vs 30 min SLA

### The Query

```sql
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

### Root Cause 1 — No Partition on `fct_trade` (Primary cause)

From `scale_profile.md`: `fct_trade` is **not partitioned, not clustered**. The table holds ~9 billion rows / ~720 GB. Every execution of this query performs a full 720 GB scan regardless of any filter.

The `WHERE trade_status = 'closed'` predicate eliminates only ~3% of rows — it is evaluated **post-scan**, not pre-scan. BigQuery has no way to prune partitions because no partitions exist.

At $6.25/TB on-demand, each run bills ~$4.50. Running 8× per day = **~$36/day** for this job alone.

**Fix:** Partition `fct_trade` by `trade_date` (DAY). The agg query covers all historical data so it would still scan everything on a full rebuild — but incremental rebuilds (last 30 days) would scan ~120 GB instead of 720 GB. More importantly, the BI dashboard query (`trade_date >= CURRENT_DATE - 30`) would drop from 720 GB to ~120 GB per run.

```sql
-- After partitioning, incremental rebuild scans only recent partitions
WHERE trade_date >= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)
  AND trade_status = 'closed'
```

---

### Root Cause 2 — Gold Instrument Skew (41% of rows)

From `scale_profile.md`: `instrument = 'Gold'` accounts for **41% of all `fct_trade` rows** (~20.5M rows/day). The `GROUP BY instrument` causes one shuffle partition to hold 41% of the data. That partition completes last and determines the total query runtime — all other instruments finish and wait.

**Fix:** Cluster `fct_trade` on `instrument` after partitioning by `trade_date`. BigQuery's clustered scans co-locate Gold rows, reducing the shuffle cost. For the agg query, this alone will not eliminate the skew effect but reduces the data movement cost.

For Dataflow/Spark equivalents: use `repartition` on a salted key to distribute Gold rows across multiple executors rather than concentrating them in one partition.

---

### Root Cause 3 — CL019 Skew on `client_id` (34% of rows, 18–25 min stall)

From `scale_profile.md`: `client_id = 'CL019'` (Michelle Lee) holds **34% of all rows** (~17M rows/day). The BI dashboard query (`fct_trade JOIN dim_client GROUP BY country`) hits this skew during its daily batch window (09:00–10:00 SGT), causing an 18–25 min stall.

This skew does not directly affect the `agg_monthly_pnl_by_instrument` query (which groups by instrument, not client), but it is the primary driver of the **BI dashboard SLA breach** (12 min vs 5 min SLA).

**Fix for BI dashboard:** Materialise a daily `agg_daily_pnl_by_client_country` table. The dashboard reads from the aggregate, not the raw 9B-row fact table. CL019's rows are collapsed into a single country-level summary row before the dashboard query runs.

---

### Root Cause 4 — Full Rebuild Every 3 Hours (Job Design)

The `agg_monthly_pnl_by_instrument` job rebuilds from scratch on every run (`TRUNCATE + INSERT`). Since `fct_trade` grows by +50M rows / +4 GB per day, 95%+ of the data being scanned on each rebuild is unchanged from the previous run.

**Fix:** Switch to an incremental append strategy:

```sql
-- Only process trades closed since last run
INSERT INTO `warehouse.agg_monthly_pnl_by_instrument`
SELECT DATE_TRUNC(trade_date, MONTH) AS trade_month
     , instrument
     , direction
     , SUM(pnl_usd)                  AS total_pnl
     , COUNT(*)                       AS trade_count
     , AVG(volume_lots)               AS avg_volume
  FROM `warehouse.fct_trade`
 WHERE trade_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
   AND trade_status = 'closed'
 GROUP BY 1, 2, 3
```

Current month's partial aggregate is updated; historical months are never re-scanned. Full rebuild is reserved for month-close reconciliation only.

---

### Root Cause 5 — `dim_client_scd` Full Scan on Compliance Query

From `scale_profile.md`: `dim_client_scd` has no clustering. The compliance snapshot query (`trade_date BETWEEN effective_from AND effective_to`) scans all 12M SCD rows on every run regardless of the trade date range being queried.

**Fix:** Cluster `dim_client_scd` on `client_id`. The point-in-time join filters on `client_id` first — clustering co-locates rows for the same client, reducing the effective scan from 12M rows to the rows for the queried clients only.

---

### Summary of Fixes and Expected Impact

| Root cause | Evidence from scale_profile | Fix | Expected impact |
|---|---|---|---|
| No partition on `fct_trade` | 720 GB full scan, 0% pruning | Partition by `trade_date` (DAY) | Incremental runs scan ~4 GB/day; monthly rebuild scans ~120 GB |
| Gold instrument skew (41%) | Determines total runtime | Cluster on `instrument` after partitioning | Reduces shuffle; Gold partition no longer bottleneck for scan |
| Full rebuild every 3h | 8× $4.50/day = $36/day | Incremental append; full rebuild monthly only | ~87% cost reduction; runtime drops to minutes |
| CL019 client skew (34%) | 18–25 min BI dashboard stall | Pre-aggregated `agg_daily_pnl_by_client_country` | BI dashboard reads aggregate, not 9B-row table |
| `dim_client_scd` no cluster | All 12M rows scanned nightly | Cluster on `client_id` | Compliance query scans only relevant client rows |
