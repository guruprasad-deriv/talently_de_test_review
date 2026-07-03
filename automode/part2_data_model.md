# Part 2 — Data Model & Historization

---

## 2a. Dimensional Model / ERD

### Schema approach: Kimball Star Schema

**Why Kimball over Data Vault:**

| Criterion | Kimball (chosen) | Data Vault |
|-----------|-----------------|------------|
| Grain clarity | fct_trade = 1 row/trade, fct_deposit = 1 row/deposit — unambiguous | Would decompose into hub_client + hub_trade + link_client_trade + sat_* — adds layers without additional clarity |
| Query pattern | BI dashboards, C-suite reports: aggregate by date/instrument/country — star schema joins are optimal | Hub-link-satellite requires multiple joins before any aggregation |
| Team size | ~4 data engineers — Kimball's simplicity is maintainable | DV's auditability and flexibility justify complexity only at larger teams or 10+ volatile source systems |
| Source system count | 4 source tables from 1 operational database | DV shines when 10+ heterogeneous sources need to be integrated |

**When Data Vault would win:** If the number of source systems grew to 10+ with different schemas for overlapping concepts (e.g. two trading platforms both contributing trades), DV's hub-and-spoke handles schema evolution more gracefully. That is not this dataset's current state.

---

### Dimensional Model

```
                         ┌──────────────┐
                         │  dim_date    │
                         │  date_key PK │
                         └──────┬───────┘
                                │
            ┌───────────────────┼────────────────────┐
            │                   │                    │
     ┌──────▼────────┐   ┌──────▼────────┐   ┌──────▼──────────┐
     │  fct_trade    │   │  fct_deposit  │   │ dim_instrument  │
     │  trade_sk PK  │   │  deposit_sk PK│   │ instrument_sk PK│
     │  client_sk FK ├─┐ │  client_sk FK ├─┐ │ instrument_name │
     │  instrument_sk│ │ │  date_key  FK │ │ │ asset_class     │
     │  date_key  FK │ │ │  amount_usd   │ │ └─────────────────┘
     │  pnl_usd      │ │ │  fee_usd      │ │
     │  volume_lots  │ │ │  payment_     │ │
     │  direction    │ │ │  method       │ │
     │  trade_status │ │ └───────────────┘ │
     └───────────────┘ │                   │
                       │  ┌───────────────────────────┐
                       └─►│  dim_client_scd           │
                       └─►│  client_sk  PK (surrogate)│
                          │  client_id  NK             │
                          │  effective_from            │
                          │  effective_to              │
                          │  is_current                │
                          │  is_deleted                │
                          │  risk_category    ◄ SCD2   │
                          │  account_status   ◄ SCD2   │
                          │  account_balance_usd ◄SCD1 │
                          └────────────┬──────────────┘
                                       │ client_id
                          ┌────────────▼──────────────┐
                          │  dim_client               │
                          │  client_id  PK            │
                          │  client_sk  (current sk)  │
                          │  full_name                │
                          │  country                  │
                          │  account_type             │
                          │  kyc_status               │
                          │  nationality              │
                          │  signup_date              │
                          │  assigned_manager         │
                          │  preferred_language       │
                          │  is_deleted               │
                          └───────────────────────────┘
```

---

### Table Definitions

**`fct_trade`** — Grain: one row per closed or open trade.

| Column | Type | Notes |
|--------|------|-------|
| `trade_sk` | INT64 | Surrogate key |
| `trade_id` | STRING | Natural key (degenerate dimension) |
| `client_sk` | INT64 | FK → `dim_client_scd.client_sk` at `trade_date` (point-in-time) |
| `instrument_sk` | INT64 | FK → `dim_instrument` |
| `date_key` | INT64 | FK → `dim_date` |
| `trade_date` | DATE | Partition key (MONTH) |
| `direction` | STRING | buy / sell |
| `volume_lots` | FLOAT64 | |
| `open_price` | FLOAT64 | |
| `close_price` | FLOAT64 | |
| `pnl_usd` | FLOAT64 | TRD012 flagged: open=close=2320 but pnl=245 (dq_flag) |
| `trade_status` | STRING | closed / open |
| `dq_flag` | STRING | NULL or e.g. `inactive_account_trade` |

**`fct_deposit`** — Grain: one row per deposit event.

| Column | Type | Notes |
|--------|------|-------|
| `deposit_sk` | INT64 | Surrogate key |
| `deposit_id` | STRING | Natural key |
| `client_sk` | INT64 | FK → `dim_client_scd` at `deposit_date` |
| `client_id` | STRING | Natural key — denormalised for direct joins and orphan reconciliation |
| `date_key` | INT64 | FK → `dim_date` |
| `deposit_date` | DATE | Partition key (MONTH) — already applied in source |
| `amount_usd` | FLOAT64 | |
| `fee_usd` | FLOAT64 | |
| `payment_method` | STRING | Normalised from `method` alias in vendor files |
| `currency_original` | STRING | |
| `exchange_rate` | FLOAT64 | |
| `status` | STRING | completed / pending |
| `processing_days` | INT64 | |
| `late_delivery_flag` | BOOL | TRUE for records from late vendor files |
| `dq_flag` | STRING | NULL or e.g. `fee_exceeds_amount` |

**`dim_client_scd`** — SCD Type 2 history for compliance-critical fields.

| Column | Type | Notes |
|--------|------|-------|
| `client_sk` | INT64 | Surrogate key — new row on each change |
| `client_id` | STRING | Natural key |
| `effective_from` | TIMESTAMP | Inclusive start of this row's validity |
| `effective_to` | TIMESTAMP | `9999-12-31 23:59:59` for current row |
| `is_current` | BOOL | TRUE for the current/latest row |
| `is_deleted` | BOOL | TRUE after CDC DELETE event |
| `deleted_at` | TIMESTAMP | NULL unless is_deleted=TRUE |
| `risk_category` | STRING | low / medium / high — SCD2 |
| `account_status` | STRING | active / inactive / suspended / deleted — SCD2 |
| `account_balance_usd` | FLOAT64 | SCD1 — overwrite; actual history in fct_deposit+fct_trade |

**`dim_client`** — Current state (SCD Type 1).

| Column | Type | Notes |
|--------|------|-------|
| `client_id` | STRING | PK |
| `client_sk` | INT64 | FK → current row in dim_client_scd |
| `full_name` | STRING | |
| `country` | STRING | |
| `account_type` | STRING | standard / professional / vip |
| `kyc_status` | STRING | approved / rejected / pending |
| `nationality` | STRING | |
| `signup_date` | DATE | |
| `assigned_manager` | STRING | |
| `preferred_language` | STRING | |
| `is_deleted` | BOOL | |

**`dim_instrument`** — Static lookup.

| Column | Type | Notes |
|--------|------|-------|
| `instrument_sk` | INT64 | Surrogate key |
| `instrument_name` | STRING | EUR/USD, Gold, BTC/USD, USD/JPY, S&P500 |
| `asset_class` | STRING | FX, Commodity, Crypto, Index |

**`dim_date`** — Standard date spine (calendar year grain).

---

### Late-Arriving Dimension Records

If a fact record arrives before its corresponding dimension row exists (e.g. DEP020 references `client_id = CL031` which is absent from `client_signup`):

1. Assign `client_sk = -1` (the "unknown client" surrogate — a pre-seeded row in `dim_client_scd`).
2. Load the fact row with `client_sk = -1`.
3. Run a daily reconciliation job. For each orphan fact, resolve `client_sk` using a **point-in-time SCD join** (`deposit_date BETWEEN effective_from AND effective_to`) — not a current-row join. The dim row that was current when the deposit occurred may not be the current row today.

```sql
-- Step 3a: Identify orphan deposits and their now-resolvable dim rows
SELECT fd.deposit_id
     , fd.client_id
     , fd.deposit_date
     , scd.client_sk            AS resolved_client_sk
  FROM `deriv-warehouse.trading.fct_deposit`     fd
  JOIN `deriv-warehouse.trading.dim_client_scd`  scd
    ON scd.client_id      = fd.client_id
   AND CAST(scd.effective_from AS DATE) <= fd.deposit_date
   AND fd.deposit_date          < CAST(scd.effective_to AS DATE)
 WHERE fd.client_sk = -1;

-- Step 3b: Backfill client_sk with point-in-time surrogate
UPDATE `deriv-warehouse.trading.fct_deposit` fd
   SET fd.client_sk = scd.client_sk
  FROM `deriv-warehouse.trading.dim_client_scd` scd
 WHERE fd.client_id                        = scd.client_id
   AND fd.client_sk                        = -1
   AND CAST(scd.effective_from AS DATE)   <= fd.deposit_date
   AND fd.deposit_date                     < CAST(scd.effective_to AS DATE);
```

This ensures fact rows are never blocked on dimension availability, and the unknown-client pattern is visible and auditable.

---

## 2b. Historization (SCD)

### Question 1: Which SCD type for each attribute?

**`risk_category` → SCD Type 2**

Justification: `risk_category` determines which trading instruments and leverage a client can access. For compliance reporting (e.g. MiFID II suitability assessment, MAS risk profiling), we need to answer: "What was this client's risk category on the date of this trade?" Without SCD2, we cannot reconstruct that fact post-hoc. The CDC data confirms this is a meaningful change: CL014 shifts from `high` to `medium` risk (LSN 1009) — a change that affects their permitted instruments going forward.

**`account_status` → SCD Type 2**

Justification: `account_status` governs whether a client can trade. For regulatory audit, we need to prove that a trade was executed when the account was `active`. The inactive-account trade anomaly (TRD006, CL008 trading while `inactive`) can only be detected if we have point-in-time status history. Without SCD2, the current `inactive` state would be back-projected onto the historical trade date, making it impossible to distinguish between "status changed after the trade" and "trade was executed illegally".

**`account_balance_usd` → SCD Type 1 (overwrite)**

This is the most nuanced decision. `account_balance_usd` changes with every deposit and every closed trade — if treated as SCD2, it would create one history row per transaction, effectively duplicating `fct_deposit` and `fct_trade` data inside the dimension. That is the wrong layer.

The authoritative history of the balance is: `initial_deposit + SUM(fct_deposit.amount_usd) + SUM(fct_trade.pnl_usd)`. This is computable from facts. In `dim_client_scd`, the balance field represents the "current balance as of this SCD row's effective_from" — which is useful as a snapshot but not as the primary historical record.

Trade-off accepted: if you need to query "what was CL001's balance on 2024-11-14?", you must reconstruct from facts (sum deposits and PnL up to that date) rather than reading a SCD2 row. This is the correct architectural choice.

---

### Question 2: Update event handling — walk-through for CL014

CL014 has two updates in the CDC file. After sorting by LSN:

```
LSN 1008 | commit_ts: 2024-11-20T09:00:00Z | op: update
  before: {risk_category: "high", account_balance_usd: 9800, account_status: "active"}
  after:  {risk_category: "high", account_balance_usd: 12300, account_status: "active"}

LSN 1009 | commit_ts: 2024-11-21T10:00:00Z | op: update
  before: {risk_category: "high", account_balance_usd: 12300, account_status: "active"}
  after:  {risk_category: "medium", account_balance_usd: 12300, account_status: "active"}
```

**Processing LSN 1008:**

Only `account_balance_usd` changed (9800 → 12300). `account_balance_usd` is a **SCD1 field** — it is a running operational value whose full history is computable from `fct_deposit` + `fct_trade`. No SCD Type 2 row is created; the existing row is updated in-place.

```sql
-- SCD1: account_balance_usd changed; no SCD2-tracked field (risk_category,
-- account_status) changed → update in-place, no new history row.
UPDATE dim_client_scd
   SET account_balance_usd = 12300.00
 WHERE client_id  = 'CL014'
   AND is_current = TRUE;
```

**Processing LSN 1009:**

```sql
-- Step 1: Close the current row (risk_category changes — SCD2-tracked field)
UPDATE dim_client_scd
   SET effective_to = TIMESTAMP '2024-11-21T10:00:00Z'
     , is_current   = FALSE
 WHERE client_id   = 'CL014'
   AND is_current  = TRUE;

-- Step 2: Insert new row with updated risk_category
INSERT INTO dim_client_scd
  (client_id, effective_from, effective_to, is_current,
   risk_category, account_balance_usd, account_status, is_deleted)
VALUES
  ('CL014', '2024-11-21T10:00:00Z', '9999-12-31 23:59:59', TRUE,
   'medium', 12300.00, 'active', FALSE);
```

**Final state of CL014 SCD rows:**

LSN 1008 was SCD1 — `account_balance_usd` updated in-place (9800 → 12300), no new row created. LSN 1009 was SCD2 — `risk_category` (high → medium) triggered close + insert.

| client_sk | effective_from | effective_to | risk_category | account_balance_usd | is_current |
|-----------|---------------|-------------|---------------|--------------------|-----------| 
| (original) | 2024-03-01 | 2024-11-21T10:00:00Z | high | 12300 | FALSE |
| (new 1) | 2024-11-21T10:00:00Z | 9999-12-31 | medium | 12300 | TRUE |

Two rows, not three: the balance-only update (LSN 1008) leaves no trace in the SCD2 history — only the compliance-critical `risk_category` change (LSN 1009) creates a new history row.

---

### Question 3: Delete event handling — CL012

CDC event at LSN 1010:
```json
{"lsn": 1010, "op": "delete", "client_id": "CL012",
 "before": {"full_name": "David Tan", "risk_category": "low",
             "account_balance_usd": 0.00, "account_status": "suspended"},
 "after": null}
```

**In the warehouse:**

```sql
-- 1. Close the current SCD row
UPDATE dim_client_scd
   SET effective_to = TIMESTAMP '2024-11-21T14:00:00Z'
     , is_current   = FALSE
 WHERE client_id  = 'CL012'
   AND is_current = TRUE;

-- 2. Insert a terminal "deleted" SCD row
INSERT INTO dim_client_scd
  (client_id, effective_from, effective_to, is_current,
   risk_category, account_balance_usd, account_status, is_deleted, deleted_at)
VALUES
  ('CL012', '2024-11-21T14:00:00Z', '9999-12-31 23:59:59', TRUE,
   'low', 0.00, 'deleted', TRUE, '2024-11-21T14:00:00Z');

-- 3. Update current-state dimension
UPDATE dim_client
   SET account_status = 'deleted'
     , is_deleted     = TRUE
     , deleted_at     = TIMESTAMP '2024-11-21T14:00:00Z'
 WHERE client_id = 'CL012';
```

**Why not hard delete:**
- CL012's trades (`fct_trade`) and deposits (`fct_deposit`) must remain queryable for audit and P&L reporting — hard deleting the dimension row would break all historical joins.
- In financial services, regulatory requirements (MiFID II Article 25, MAS Notice SFA04-N12) mandate that client interaction records are retained for a minimum of 5 years. Deleting the warehouse row would violate this.
- GDPR right-to-erasure applies to PII fields (`full_name`, `email`, `date_of_birth`) — handled by a separate anonymisation job that overwrites those fields with pseudonymous values, not by deleting the row.

CL012 was already `account_status = 'suspended'` with `kyc_status = 'rejected'`. The delete is the expected end-of-lifecycle event.

---

### Question 4: LSN out-of-order handling

The file delivers events in arrival order. LSNs in the file: **1005, 1009, 1001, 1004, 1010, 1012, 1003, 1015, 1008, 1018, 1006, 1020**.

CL001 has three events:
- LSN 1004 | commit_ts 2024-11-15T10:30:00Z: risk_category medium → high
- LSN 1005 | commit_ts 2024-11-15T11:00:00Z: account_balance_usd 1250 → 1850
- LSN 1006 | commit_ts 2024-11-15T14:00:00Z: account_status active → under_review

**If applied in arrival order (WRONG):**

LSN 1005 arrives first in the file. The `before` state in LSN 1005 says `risk_category = "high"` — but at the time of arrival-order processing, we haven't applied LSN 1004 yet, so the warehouse still has `risk_category = "medium"`. The pipeline would detect a mismatch between the expected `before` state and the actual warehouse state, signalling that the events are out of order.

More critically: if LSN 1006 were applied before LSN 1004, the SCD row inserted for LSN 1006's change would have `risk_category = "medium"` (current state before LSN 1004 is applied), and then LSN 1004 would try to close a row and insert with `risk_category = "high"` — resulting in two "current" rows with conflicting states.

**If applied in LSN order (CORRECT):**

```
Start state:  risk_category=medium, balance=1250, status=active
After LSN 1004: risk_category=HIGH,   balance=1250, status=active
After LSN 1005: risk_category=high,   balance=1850, status=active
After LSN 1006: risk_category=high,   balance=1850, status=UNDER_REVIEW
```

**Implementation:** The staging model reads the entire JSONL batch into memory, sorts by `lsn ASC`, then processes events sequentially. For a streaming architecture, use a watermark with `lsn` as the ordering field (not `commit_ts`, which can also be slightly out of order for transactions that commit concurrently).

LSN gap detection: if the sequence jumps from 1006 to 1008 (skipping 1007), wait up to 30 minutes for LSN 1007 to arrive. If it does not arrive within the timeout, log a gap event and continue — the missing LSN may represent a transaction type not tracked by the CDC filter.

---

### Question 5: Reloading a historical date range

To re-process November 2024 without corrupting history:

```sql
-- Step 1: Identify affected clients (those with CDC events in Nov 2024)
CREATE TEMP TABLE clients_to_reload AS
SELECT DISTINCT client_id
FROM stg_cdc_events
WHERE commit_ts BETWEEN '2024-11-01' AND '2024-11-30 23:59:59';

-- Step 2: Back up the current SCD rows for those clients
CREATE TABLE dim_client_scd_backup_nov2024 AS
SELECT * FROM dim_client_scd
WHERE client_id IN (SELECT client_id FROM clients_to_reload);

-- Step 3: Delete the SCD rows created by November events
-- (i.e. rows with effective_from within November)
DELETE FROM dim_client_scd
WHERE client_id IN (SELECT client_id FROM clients_to_reload)
  AND effective_from >= '2024-11-01';

-- Step 4: Reset only the LAST pre-November row per client to is_current = TRUE.
-- A naive UPDATE WHERE effective_from < '2024-11-01' would incorrectly set ALL
-- historical SCD rows to is_current=TRUE, violating the single-current-row invariant.
-- ROW_NUMBER() identifies the single correct row (latest effective_from before Nov).
MERGE INTO dim_client_scd AS target
USING (
    SELECT client_sk
      FROM (
          SELECT client_sk
               , ROW_NUMBER() OVER (
                     PARTITION BY client_id
                     ORDER BY effective_from DESC
                 ) AS rn
            FROM dim_client_scd
           WHERE client_id     IN (SELECT client_id FROM clients_to_reload)
             AND effective_from < TIMESTAMP '2024-11-01'
      ) ranked
     WHERE rn = 1
) AS pre_nov_rows
   ON target.client_sk = pre_nov_rows.client_sk
 WHEN MATCHED
 THEN UPDATE SET target.effective_to = TIMESTAMP '9999-12-31 23:59:59'
              , target.is_current   = TRUE;

-- Step 5: Re-apply November CDC events in LSN order
-- (re-run the cdc_ingestion_dag for the November date range)
-- The checkpoint table must be reset for these clients' LSN ranges

-- Step 6: Validate
SELECT client_id, COUNT(*) AS scd_rows
FROM dim_client_scd
WHERE client_id IN (SELECT client_id FROM clients_to_reload)
GROUP BY 1
ORDER BY 2 DESC;
```

Steps 3-4 must be executed in a transaction to ensure atomicity. The backup table in Step 2 provides a rollback path. The checkpoint table records the reload event with `reload_reason` and `requested_by` for audit.

---

### Question 6: 250+ field SCD optimisation — Split SCD pattern

Maintaining full SCD Type 2 history on a 250-field table creates a row for every field change. If `account_balance_usd` changes daily (which it does — every deposit and trade), that's 365 SCD rows per client per year × 2.4M clients = 876M rows per year, growing dim_client_scd at ~7× the rate of dim_client itself.

**Pattern: Split SCD by attribute group**

Separate the 250 fields into groups based on change frequency and audit requirement:

| Group | Fields (examples) | SCD type | Rationale |
|-------|------------------|----------|-----------|
| **Compliance-critical** | `risk_category`, `account_status`, `kyc_status` | SCD Type 2 | Regulatory audit; 5-10 changes per client lifetime |
| **Operational / running values** | `account_balance_usd`, `last_login_date`, `assigned_manager` | SCD Type 1 | High-frequency changes; history computable from facts |
| **Static / rare** | `date_of_birth`, `nationality`, `preferred_language`, `country` | SCD Type 0 / JSON | Rarely change; no point-in-time compliance requirement |

Implementation:
- `dim_client_scd` tracks only Group 1 fields (~5 fields) → keeps SCD2 row count manageable.
- `dim_client` tracks Groups 2-3 as current-state overwrite.
- Group 3 fields stored as a JSON column (`STRUCT` or `JSON` type) for schema flexibility.

**Storage savings estimate:**
- Full SCD2 on 250 fields: 12M rows × 250 fields = 3B field-values
- Split SCD: 12M rows × 5 fields (compliance only) = 60M field-values → **98% reduction in SCD2 storage**
- For `dim_client_scd` specifically: at 2.8 GB current size for 12M rows, the split approach would reduce this to ~56 MB.

**Additional pattern — Columnar CDC (for deep audit requirements):**

```sql
CREATE TABLE client_attribute_changes (
    client_id    STRING,
    changed_at   TIMESTAMP,
    lsn          INT64,
    field_name   STRING,   -- e.g. 'risk_category'
    old_value    STRING,   -- serialised as string
    new_value    STRING,
    source       STRING    -- 'cdc', 'manual_correction', etc.
);
```

This allows tracking individual field changes at the column level without row duplication, and is queryable as "show me every time CL001's risk_category changed" without a full SCD2 table scan.

---

## 2c. Analytical SQL Query

> Dialect: **BigQuery Standard SQL**

### Query A — Deposit count by country

Returns every country from `client_signup`, including those with zero deposits, sorted with zero-deposit countries first, then descending by deposit count.

Note: `DEP020` references `client_id = CL031` which is not in `client_signup`. Since we join FROM `client_signup` LEFT to `client_deposit`, this orphan deposit is not included — correct behaviour (the question asks for countries in `client_signup`).

```sql
-- BigQuery Standard SQL
-- Deposit count by country; zero-deposit countries appear first

SELECT cs.country
     , COUNT(d.deposit_id)                AS deposit_count
  FROM `deriv-warehouse.trading.client_signup` cs
  LEFT JOIN `deriv-warehouse.trading.client_deposit` d
         ON d.client_id = cs.client_id
 GROUP BY cs.country
 ORDER BY deposit_count ASC   -- zero-deposit countries sort first (COUNT = 0)
        , cs.country ASC      -- alphabetical tie-break within same count
```

**Why `COUNT(d.deposit_id)` not `COUNT(*)`:** With a LEFT JOIN, unmatched `client_signup` rows produce a NULL-padded row. `COUNT(*)` counts this NULL row as 1. `COUNT(d.deposit_id)` counts NULLs as 0 — the correct semantics for "clients with no deposits".

**Alternative using explicit CASE for tie-breaking clarity:**

```sql
-- Explicit sort: zero-deposit countries first, then descending by count
SELECT cs.country
     , COUNT(d.deposit_id)                AS deposit_count
  FROM `deriv-warehouse.trading.client_signup` cs
  LEFT JOIN `deriv-warehouse.trading.client_deposit` d
         ON d.client_id = cs.client_id
 GROUP BY cs.country
 ORDER BY CASE WHEN COUNT(d.deposit_id) = 0 THEN 0 ELSE 1 END ASC
        , COUNT(d.deposit_id) DESC
        , cs.country ASC
```

---

### Query B — Final client state from CDC (LSN-ordered)

Shows the latest value of each CDC-tracked field per client, correctly derived by applying events in LSN order.

```sql
-- BigQuery Standard SQL
-- Latest CDC state per client (LSN-ordered, most recent per client)

WITH cdc_ranked AS (
  SELECT client_id
       , lsn
       , commit_ts
       , op
       , JSON_VALUE(after_json, '$.risk_category')       AS risk_category
       , CAST(JSON_VALUE(after_json, '$.account_balance_usd') AS FLOAT64)
                                                          AS account_balance_usd
       , JSON_VALUE(after_json, '$.account_status')       AS account_status
       , ROW_NUMBER() OVER (
             PARTITION BY client_id
             ORDER BY lsn DESC
         )                                                AS rn
    FROM `deriv-warehouse.trading.stg_cdc_events`
   WHERE op IN ('insert', 'update')  -- exclude DELETEs; handled separately via is_deleted flag
)
SELECT client_id
     , risk_category
     , account_balance_usd
     , account_status
     , commit_ts         AS last_changed_at
     , lsn               AS last_lsn
  FROM cdc_ranked
 WHERE rn = 1
 ORDER BY client_id
```

**Expected results for key clients from the CDC data:**

| client_id | risk_category | account_balance_usd | account_status | last_lsn |
|-----------|---------------|--------------------|--------------:|----------|
| CL001 | high | 1850.00 | under_review | 1006 |
| CL014 | medium | 12300.00 | active | 1009 |
| CL019 | high | 80000.00 | active | 1015 |
| CL025 | medium | 18200.00 | active | 1020 |
