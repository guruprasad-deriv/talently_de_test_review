# Part 2 — Data Model & Historization

## Part 2a — Dimensional Model / ERD

### Approach: Kimball Star Schema

Kimball over Data Vault for the following reasons:

- Two known, stable source systems (warehouse tables + vendor feed) with a fixed, understood grain
- All consumers are analytics-oriented — BI dashboards, C-suite reports, retention team queries. Star schema joins are direct and BI-tool friendly.
- The immutable raw landing layer already provides the auditability that Data Vault's hub/satellite pattern is designed to deliver. Adding Data Vault on top would duplicate that concern.
- Data Vault suits enterprises with dozens of constantly evolving sources. Four sources with known schemas do not justify the link/satellite/hub overhead.

---

### ERD

```
                         ┌──────────────────────────────────────┐
                         │              dim_client               │
                         │          (current snapshot)           │
                         │──────────────────────────────────────│
                         │  client_id          STRING   NK/PK    │
                         │  ── from client_signup ──             │
                         │  signup_date        DATE              │
                         │  country            STRING            │
                         │  email              STRING            │
                         │  kyc_status         STRING            │
                         │  account_type       STRING            │
                         │  referral_source    STRING            │
                         │  signup_platform    STRING            │
                         │  assigned_manager   STRING            │
                         │  promo_code         STRING (nullable) │
                         │  ── from client_profile ──            │
                         │  full_name          STRING            │
                         │  date_of_birth      DATE              │
                         │  nationality        STRING            │
                         │  risk_category      STRING  ◄── CDC   │
                         │  account_balance_usd NUMERIC ◄── CDC  │
                         │  account_status     STRING  ◄── CDC   │
                         │  currency           STRING            │
                         │  last_login_date    DATE              │
                         │  ── soft delete ──                    │
                         │  is_deleted         BOOL              │
                         │  deleted_at         TIMESTAMP         │
                         └──────────────┬───────────────────────┘
                                        │ client_id (1:many)
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
                    ▼                   │                   ▼
   ┌────────────────────────┐           │     ┌────────────────────────┐
   │       fct_deposit      │           │     │       fct_trade        │
   │────────────────────────│           │     │────────────────────────│
   │ deposit_id   STRING PK  │           │     │ trade_id    STRING PK  │
   │ client_id    STRING FK  │           │     │ client_id   STRING FK  │
   │ deposit_date DATE       │           │     │ trade_date  DATE       │
   │ amount_usd   NUMERIC    │           │     │ instrument  STRING     │
   │ payment_method STRING   │           │     │ direction   STRING     │
   │ currency_original STRING│           │     │ volume_lots NUMERIC    │
   │ exchange_rate NUMERIC   │           │     │ open_price  NUMERIC    │
   │ status       STRING     │           │     │ close_price NUMERIC    │
   │ processing_days INT     │           │     │ pnl_usd     NUMERIC    │
   │ fee_usd      NUMERIC    │           │     │ trade_status STRING    │
   │ loaded_at    TIMESTAMP  │           │     │ loaded_at   TIMESTAMP  │
   └────────────────────────┘           │     └────────────────────────┘
                                        │
                    ┌───────────────────┘
                    ▼
   ┌──────────────────────────────────────┐
   │           dim_client_scd             │
   │       (SCD Type 2 history)           │
   │──────────────────────────────────────│
   │  client_scd_key  STRING   PK (uuid)  │
   │  client_id       STRING   NK/FK      │
   │  risk_category   STRING              │
   │  account_balance_usd NUMERIC         │
   │  account_status  STRING              │
   │  effective_from  TIMESTAMP           │
   │  effective_to    TIMESTAMP (null=current) │
   │  is_current      BOOL                │
   └──────────────────────────────────────┘
```

---

### Table Definitions

#### `fct_deposit`

- **Grain:** one row per deposit transaction
- **Natural key:** `deposit_id` — VDEP### (new vendor) and DEP### (previous vendor) coexist in the same column. No ID translation. No surrogate key generated.
- **Partition:** `deposit_date` (DAY) — enables partition pruning on the INSERT IF NOT EXISTS existence check and on all date-range analytics queries
- **Cluster:** `client_id` — most queries filter or join on client

#### `fct_trade`

- **Grain:** one row per closed trade
- **Natural key:** `trade_id`
- **Partition:** `trade_date` (DAY) — the single most impactful change available (see Part 3)
- **Cluster:** `instrument`, `client_id` — aligns with the two highest-cardinality skew dimensions (Gold 41%, CL019 34%)

#### `dim_client`

- **Grain:** one row per client (current snapshot)
- **Natural key:** `client_id`
- **Source merge:** `client_signup` (static fields) + `client_profile` (mutable fields) joined on `client_id`. Both are 1:1 — merging into one dimension removes a join from every downstream query.
- **Mutable fields updated via CDC:** `risk_category`, `account_balance_usd`, `account_status`
- **Soft delete:** `is_deleted = TRUE`, `deleted_at = commit_ts` when CDC op=delete. Row is never physically removed — FK references from `fct_deposit` and `fct_trade` remain valid.

#### `dim_client_scd`

- **Grain:** one row per version of a client's mutable profile attributes
- **Tracks only CDC-managed fields:** `risk_category`, `account_balance_usd`, `account_status`. Storing all 30 client fields in SCD2 would balloon storage unnecessarily (see Part 2b Q6).
- **Point-in-time join pattern:**

```sql
SELECT t.trade_id
     , t.pnl_usd
     , s.risk_category      AS risk_at_trade_time
     , s.account_status     AS status_at_trade_time
  FROM fct_trade           t
  JOIN dim_client_scd      s
    ON s.client_id     = t.client_id
   AND t.trade_date BETWEEN s.effective_from AND COALESCE(s.effective_to, CURRENT_TIMESTAMP)
```

---

### Late-Arriving Dimension Records

When a vendor deposit arrives for a `client_id` that exists in `client_signup` but whose `dim_client` row has not yet loaded:

- The deposit is **not quarantined** — the data is valid.
- A **Warning DQ check** fires: `client_id_in_signup_not_yet_in_dim_client`. Logged to `pipeline_manifest`.
- The deposit is inserted into `fct_deposit`. The FK to `dim_client` resolves automatically when the dimension row loads — no reprocessing required.
- Any BI query joining `fct_deposit → dim_client` will return NULL for that client until the dimension row exists. This is acceptable and self-healing.

When a vendor deposit arrives for a `client_id` that does not exist in `client_signup` at all (e.g. VDEP020, CL099):

- This is a genuine orphan — **Critical DQ check**, quarantine to error table. No deposit inserted.

---

## Part 2b — Historization (SCD)

### Q1 — Which SCD type, and why?

**`dim_client` (current snapshot) — SCD Type 1**
The current values of `risk_category`, `account_balance_usd`, and `account_status` are updated in-place via CDC MERGE. Always reflects the latest known state.

**`dim_client_scd` (history table) — SCD Type 2**
A new row is opened for every change. Closed rows retain `effective_from` / `effective_to` timestamps derived from CDC `commit_ts`.

**Why SCD Type 2 for the history table:**
- The compliance snapshot query in `scale_profile.md` requires point-in-time joins: `trade_date BETWEEN effective_from AND effective_to`. SCD2 is the only pattern that supports this.
- `risk_category` and `account_status` are regulatory fields — knowing a client's risk profile at the exact time of a trade is an audit requirement, not just an analytics convenience.
- `account_balance_usd` history is needed for the deposit-to-trade lag analysis.

**Trade-offs:**

| Trade-off | Impact |
|---|---|
| Storage growth | `dim_client_scd` already at 12M rows (scale_profile). Mitigated by tracking only 3 CDC fields, not all 30 (see Q6). |
| Query complexity | Point-in-time join requires `BETWEEN effective_from AND COALESCE(effective_to, CURRENT_TIMESTAMP)` — harder than a simple FK join |
| Late CDC events | An event arriving out of order may require retroactive row adjustment (close wrong row, re-open) — handled by the watermark approach in Q4 |
| Delete handling | Delete is terminal — current row is closed, no new row opened (see Q3) |

SCD Type 3 (add a `previous_value` column) was considered and rejected — it only retains one prior value, insufficient for compliance queries that need full history.

---

### Q2 — How are update events handled?

Walk-through using LSN 1008: CL014 `account_balance_usd` 9800 → 12300.

**Step 1 — Read incremental events from staging**

```sql
SELECT client_id
     , after.risk_category        AS new_risk_category
     , after.account_balance_usd  AS new_balance
     , after.account_status       AS new_status
     , commit_ts
     , lsn
  FROM staging.cdc_client_profile
 WHERE lsn > (
         SELECT max_processed_lsn
           FROM pipeline_metadata
          WHERE source = 'cdc_client_profile'
       )
   AND op = 'update'
 ORDER BY lsn ASC
```

**Step 2 — Update `dim_client` (SCD Type 1 — current snapshot)**

```sql
MERGE `warehouse.dim_client` AS target
USING cdc_updates AS source
   ON target.client_id = source.client_id
 WHEN MATCHED THEN
UPDATE SET
    risk_category       = source.new_risk_category
  , account_balance_usd = source.new_balance
  , account_status      = source.new_status
```

**Step 3 — Close current open row in `dim_client_scd`**

```sql
UPDATE `warehouse.dim_client_scd`
   SET effective_to = source.commit_ts
     , is_current   = FALSE
 WHERE client_id = source.client_id
   AND is_current = TRUE
```

**Step 4 — Insert new open row in `dim_client_scd`**

```sql
INSERT INTO `warehouse.dim_client_scd`
     ( client_scd_key
     , client_id
     , risk_category
     , account_balance_usd
     , account_status
     , effective_from
     , effective_to
     , is_current
     )
VALUES
     ( GENERATE_UUID()
     , source.client_id
     , source.new_risk_category
     , source.new_balance
     , source.new_status
     , source.commit_ts
     , NULL
     , TRUE
     )
```

**Step 5 — Advance watermark**

```sql
UPDATE pipeline_metadata
   SET max_processed_lsn = <max lsn from this batch>
 WHERE source = 'cdc_client_profile'
```

Multiple events for the same `client_id` in one batch (CL001 has LSN 1004, 1005, 1006) are processed sequentially in LSN order — each iteration closes the row opened by the previous one.

---

### Q3 — How are delete events handled?

CDC event: LSN 1010 — CL012 (David Tan), op=delete.

`dim_client` — soft delete, row is never physically removed:

```sql
UPDATE `warehouse.dim_client`
   SET is_deleted     = TRUE
     , deleted_at     = TIMESTAMP '2024-11-21T14:00:00Z'
     , account_status = 'deleted'
 WHERE client_id = 'CL012'
```

`dim_client_scd` — close the current open row. No new row is inserted. Delete is the terminal state.

```sql
UPDATE `warehouse.dim_client_scd`
   SET effective_to = TIMESTAMP '2024-11-21T14:00:00Z'
     , is_current   = FALSE
 WHERE client_id  = 'CL012'
   AND is_current = TRUE
```

**Why no physical delete:** CL012 has DEP008 ($350) and trade history in `fct_trade`. Physical deletion orphans those records and breaks every historical P&L join. The soft delete preserves FK integrity while signalling to consumers that the client is no longer active. BI views add `WHERE NOT is_deleted` to exclude deleted clients from live reporting.

**GDPR note:** `is_deleted = TRUE` alone does not satisfy right-to-erasure. A separate GDPR anonymisation job, triggered by `deleted_at`, nulls out PII fields (`full_name`, `date_of_birth`, `nationality`) in `dim_client`. The `before` state from the CDC event is preserved in `landing.cdc_client_profile` as the permanent raw audit log.

---

### Q4 — CDC arrives in arrival order, not LSN order — how is correctness ensured?

The file `client_profile_changes.jsonl` arrives with LSNs: `1005, 1009, 1001, 1004, 1010, 1012, 1003, 1015, 1008, 1018, 1006, 1020`.

CL001 has three sequential events (LSN 1004 → 1005 → 1006) arriving in the file as 1005, 1004, 1006. Applying in arrival order would set `account_status = under_review` before the balance change — wrong interim state.

**Solution: accumulate then sort**

All events are INSERTed into `staging.cdc_client_profile` as-is (arrival order, immutable log). No sorting at load time.

Processing reads only events `lsn > watermark`, sorted by LSN ascending:

```sql
SELECT *
  FROM staging.cdc_client_profile
 WHERE lsn > (SELECT max_processed_lsn FROM pipeline_metadata
               WHERE source = 'cdc_client_profile')
 ORDER BY lsn ASC
```

Because the entire JSONL file is loaded into staging before processing begins, all three CL001 events (1004, 1005, 1006) are visible in the same staging query, sorted and applied in correct LSN order.

**Cross-batch gap detection:** If an event arrives in a later batch with `lsn < max_processed_lsn` (out-of-order across deliveries), the `cdc_before_state_mismatch` DQ check catches it — the `before` values in the late event will not match the current `dim_client` state. This fires a Warning alert and the event is flagged for manual review.

---

### Q5 — Reloading a historical date range without corrupting existing history

Scenario: re-process all CDC events for November 2024.

The immutable landing layer is the safety net. Raw events are always available in `landing.cdc_client_profile` and can be replayed at any time.

**Steps:**

1. Identify the affected LSN range — query staging for events with `DATE(commit_ts) BETWEEN '2024-11-01' AND '2024-11-30'`

2. Find the boundary in `dim_client_scd` — the last row closed before November and the first row opened in November. These rows define the "before" state to restore to.

3. Delete affected SCD rows:
```sql
DELETE FROM `warehouse.dim_client_scd`
 WHERE effective_from >= TIMESTAMP '2024-11-01'
   AND effective_from <  TIMESTAMP '2024-12-01'
```

4. For rows whose `effective_to` falls within November (closed during November), reopen them:
```sql
UPDATE `warehouse.dim_client_scd`
   SET effective_to = NULL
     , is_current   = TRUE
 WHERE effective_to >= TIMESTAMP '2024-11-01'
   AND effective_to <  TIMESTAMP '2024-12-01'
```

5. Delete November staging events and re-parse from landing:
```sql
DELETE FROM staging.cdc_client_profile
 WHERE DATE(commit_ts) BETWEEN '2024-11-01' AND '2024-11-30'
```

6. Re-insert from landing (`WHERE DATE(insert_timestamp) BETWEEN ...`), re-run staging parse

7. Reset watermark to the LSN just before November, re-run the CDC apply pipeline — events replay in LSN order and rebuild the correct SCD rows

**Why this is safe:** Landing is append-only and never modified. Steps 3–7 only touch staging and target, both of which are derived from landing. The raw truth is always available.

---

### Q6 — Source table with 250+ fields that changes frequently

Full SCD Type 2 on a 250-field table that changes frequently creates two problems:

1. **Storage explosion** — every field change creates a new full-width row. If 50 fields change daily across 2.4M clients, the SCD table doubles in months.
2. **Noise** — most field changes are operationally irrelevant to analytics. Storing a full snapshot for a phone number update alongside a risk_category change makes history hard to query and reason about.

**Pattern: Partial SCD2 — track only analytically significant fields**

This is exactly the approach taken in `dim_client_scd`:

- Only `risk_category`, `account_balance_usd`, `account_status` are historised in `dim_client_scd`
- These are the fields that change via CDC and have analytical/compliance value at a point in time
- All other fields (signup data, contact info, preferences) are SCD Type 1 in `dim_client` — current value only

For a 250-field table, the process is:
1. Classify fields: which ones need point-in-time history? (regulatory, financial, status fields)
2. Put those in the SCD2 history table (narrow — 5–10 fields max)
3. Put everything else in the SCD1 current table
4. The SCD2 table stays narrow and manageable regardless of how many other fields change

**Alternative for extreme cases — delta/change log pattern:**

Store only the diff per event rather than a full row snapshot:

```
client_id | changed_field        | old_value | new_value | effective_from
CL001     | risk_category        | medium    | high      | 2024-11-15 10:30
CL001     | account_balance_usd  | 1250.00   | 1850.00   | 2024-11-15 11:00
```

This is more storage-efficient but requires pivot logic to reconstruct state at a point in time — adds query complexity. Suitable when the number of fields that could change is very large and you cannot predict which ones matter.

---

## Part 2c — Analytical SQL: Deposit Count by Country

**Dialect: BigQuery standard SQL**

Requirements:
- Every country in `client_signup` must appear — including countries with zero deposits
- Sort: zero-deposit countries first, then remaining countries descending by deposit count

```sql
SELECT cs.country
     , COUNT(fd.deposit_id)  AS deposit_count
  FROM `warehouse.dim_client`   cs
  LEFT JOIN `warehouse.fct_deposit` fd
         ON fd.client_id = cs.client_id
 GROUP BY cs.country
 ORDER BY CASE WHEN COUNT(fd.deposit_id) = 0 THEN 0 ELSE 1 END ASC
        , COUNT(fd.deposit_id) DESC
        , cs.country           ASC
```

**Why LEFT JOIN, not INNER JOIN:** An INNER JOIN would silently drop countries whose clients have made no deposits. The LEFT JOIN preserves all countries from `dim_client` and returns `COUNT = 0` where no matching `fct_deposit` rows exist.

**Why `COUNT(fd.deposit_id)` not `COUNT(*)`:** `COUNT(*)` counts NULL rows (countries with no deposits) as 1. `COUNT(fd.deposit_id)` counts only non-null values — zero-deposit countries correctly return 0.

**Sort logic:** The `CASE` expression buckets zero-deposit countries (bucket 0) before non-zero countries (bucket 1). Within non-zero countries, `COUNT DESC` puts the highest-volume countries first. The `country ASC` tertiary sort gives deterministic ordering within tied counts. `ORDER BY deposit_count DESC` alone would not work — it would put zero-deposit countries last, not first.
