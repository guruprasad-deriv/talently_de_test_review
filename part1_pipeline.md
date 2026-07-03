# Part 1a — Pipeline Design Document

## 1. Architecture Overview

### Source-to-Target Flow

```
                    ┌─────────────────────────────────────────┐
                    │              GCS Landing Bucket          │
                    │  gs://warehouse-landing/                 │
                    │    vendor-deposits/YYYY/MM/DD/           │
                    │    cdc-client-profile/YYYY/MM/DD/        │
                    └──────────────┬──────────────────────────┘
                                   │ GCS sensor (file + .done marker)
                                   ▼
                    ┌─────────────────────────────────────────┐
                    │           LANDING LAYER (BigQuery)       │
                    │                                          │
                    │  landing.vendor_deposits                 │
                    │    file_name, metadata_json, insert_ts   │
                    │    PARTITION BY DATE(insert_timestamp)   │
                    │                                          │
                    │  landing.cdc_client_profile              │
                    │    file_name, metadata_json, insert_ts   │
                    │    PARTITION BY DATE(insert_timestamp)   │
                    │                                          │
                    │  pipeline_manifest                       │
                    │    file_name, checksum, row_count,       │
                    │    loaded_at, status                     │
                    └──────────────┬──────────────────────────┘
                                   │ Staging DAG
                                   ▼
                    ┌─────────────────────────────────────────┐
                    │           STAGING LAYER (BigQuery)       │
                    │                                          │
                    │  staging.vendor_deposits                 │
                    │    - Parse metadata_json                 │
                    │    - Normalise schema drift              │
                    │      (method → payment_method)           │
                    │    - DQ fail → raw JSON to error table   │
                    │    - INSERT WHERE NOT EXISTS             │
                    │      (deposit_id + deposit_date prune)   │
                    │                                          │
                    │  staging.cdc_client_profile              │
                    │    - Parse metadata_json                 │
                    │    - Sort by LSN (critical)              │
                    │    - Validate LSN continuity             │
                    │    - Identify op type (insert/update/    │
                    │      delete)                             │
                    └──────────────┬──────────────────────────┘
                                   │ Target DAG
                                   ▼
                    ┌─────────────────────────────────────────┐
                    │            TARGET LAYER (BigQuery)       │
                    │                                          │
                    │  fct_deposit                             │
                    │    Pre-insert: reconcile VDEP### vs DEP##│
                    │    INSERT IF NOT EXISTS on deposit_id    │
                    │    Partition prune on deposit_date       │
                    │                                          │
                    │  dim_client (current snapshot)           │
                    │    MERGE ON client_id, apply CDC ops     │
                    │    Soft delete on op=delete              │
                    │                                          │
                    │  dim_client_scd (SCD Type 2 history)     │
                    │    Close current row on change/delete    │
                    │    Insert new row for each change        │
                    └─────────────────────────────────────────┘
```

### What Happens at Each Layer

**Landing layer** — immutable raw store. No parsing, no transformation, no column mapping. Every row from every file is stored as a single JSON blob (`metadata_json`) alongside `file_name` and `insert_timestamp`. Schema drift is fully absorbed here — the 20240302 `method` column rename, the varying CDC JSON shapes — all become different JSON structures in the same column. The layer never changes shape regardless of what the vendor sends.

**Staging layer** — parse, normalise, validate, INSERT-only. JSON is unpacked into typed columns. Schema drift is resolved by mapping rules (e.g. `method → payment_method`). DQ checks run here: range validation, FK checks against `dim_client`, allowlist checks on `payment_method`, `status`, `op`. Records that fail any DQ check are written as raw `metadata_json` directly to the error table from landing — they never enter staging. Staging receives only clean records. No mutations, no tagging, no updates — staging is INSERT-only. CDC events are sorted by LSN before any further processing.

**Target layer** — insert, historise, report. `fct_deposit` receives an INSERT IF NOT EXISTS keyed on `deposit_id`, with partition pruning on `deposit_date` so the existence check scans only the relevant partition rather than the full table. No UPDATEs — vendor deposit data has no mutations. `dim_client` receives CDC events applied in LSN order via MERGE. `dim_client_scd` receives SCD Type 2 rows opened and closed based on `commit_ts`.

---

## 2. Idempotency Strategy

Idempotency is enforced at two independent layers, each protecting against a different failure mode.

### Layer 1 — File Manifest (landing)

Before loading any file, the pipeline computes a SHA-256 checksum of the full file contents and checks the `pipeline_manifest` table.

```
pipeline_manifest
  file_name       STRING    NOT NULL
  checksum        STRING    NOT NULL   -- SHA-256 of file contents
  row_count       INT64
  loaded_at       TIMESTAMP
  status          STRING    -- 'loaded' | 'skipped' | 'reloaded'
```

**Decision logic on each run:**

| Manifest lookup result | Action |
|---|---|
| `file_name` not found | Load all rows → INSERT manifest record (`status=loaded`) |
| `file_name` found, checksum matches | Skip entirely — file unchanged (`status=skipped`) |
| `file_name` found, checksum differs | DELETE landing rows `WHERE file_name = X` → reload all rows → UPDATE manifest (`status=reloaded`) |

This handles the case where a vendor re-delivers a corrected file (e.g. VDEP001's negative amount is corrected in a re-send of `deposits_vendor_20240301.csv`). The entire file is replaced atomically.

**Partial upload protection:** The pipeline only triggers when both the data file and a co-located success marker (`deposits_vendor_YYYYMMDD.csv.done`) are present in GCS. A GCS sensor polls for both. This prevents computing a checksum on a mid-upload file.

### Layer 2 — INSERT IF NOT EXISTS (staging → target)

The file manifest operates at file level and cannot detect cross-file duplicates. `VDEP002` (CL001, 2024-03-01, $1,500) appears in both `deposits_vendor_20240301.csv` and `deposits_vendor_20240302.csv` as an exact duplicate. Both files have different checksums so both load into landing and staging. The INSERT IF NOT EXISTS at staging → target handles this:

```sql
INSERT INTO `warehouse.fct_deposit`
SELECT s.deposit_id
     , s.client_id
     , s.deposit_date
     , s.amount_usd
     , s.payment_method
     , s.currency_original
     , s.exchange_rate
     , s.status
     , s.processing_days
     , s.fee_usd
  FROM staging.vendor_deposits s
 WHERE NOT EXISTS (
         SELECT 1
           FROM `warehouse.fct_deposit` t
          WHERE t.deposit_id   = s.deposit_id
            AND t.deposit_date = s.deposit_date
       )
```

The `AND t.deposit_date = s.deposit_date` clause enables BigQuery partition pruning — the existence check scans only the matching date partition (~160 MB) rather than the full 28 GB table. Deposit data has no mutations (amounts, status, payment method are immutable once settled), so UPDATE is not needed. The second occurrence of VDEP002 finds the row already present and produces no insert — a true no-op.

For the CDC log, the LSN is the idempotency key. The pipeline tracks `max_processed_lsn` in a metadata table and only processes events with `lsn > max_processed_lsn` on each run.

### Vendor Deposit ID Strategy

The existing `fct_deposit` records (`DEP###`) were loaded from the previous payment processor. The new vendor uses its own native ID format (`VDEP###`). The pipeline loads `VDEP###` directly as `deposit_id` in `fct_deposit` — no internal sequence is generated. This preserves the vendor's native key for traceability and support queries.

**Pre-MERGE reconciliation check:** Before inserting new vendor records, staging runs a collision check to detect whether the new vendor is reporting a deposit that the old system already captured for the same `(client_id, deposit_date, amount_usd)`:

```sql
SELECT v.deposit_id         AS new_vendor_id
     , d.deposit_id         AS old_vendor_id
     , v.client_id
     , v.deposit_date
     , v.amount_usd
  FROM staging.vendor_deposits  v
  JOIN fct_deposit               d
    ON d.client_id    = v.client_id
   AND d.deposit_date = v.deposit_date
   AND d.amount_usd   = v.amount_usd
   AND d.deposit_id NOT LIKE 'VDEP%'
```

Any match = same underlying transaction captured by both vendors = double-payment risk. These records are quarantined and an alert fires. They are not auto-inserted. In the provided test data, no such collision exists, but the check runs on every batch.

---

## 3. Late and Missing Data

### Late Delivery

`deposits_vendor_20240303.csv` was delivered on 2024-03-03 but contains records with deposit dates ranging from 2024-02-24 to 2024-02-28 — up to 8 days prior to delivery.

The pipeline never uses file arrival date as the business date. All partitioning and logic in the target layer uses the `deposit_date` field from the source record. The `insert_timestamp` in landing records when the record was loaded — this is useful for audit and SLA monitoring but has no effect on how the record is stored in the target.

Because the target MERGE operates on `deposit_id` (not date ranges), a late record landing in landing on 2024-03-03 will be merged into `fct_deposit` correctly regardless of its `deposit_date`. No backfill job, no manual reconciliation, no special handling needed.

### Missing Files

The pipeline tracks expected files in a file schedule table (one row per expected filename per date). The Airflow DAG checks at end-of-window (e.g. 08:00 SGT, 2 hours after the expected drop time) whether each expected file is present in `pipeline_manifest` with `status != skipped`.

If a file is absent, the DAG raises an SLA alert to the data engineering Slack channel and continues without failing. When the file later arrives (same day or next day), the regular GCS sensor picks it up and processes it normally. No manual trigger is needed.

---

## 4. Source-Delete Handling

The CDC log contains a hard delete for CL012 (David Tan) at LSN 1010:

```json
{"lsn": 1010, "op": "delete", "client_id": "CL012",
 "before": {"full_name": "David Tan", "risk_category": "low",
             "account_balance_usd": 0.00, "account_status": "suspended"},
 "after": null}
```

CL012 has an existing deposit (DEP008, $350) and trade history in the warehouse. A physical delete from `dim_client` would orphan those records and break all joins downstream.

### Approach: Soft Delete on `dim_client`, Close Row on `dim_client_scd`

**`dim_client` (current snapshot):**

```sql
UPDATE `warehouse.dim_client`
   SET is_deleted   = TRUE
     , deleted_at   = TIMESTAMP '2024-11-21T14:00:00Z'   -- from CDC commit_ts
     , account_status = 'deleted'
 WHERE client_id = 'CL012'
```

The row remains. All FK relationships from `fct_trade` and `fct_deposit` remain valid. BI consumers must add `WHERE NOT is_deleted` to exclude deleted clients — this is enforced at the view layer.

**`dim_client_scd` (SCD Type 2 history):**

The current open row for CL012 is closed:

```sql
UPDATE `warehouse.dim_client_scd`
   SET effective_to   = TIMESTAMP '2024-11-21T14:00:00Z'
     , is_current     = FALSE
 WHERE client_id  = 'CL012'
   AND is_current = TRUE
```

No new row is opened. The delete event is the terminal state.

**Trade-offs:**

| Consideration | Impact |
|---|---|
| FK integrity preserved | `fct_trade`, `fct_deposit` rows for CL012 remain queryable for historical P&L |
| Consumers must filter | Any aggregate that forgets `WHERE NOT is_deleted` will include deleted clients — mitigated by always exposing `dim_client` through a view that applies the filter |
| GDPR right-to-erasure | Soft delete flag alone is not sufficient. PII fields (`full_name`, `date_of_birth`, `nationality`) must be anonymised (nulled or replaced with a token) in a separate GDPR erasure job. The `deleted_at` timestamp triggers this job. |
| Audit trail | `before` state from the CDC record is preserved in `landing.cdc_client_profile` as the permanent raw log |

---

## 5. Orchestration and Scheduling

**Tool: Cloud Composer (managed Airflow) on GCP**

Chosen because the warehouse is BigQuery on GCP, the files land in GCS, and Cloud Composer provides native operators for both (`GCSObjectExistsOperator`, `BigQueryInsertJobOperator`). The DAG definition lives in version control alongside the pipeline code.

### DAG Structure

```
vendor_deposit_pipeline (daily, triggers on file + .done marker)
  ├── check_manifest (GCSObjectExistsOperator — data file + .done marker)
  ├── compute_checksum (PythonOperator)
  ├── manifest_lookup (BigQueryOperator — check pipeline_manifest)
  ├── land_raw (BigQueryInsertJobOperator — INSERT into landing)
  ├── update_manifest (BigQueryOperator)
  ├── stage_and_validate (BigQueryOperator — parse, normalise, DQ)
  ├── quarantine_failures (BigQueryOperator — write dq_quarantine)
  └── merge_to_target (BigQueryOperator — MERGE fct_deposit)

cdc_client_profile_pipeline (triggers on file arrival)
  ├── land_raw
  ├── sort_by_lsn (staging sort — critical)
  ├── apply_cdc_to_dim_client (MERGE — inserts, updates, soft deletes)
  └── apply_cdc_to_dim_client_scd (SCD2 open/close)

file_sla_monitor (daily, runs at 08:00 SGT)
  └── check_expected_files (alert to Slack if any expected file absent)
```

**Why not Cloud Scheduler alone:** Cloud Scheduler can trigger a Cloud Run job but provides no retry logic, dependency tracking, or DAG-level observability. Airflow provides task-level retries, SLA miss callbacks, and a UI for debugging failed runs — essential for a financial data pipeline where missed SLAs have downstream reporting impact.

---

## 6. Edge Cases

### 6.1 Schema Drift — Column Rename Across Files

**Observed:** `deposits_vendor_20240302.csv` uses `method` instead of `payment_method`. The 20240303 file reverts to `payment_method`.

**Handling:** Because landing stores raw JSON blobs, both column names land without issue. Staging applies a normalisation rule:

```python
raw["payment_method"] = raw.get("payment_method") or raw.get("method")
```

A schema drift alert fires when staging detects a column name that differs from the expected schema, so the DE team is notified even though the pipeline continues without failing.

### 6.2 Cross-File Exact Duplicates

**Observed:** `VDEP002` (CL001, $1,500) and `VDEP005` (CL005, $875) appear identically in both 20240301 and 20240302 vendor files.

**Handling:** Both records land in `landing.vendor_deposits` under different `file_name` values. The `MERGE ON deposit_id` at staging → target produces a no-op UPDATE for the second occurrence (row_hash matches). No duplicate rows are created in `fct_deposit`.

### 6.3 Negative Deposit Amount

**Observed:** `VDEP001` (CL003) has `amount_usd = -250.00`.

**Handling:** Staging DQ check: `amount_usd > 0` is a hard constraint. The record is written to `dq_quarantine` with `dq_rule = 'negative_amount'` and excluded from the target MERGE. An alert fires. The record is not silently dropped — it stays in landing and quarantine for investigation.

### 6.4 Out-of-Order LSN in CDC

**Observed:** `client_profile_changes.jsonl` arrives with LSNs in file order `1005, 1009, 1001, 1004, 1010, ...`. CL001 has three sequential events at LSNs 1004 → 1005 → 1006 that arrive as `1005, 1004, 1006`. Applying in arrival order would apply the balance change before the risk change — producing a wrong interim state.

**Handling:** Staging always sorts by LSN before applying CDC events:

```sql
SELECT *
  FROM staging.cdc_client_profile
 ORDER BY lsn ASC
```

Events for the same `client_id` are applied sequentially within a single staging run. The `before` state in each event is used to validate that the expected prior state matches what is in the target — a mismatch indicates a gap in the CDC feed.

### 6.5 CDC Delete for Client With Downstream FK References

**Observed:** CL012 (David Tan) is deleted via CDC at LSN 1010. CL012 has deposit DEP008 and a suspended profile in the warehouse. Physical deletion would orphan all downstream records.

**Handling:** Soft delete as described in Section 4. The `before` state from the CDC record (`{"account_status": "suspended", "account_balance_usd": 0.00}`) is preserved in landing as the permanent audit record of the client's final known state before deletion.

---

## Part 1b — Data Quality Check Suite

All checks run at the staging layer before any record enters `staging.vendor_deposits` or `staging.cdc_client_profile`. Records failing a Critical check are written as raw `metadata_json` to the error table and excluded from staging. Records failing a Warning check proceed to staging with a flag logged to the pipeline manifest.

| Check name | Source file(s) | Field(s) checked | Failure mode | Severity | On-failure action |
|---|---|---|---|---|---|
| `required_fields_not_null` | All vendor CSVs | `deposit_id`, `client_id`, `deposit_date`, `amount_usd` | Any required field is null | Critical | Quarantine row as raw JSON to error table; continue processing remaining rows in file |
| `amount_usd_positive` | All vendor CSVs | `amount_usd` | Value ≤ 0 — observed: VDEP001 = -250.00 (CL003) | Critical | Quarantine row to error table with `dq_rule=negative_amount`; fire alert to #data-eng Slack |
| `valid_payment_method_allowlist` | All vendor CSVs | `payment_method` (post-normalisation) | Value not in `{bank_transfer, credit_card, e_wallet}` | Critical | Quarantine row to error table with `dq_rule=invalid_payment_method`; continue remaining rows |
| `expected_columns_present` | All vendor CSVs | All header columns | `payment_method` renamed to `method` in 20240302 — schema drift | Warning | Apply normalisation mapping (`method → payment_method`); fire schema drift alert to #data-eng; pipeline continues |
| `deposit_id_unique_within_file` | Each vendor CSV (per-file) | `deposit_id` | Same `deposit_id` appears more than once within a single file | Critical | Block entire file load (not just the duplicate rows); update manifest `status=blocked`; alert to #data-eng |
| `cross_file_duplicate_count` | Vendor CSVs + `staging.vendor_deposits` | `deposit_id` | `deposit_id` already present in staging from a prior file — observed: VDEP002, VDEP005 in both 20240301 and 20240302 | Warning | Log duplicate count to `pipeline_manifest`; INSERT IF NOT EXISTS at target handles deduplication; no record blocked |
| `client_id_not_in_client_signup` | All vendor CSVs | `client_id` → `client_signup.client_id` | `client_id` does not exist in `client_signup` at all — genuine orphan; observed: VDEP020 references CL099 (never registered) | Critical | Quarantine row to error table with `dq_rule=orphan_client`; data is invalid, do not insert; alert to #data-ops |
| `client_id_in_signup_not_yet_in_dim_client` | All vendor CSVs | `client_id` → `client_signup` present, `dim_client` absent | `client_id` exists in `client_signup` but the corresponding `dim_client` row has not yet been loaded — pipeline timing gap, not bad data | Warning | Insert into `fct_deposit` anyway; FK will resolve when `dim_client` next loads; alert to #data-ops (informational) |
| `kyc_status_deposit_guard` | All vendor CSVs + `dim_client` | `client_id` → `kyc_status` | Client has `kyc_status = rejected` — observed: VDEP004 for CL012 (rejected KYC, regulatory risk) | Critical | Quarantine row to error table; fire compliance alert to #risk-and-compliance (separate from #data-eng); do not auto-insert |
| `vendor_old_vendor_collision` | All vendor CSVs + `fct_deposit` | `client_id`, `deposit_date`, `amount_usd` | Same `(client_id, deposit_date, amount_usd)` exists in `fct_deposit` under a DEP### ID — double-payment risk | Critical | Quarantine row to error table with `dq_rule=double_payment_risk`; fire payment reconciliation alert to #finance-ops; block insert |
| `late_delivery_detection` | All vendor CSVs | `deposit_date` vs `insert_timestamp` | All records in 20240303 have `deposit_date` 5–7 days before file delivery — late delivery confirmed | Warning | Log delivery gap to SLA monitoring table; pipeline processes by business `deposit_date` (no action needed); alert to #data-eng if gap > 3 days |
| `valid_cdc_op_type` | `client_profile_changes.jsonl` | `op` | Value not in `{insert, update, delete}` | Critical | Halt entire CDC batch; do not process any events from this file; alert to #data-eng |
| `cdc_before_state_mismatch` | `client_profile_changes.jsonl` + `dim_client` | `before.risk_category`, `before.account_balance_usd`, `before.account_status` vs `dim_client` current values | `before` state in CDC event does not match current `dim_client` row — indicates missed or out-of-order CDC events | Warning | Log mismatch to `pipeline_metadata` with affected `client_id` and `lsn`; continue processing (`after` values are authoritative); alert to #data-eng for gap investigation |
