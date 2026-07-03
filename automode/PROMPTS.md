# AI Prompts Used — Technical Assessment

This document records the substantive prompts issued during this assessment, what was decided or changed as a result, and where the output appears.

---

## Part 1 — Pipeline Design & DQ Suite

### Prompt 1.1 — Idempotency Strategy

> "What is the correct idempotency strategy for a CSV deposit pipeline where the same file may be re-delivered with the same filename but potentially different content? And separately, what handles the case where two files contain the same deposit_id for the same event?"

**Decision reached**: Two-layer idempotency. Layer 1: file manifest keyed on `(filename, md5_content_hash)` — same file re-delivered with same content is skipped; same filename with different content is a new entry. Layer 2: row-level MERGE on `deposit_id` (natural business key) handles cross-file deduplication.

**Why it matters**: A naive check on filename alone would skip re-delivered files with corrected data. A naive check on content hash alone would accept the same deposit_id arriving in two different files.

**Output location**: `part1_pipeline.md` §Idempotency; `code/vendor_ingestion.py` `is_already_processed()` + `merge_row()`

---

### Prompt 1.2 — LSN Ordering Invariant

> "The CDC JSONL file is delivered as a batch. The events are in delivery order (network flush order), not LSN order. What happens if we apply them in arrival order? Show the corrupted state vs correct state for CL001."

**Decision reached**: Sort by `lsn ASC` before applying any events. The CL001 example from the sample data has LSN 1006 (account_status → under_review) arriving before LSN 1005 (balance update). Applying 1006 first sets balance correctly from 1005's value; but if 1005 then overwrites the balance post-1006, the balance is wrong because it arrived out of order. The correct state chain is: balance 1600 → balance 1850 (LSN 1005, SCD1) → under_review (LSN 1006, SCD2).

**Output location**: `part1_pipeline.md` §Edge Cases; `part2_data_model.md` §CL001 LSN Ordering; `code/cdc_processor.py` LSN sort comment

---

### Prompt 1.3 — Soft Delete vs Hard Delete

> "The CDC stream contains a DELETE event for CL012. Should we hard-delete the row from the warehouse? What regulatory constraint applies?"

**Decision reached**: Soft delete only. SET `is_deleted=TRUE`, `effective_to=commit_ts`. Never physically remove the row. Reason: MiFID II (EU) and MAS Notice SFA04-N02 (Singapore) require complete audit trails for all client records and transactions. GDPR erasure is handled separately as PII anonymisation (replace PII fields with pseudonym), not as a row delete.

**Output location**: `part1_pipeline.md` §Soft Deletes; `part2_data_model.md` §CL012 Delete; `code/cdc_processor.py` `apply_delete()`

---

### Prompt 1.4 — DQ Check Severity Differentiation

> "What is the right severity model for DQ checks in a financial trading pipeline? Should all failures block the load, or only some?"

**Decision reached**: Three-tier model — Critical (block load, alert on-call), Warning (load proceeds, flag rows, alert data team), Info (log only, no action). Critical examples: negative amounts, orphan client refs, duplicate deposit_ids, null required fields, LSN ordering violations. Warning: schema drift, fee/amount ratio anomalies, CDC delete integrity. Info: late delivery, impossible DOB.

Rationale: blocking all loads on any DQ failure causes operational disruption for minor issues. A Warning-level schema drift (new optional column) should not halt a pipeline serving a C-suite report.

**Output location**: `part1_pipeline.md` §DQ Suite (12-check table with severity column)

---

## Part 2 — Dimensional Model

### Prompt 2.1 — Kimball vs Data Vault

> "For a team of 3 data engineers, known grain (one row per deposit, one row per trade), BI-serving fact tables, and no requirement for raw audit replay — is Kimball or Data Vault the right choice? What's the tipping point?"

**Decision reached**: Kimball. Data Vault's raw vault + business vault + information mart pattern adds two extra layers of complexity. The tipping point for Data Vault is: (a) multiple conflicting source systems for the same entity (here: one source per entity), (b) need to replay raw history independently of business rules (not stated), (c) team large enough to maintain hub/link/satellite separation. None apply here.

**Output location**: `part2_data_model.md` §Model Choice

---

### Prompt 2.2 — SCD Type for account_balance_usd

> "CL014 receives a balance update via CDC. Should account_balance_usd trigger SCD2 (new row) or SCD1 (in-place update)? What's the analytical impact of each?"

**Decision reached**: SCD1 (in-place update). Balance is a running operational value — the current balance is what analysts query. Storing every intermediate balance as a SCD2 row would generate millions of rows per active client per year, making point-in-time balance reconstruction possible but querying current balance expensive. The correct pattern: current balance in `dim_client` (SCD1); if full balance history is needed, build a separate `fct_balance_snapshot` fact table on a daily or hourly cadence.

Contrast: `risk_category` and `account_status` DO trigger SCD2 because compliance requires knowing what category a client was in at the time of a specific trade — this is a legal requirement, not an analytical preference.

**Output location**: `part2_data_model.md` §SCD Type Decisions; `code/cdc_processor.py` `SCD2_TRACKED_FIELDS` constant

---

### Prompt 2.3 — Query A: Zero-Deposit Countries

> "Write a query that returns deposit count by country. The result must include countries with zero deposits. What join type is required, and what's the risk in COUNT(*) vs COUNT(column)?"

**Decision reached**: LEFT JOIN from `dim_client` to `fct_deposit`. COUNT(`d.deposit_id`) not COUNT(*) — COUNT(*) counts the NULL join row for a zero-deposit country as 1, giving incorrect count of 1 instead of 0. ORDER BY `deposit_count ASC` puts zero-deposit countries first as requested.

**Output location**: `part2_data_model.md` §Query A; `sql/query_a_deposit_count_by_country.sql`

---

### Prompt 2.4 — Historical Reload Safety

> "A vendor re-delivers the November 2024 deposit file. The records have changed. How do you safely reload just that month without corrupting adjacent months or violating the SCD2 chain?"

**Decision reached**: Atomic reload procedure — (1) backup `fct_deposit` WHERE deposit_date IN November, (2) DELETE November rows from `fct_deposit` (hard delete, since this is a controlled reload not a CDC event), (3) reset SCD2 `effective_to` for any dim rows that changed during November, (4) re-run the ingestion pipeline for November, (5) validate row counts and sum(amount_usd) match redelivered file. All steps in a single transaction; rollback to backup if validation fails.

**Output location**: `part2_data_model.md` §Historical Reload Procedure

---

## Part 3 — Scalability & Diagnosis

### Prompt 3.1 — Root Cause Identification from Evidence

> "The query takes 47 minutes vs 30-minute SLA. Before recommending fixes, what should I look at in BigQuery's observability surfaces to confirm the root cause? List the specific INFORMATION_SCHEMA queries."

**Decision reached**: Three diagnostic queries — (1) `INFORMATION_SCHEMA.JOBS_BY_PROJECT` for job history and bytes processed, (2) `INFORMATION_SCHEMA.PARTITIONS` to confirm no partition exists on `fct_trade`, (3) `JOBS_TIMELINE_BY_PROJECT` for slot utilisation timeline to distinguish compute-bound vs shuffle-wait vs skew stall. Plus: Stages tab in BigQuery UI for unequal input rows per worker (skew signal).

**Output location**: `part3_diagnosis.md` §Step 1

---

### Prompt 3.2 — Partition Key Selection

> "Given fct_trade has trade_date, instrument, direction, trade_status as candidate partition keys — which should we partition on, and why not the others?"

**Decision reached**: Partition by `DATE_TRUNC(trade_date, MONTH)`. Cluster by `(instrument, direction)`.

Rejected keys:
- `instrument` — Gold at 41% creates a catastrophically skewed partition (0.66 TB vs ~0.16 TB average). BigQuery has a 4,000 partition limit and will not prune unequal partitions efficiently.
- `direction` — only 2 values (BUY/SELL); a partition on a 2-value column provides almost no pruning benefit.
- `trade_status` — only a few distinct values, does not correlate with query filter patterns; the existing `trade_status='closed'` filter eliminates only 3% of rows.

**Output location**: `part3_diagnosis.md` §Fix 1

---

### Prompt 3.3 — Cost Calculation

> "Calculate the monthly cost of the current agg_monthly_pnl_by_instrument job, and the projected cost after the partitioning + incremental dbt fixes. Show working."

**Decision reached**: Current: 8 runs/day × 720 GB × $6.25/TB = $36/day = ~$1,080/month. After Fix 1+2: 8 runs/day × 4 GB × $6.25/TB = $0.20/day = ~$6/month. Saving: ~$1,074/month for a single aggregation job.

**Output location**: `part3_diagnosis.md` §Fix 2 (cost table)

---

## Part 4 — Architecture

### Prompt 4.1 — Decoupling Real-Time and Batch

> "A fraud signal must fire within 5 seconds of deposit, but the batch pipeline must remain consistent for the C-suite report. These two requirements are fundamentally incompatible in a single pipeline. How do you satisfy both?"

**Decision reached**: Kafka as integration hub. The deposit event is published once to `deposits.raw`. Flink consumes for fraud detection (<5s). Dataflow/Beam consumes for batch write to GCS → BigQuery (minutes to hours). Each consumer runs at its own pace; a slow batch consumer does not block the real-time path. The key is that they are independent consumer groups — not sequential stages.

**Output location**: `part4_architecture.md` §Architecture Diagram; §Real-Time Path

---

### Prompt 4.2 — Build vs Buy Decision

> "The new payment processor delivers CSV over SFTP with undocumented schema and custom transaction codes. Evaluate build vs buy. What's the decisive factor?"

**Decision reached**: Build. The decisive factor is the combination of (a) undocumented schema that changes without notice and (b) custom code mapping that must be maintained regardless of tooling choice. Off-shelf ETL tools (Fivetran, Airbyte) handle stable, documented schemas well. They do not eliminate the code mapping work — they just move it to a config file inside the tool's UI. At one source with pathological schema instability, a custom Python parser with a schema version registry and a metadata-driven code mapping table provides more transparency and control at lower total cost.

**Output location**: `part4_architecture.md` §4b Build vs Buy

---

### Prompt 4.3 — External Partner API Security Model

> "An external partner needs to consume deposit data from the warehouse. What security controls are required, and why not give them direct BigQuery table access?"

**Decision reached**: REST API over materialized view (not direct table access). Direct BigQuery access would require IAM roles that can't be scoped below dataset level — the partner would see all clients, not just their allowed subset. A REST API adds: mTLS per-partner authentication, OAuth 2.0 scopes that limit response to allowed country/instrument, rate limiting, row-level security (BigQuery row access policies), audit logging of every request, and data minimisation (hashed client pseudonym instead of raw client_id).

30-minute staleness from the materialized view is acceptable because partner use case is batch reconciliation, not real-time monitoring.

**Output location**: `part4_architecture.md` §External Partner API
