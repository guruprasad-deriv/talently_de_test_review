# Technical Assessment — Data Engineering

This repository contains the full response to the data engineering technical assessment for a financial trading platform. All answers are grounded in the specific data provided in the assessment (`scale_profile.md`, `raw_deposits.csv`, `client_cdc.jsonl`).

---

## Repository Structure

```
technical-test-2-autonomous/
├── part1_pipeline.md           Part 1 — Pipeline design + DQ suite
├── part2_data_model.md         Part 2 — Dimensional model, SCD2, SQL queries
├── part3_diagnosis.md          Part 3 — BigQuery SLA breach root cause + fix
├── part4_architecture.md       Part 4 — Real-time/batch architecture + build vs buy
├── sql/
│   └── query_a_deposit_count_by_country.sql
├── code/
│   ├── vendor_ingestion.py     Idempotent CSV ingestion with DQ checks
│   ├── cdc_processor.py        CDC JSONL processor with LSN ordering + SCD2
│   └── dq_checks.py            Standalone DQ check runner
├── PROMPTS.md                  AI prompts used per part
└── README.md                   This file
```

---

## Part Summaries

### Part 1 — Ingestion Pipeline & DQ Suite

Covers: vendor CSV deposit pipeline, CDC JSONL processor, idempotency strategy, late delivery handling, soft deletes, Airflow orchestration, and a 12-check DQ suite.

Key design decisions:
- **Idempotency**: file manifest `(filename, md5_content_hash)` + row-level MERGE on `deposit_id`
- **LSN ordering**: CDC events sorted by `lsn ASC` before application — file delivery order ≠ logical order
- **Soft delete**: CDC DELETE retains the row with `is_deleted=TRUE`; regulatory compliance (MiFID II / MAS) requires audit trail preservation
- **Late delivery**: uses `deposit_date` from file content (not filename); 7-day lookback limit; `late_delivery_flag` column

### Part 2 — Dimensional Model

Covers: Kimball star schema design, SCD Type 2 walk-throughs, historical reload procedure, BigQuery SQL queries.

Key design decisions:
- **Kimball over Data Vault**: known grain, small team, BI query patterns — Data Vault overhead not justified
- **SCD2 for** `risk_category`, `account_status` (compliance, point-in-time accuracy required)
- **SCD1 for** `account_balance_usd` (running balance is a measure, not a dim attribute; SCD2 would create snapshot churn)
- **Split SCD** for 250+ field tables: track only 2 compliance fields in SCD2; JSON column for static/rare fields; 98% storage reduction

### Part 3 — Scalability & Diagnosis

Covers: root cause analysis of 47-minute BigQuery job (SLA: 30 min), remediation plan with cost estimates.

Root causes (all grounded in `scale_profile.md`):
1. `fct_trade` not partitioned — 720 GB full scan every run
2. `trade_status='closed'` eliminates only 3% of rows post-scan
3. Gold instrument = 41% of rows — GROUP BY instrument final stage is skewed
4. Full 180-day historical rebuild every 3 hours

After fixes (partition + dbt incremental): **$36/day → $0.20/day** (~$1,074/month saving).

### Part 4 — Architecture

Covers: unified real-time + batch architecture, external partner API security, build vs buy for CSV payment processor.

Key decisions:
- **Kafka as integration hub**: deposit event written once, consumed at each system's own pace
- **Flink for real-time** fraud detection (<5s end-to-end): stateful keyed windows, Redis for client dim enrichment
- **BigQuery batch path unchanged**: consistency matters more than speed for finance reporting
- **External partner API**: REST over materialized view (30 min staleness acceptable); mTLS + OAuth 2.0 + row-level security
- **Build over buy**: CSV + undocumented schema drift + custom code mapping = build wins at one source

---

## Data Quality Issues Found in Sample Data

Issues identified directly in the provided `raw_deposits.csv` and `client_cdc.jsonl`:

| # | Issue | Source | Severity | Check ID |
|---|-------|--------|----------|----------|
| 1 | VDEP002/VDEP005: duplicate deposit_id across vendor and CDC files | raw_deposits.csv | Critical | VDEP002 |
| 2 | VDEP001: negative amount_usd in deposit records | raw_deposits.csv | Critical | VDEP001 |
| 3 | DEP020 references CL031 which is absent from dim_client | raw_deposits.csv | Critical | VDEP003 |
| 4 | CL099 appears in vendor file but not in dim_client | raw_deposits.csv | Critical | VDEP003 |
| 5 | TRD012: open_price=close_price=2320 but pnl_usd=245 — impossible arithmetic | scale_profile.md | Warning | TRD012 |
| 6 | TRD006: trade executed against inactive account | scale_profile.md | Warning | TRD006 |
| 7 | CL001 CDC stream has out-of-order LSN delivery | client_cdc.jsonl | Critical | CDC-LSN |
| 8 | Vendor file uses `method` field; subsequent file uses `payment_method` — schema drift | raw_deposits.csv | Warning | VDEP011 |

---

## Running the Prototype Code

No external dependencies required — all prototypes use Python stdlib only.

```bash
# DQ check runner
python code/dq_checks.py

# Idempotent vendor CSV ingestion
python code/vendor_ingestion.py

# CDC JSONL processor with LSN ordering and SCD2
python code/cdc_processor.py
```

Expected output for each script: PASS/FAIL DQ results, ingestion summary, and final warehouse state.

---

## SQL Query

`sql/query_a_deposit_count_by_country.sql` — deposit count by country using LEFT JOIN so zero-deposit countries appear with `deposit_count = 0`.
