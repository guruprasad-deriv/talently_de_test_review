# DE BUILD Assessment — [Candidate Name]

## Overview

This repository contains a production-grade data engineering solution for a financial trading platform. The platform ingests deposits from a new payment processor vendor (3 daily CSV files with intentional anomalies), processes a CDC stream of client profile changes (JSONL), and joins against 4 existing warehouse tables (`client_signup`, `client_profile`, `client_deposit`, `client_trades`). The solution covers pipeline design, data quality, dimensional modelling, scalability diagnosis, and real-time fraud architecture.

## Repo Structure

```
├── README.md
├── part1_pipeline.md       — Pipeline design + DQ check suite (Part 1a + 1b)
├── part2_data_model.md     — Kimball star schema, SCD2 historization, analytical SQL (Part 2)
├── part3_diagnosis.md      — Scalability diagnosis: 5 root causes for 47-min SLA breach (Part 3)
├── part4_architecture.md   — Real-time fraud architecture + Build vs Buy (Part 4)
├── sql/
│   └── query_a_deposit_count_by_country.sql
├── code/
│   └── pipeline.py         — Runnable idempotent prototype
└── PROMPTS.md              — All AI prompts used, grouped by section
```

## Key Design Decisions

- Landing layer stores raw JSON blobs (`metadata_json`) to absorb schema drift — the 20240302 `method` → `payment_method` column rename lands without pipeline change
- File-level idempotency via SHA-256 checksum: unchanged files are skipped; re-delivered files trigger full reload of that file's rows only
- INSERT IF NOT EXISTS (not MERGE) for the append-only deposit ledger; `deposit_date` in the EXISTS clause enables BigQuery partition pruning to avoid full-table scans
- CDC events accumulated in arrival order; always sorted by LSN before apply — arrival order would produce wrong interim client states
- SCD Type 2 partial — only the 3 CDC-tracked fields (`risk_category`, `account_balance_usd`, `account_status`) are historised, not all 30 columns
- `fct_trade` partitioned by `trade_date` (DAY): reduces the 720 GB full scan in `agg_monthly_pnl_by_instrument` to ~120 GB for 30-day incremental runs
- Dual-path lambda: streaming Flink for fraud detection (< 5 s latency) sharing a Kafka backbone with the existing batch path — batch pipeline untouched
- Airbyte self-hosted recommended for the new payment processor connector: open-source connector ecosystem avoids vendor lock-in and keeps the connector definition in version control
- Soft delete on `dim_client` for CDC deletes preserves FK integrity across `fct_deposit` and `fct_trade`; GDPR erasure of PII fields triggered separately by `deleted_at`
- Kimball star schema over Data Vault: four stable sources with analytics-oriented consumers do not justify hub/satellite overhead; raw landing layer already provides the auditability Data Vault is designed for

## Running the Prototype

```bash
cd code/
python pipeline.py
```

## Assessment Parts

| Part | File | Covers |
|---|---|---|
| 1a | `part1_pipeline.md` | 3-layer architecture (landing → staging → target), idempotency, late data, CDC deletes, orchestration |
| 1b | `part1_pipeline.md` | 12-check DQ suite with severity, quarantine strategy, and alert routing |
| 2 | `part2_data_model.md` | Kimball ERD, SCD Type 2 historization, 3 analytical SQL queries |
| 3 | `part3_diagnosis.md` | 5 root causes for the 47-min SLA breach with concrete BigQuery fixes |
| 4 | `part4_architecture.md` | Lambda-style real-time fraud architecture, Airbyte Build vs Buy analysis |
