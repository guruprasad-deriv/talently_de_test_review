# Part 4 — Architecture

> All design decisions in this document are grounded in the specific constraints stated in the brief: 9 billion trades, 2.4 million clients, sub-5-second fraud signal requirement, existing BigQuery batch pipeline, external partner API consumption, and CSV-delivery payment processor onboarding.

---

## 4a. Unified Real-Time and Batch Architecture

### The Core Problem

Two consumers with incompatible latency requirements must read from the same source of truth:

| Consumer | Latency | Consistency | Delivery |
|----------|---------|-------------|----------|
| Fraud detection | < 5 seconds | Best-effort (miss some = acceptable; false negative is better than blocking all deposits) | Per-event stream |
| C-suite weekly report | Hours | Exact (reconciled against GL) | Batch aggregate |
| External partner API | Minutes to hours | Eventual | Pull or webhook |

A single pipeline cannot satisfy all three. The unified architecture uses **Kafka as the integration hub** — the deposit event is written once and consumed independently by each downstream system at its own pace.

---

### Architecture Diagram

```
                         ┌─────────────────────────────────────────────────────┐
                         │                  INGEST LAYER                       │
                         │                                                     │
  Vendor CSV (T+1)  ────►│  Cloud Storage (GCS)                               │
  CDC JSONL (RT)   ────►│  Pub/Sub → Cloud Function (normalise + validate)    │
                         └──────────────────┬──────────────────────────────────┘
                                            │ normalised deposit event
                                            ▼
                         ┌─────────────────────────────────────────────────────┐
                         │              KAFKA (Integration Hub)                │
                         │                                                     │
                         │  Topic: deposits.raw     (raw normalised events)   │
                         │  Topic: deposits.enriched (with client dim data)   │
                         │  Topic: fraud.signals    (fraud model output)      │
                         │  Topic: deposits.dlq     (dead-letter queue)       │
                         └──────┬───────────────────────┬────────────────────┘
                                │                       │
               ┌────────────────▼───────┐   ┌──────────▼──────────────────────┐
               │  REAL-TIME PATH        │   │  BATCH PATH                     │
               │  (Flink / Dataflow)    │   │  (BigQuery via Dataflow/dbt)    │
               │                        │   │                                 │
               │  • Stateful windowing  │   │  • GCS → BigQuery load job      │
               │  • Fraud scoring       │   │  • dbt incremental models       │
               │  • Velocity checks     │   │  • fct_deposit, fct_trade       │
               │  • < 5s end-to-end     │   │  • agg_monthly_pnl refresh      │
               └────────────┬──────────┘   └───────────────┬─────────────────┘
                            │                               │
               ┌────────────▼──────────┐   ┌───────────────▼─────────────────┐
               │  FRAUD ACTION LAYER   │   │  SERVING LAYER                  │
               │                        │   │                                 │
               │  • Block / flag API    │   │  • BigQuery (C-suite reports)   │
               │  • Case management DB  │   │  • Looker / BI dashboard        │
               │  • Slack alert         │   │  • External partner API (REST)  │
               └───────────────────────┘   └─────────────────────────────────┘
```

---

### Real-Time Path: Fraud Detection Pipeline

#### Event Flow (end-to-end < 5 seconds)

```
Deposit event ingested
        │  t = 0ms
        ▼
Pub/Sub topic (deposits.raw)
        │  t ≈ 50ms
        ▼
Flink job: enrich + score
  - Look up dim_client from Redis cache (< 10ms read)
  - Apply velocity rules (sliding 1-hour window):
      • > 3 deposits in 1 hour from same client → flag
      • Deposit amount > 10× client 90-day average → flag
      • New country IP + amount > $5,000 → flag
  - Emit scored event to deposits.enriched
        │  t ≈ 200ms–1,500ms (Flink processing)
        ▼
Fraud signal to fraud.signals topic
        │  t ≈ 50ms
        ▼
Fraud action consumer:
  - Score < threshold → pass through
  - Score ≥ threshold → POST to payment processing API to hold deposit
  - Write fraud case to operational DB (Postgres)
  - Slack alert to compliance team
        │  t ≈ total < 5,000ms ✓
```

#### Flink Job Structure

```python
# Conceptual — Flink PyFlink or Java DataStream API
env = StreamExecutionEnvironment.get_execution_environment()

deposit_stream = (
    env
    .add_source(KafkaSource("deposits.raw"))
    .map(parse_deposit_event)
    .map(enrich_with_client_dim)          # Redis lookup for client risk_category
    .key_by(lambda e: e.client_id)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
    .process(VelocityFraudDetector())     # stateful: counts deposits, max amount
    .map(score_fraud_signal)
)

deposit_stream
    .filter(lambda e: e.fraud_score >= FRAUD_THRESHOLD)
    .add_sink(KafkaSink("fraud.signals"))

deposit_stream
    .add_sink(KafkaSink("deposits.enriched"))   # all events, enriched
```

#### State Management

The Flink job maintains **keyed state** per `client_id`:
- `DepositCountState`: count of deposits in sliding 1-hour window
- `MaxAmountState`: running 90-day average deposit amount (updated daily from BigQuery)
- `LastCountryState`: last known country code (to detect country change)

State is checkpointed to GCS every 60 seconds. On restart, Flink replays from the last Kafka offset and restores keyed state from the checkpoint. No deposits are double-processed; no state is lost.

---

### Batch Path: Existing BigQuery Analytics

The batch path is unchanged from the current architecture, but now reads from Kafka rather than directly from vendor file drops:

```
deposits.enriched topic
        │
        ▼
Dataflow (Beam) streaming job
  - Reads from deposits.enriched
  - Writes to GCS in Parquet (micro-batch every 5 min)
        │
        ▼
Airflow DAG (every hour)
  - GCSObjectExistenceSensor detects new Parquet files
  - BigQuery load job: GCS → fct_deposit staging
  - dbt run: merge staging → fct_deposit production
  - dbt run: agg_monthly_pnl_by_instrument (incremental)
        │
        ▼
C-suite weekly report via Looker
```

The batch path consumes from `deposits.enriched` (already validated and enriched) — it does not re-do the enrichment work that Flink already completed for the real-time path. This is the key efficiency gain from the Kafka hub model.

---

### External Partner API

#### Access Model

The external partner receives a **read-only REST API** backed by BigQuery, not direct table access.

```
Partner request
      │
      ▼
API Gateway (Cloud Endpoints / Apigee)
  - mTLS authentication (client certificate pinned per partner)
  - OAuth 2.0 token scoped to partner's data subset
  - Rate limiting: 1,000 req/min per partner
      │
      ▼
Cloud Run API service
  - Validates token scope → maps to allowed `country` or `instrument` filter
  - Queries BigQuery read replica (materialized view, refreshed every 30 min)
  - Returns JSON with cursor-based pagination
      │
      ▼
Partner system
```

#### Security Controls

| Control | Implementation |
|---------|---------------|
| Authentication | mTLS (mutual TLS) — partner presents client certificate pinned to their org |
| Authorisation | OAuth 2.0 token scopes: `deposits:read:country:MY` limits response to Malaysia only |
| Encryption in transit | TLS 1.3; HTTP/2; no plain HTTP endpoints |
| Encryption at rest | BigQuery default CMEK for sensitive columns (`client_id`, `amount_usd`) |
| Data minimisation | API never returns raw `client_id` — returns hashed partner-specific pseudonym |
| Audit logging | Every API call logged to Cloud Audit Log: `who`, `what endpoint`, `when`, `row count returned` |
| Row-level security | BigQuery row access policies: partner A can only see their own instrument set |

#### Latency vs Consistency Trade-off

The external partner API reads from a **materialized view refreshed every 30 minutes**, not the live `fct_deposit` table. This is a deliberate trade-off:

- **Consistency loss**: Partner sees data that is up to 30 minutes behind the live warehouse.
- **Latency gain**: Query against the materialized view (~18,000 rows for a typical partner scope) returns in milliseconds. A direct query against `fct_deposit` (full 720 GB if unpartitioned, or ~24 GB with date filter after partitioning fix) would add 2–15 seconds per API call, making interactive use impractical.
- **Why 30 minutes is acceptable**: Partners use this API to reconcile their own records against the warehouse — a daily or hourly batch workflow. Sub-minute freshness is not a stated requirement for external partners; it is only required for the internal fraud signal.

If a partner requires near-real-time data, the correct product is a **Kafka consumer group grant** (read-only topic access), not a REST API.

---

### Latency vs Consistency: Decision Matrix

| Scenario | Acceptable staleness | Architecture choice | Why |
|----------|---------------------|---------------------|-----|
| Fraud detection | < 5 seconds | Flink streaming on Kafka | Blocking a deposit 30 min later is useless |
| BI dashboard (C-suite) | Minutes to hours | dbt incremental on BigQuery | Consistency matters more than speed for finance reports |
| External partner reconciliation | Up to 30 min | Materialized view + REST API | Partners reconcile in batch; freshness < isolation |
| Compliance snapshot | Same day | dbt daily full refresh of dim_client_scd | Regulatory: point-in-time accuracy > speed |

---

## 4b. Build vs Buy: New Payment Processor Onboarding

### The Stated Scenario

> *"The new processor delivers a daily CSV drop to an SFTP server. The schema is undocumented and changes without notice. Transaction types use processor-specific codes that must be mapped to your internal taxonomy."*

### Decision Framework

| Factor | Build | Buy (off-shelf ETL tool) |
|--------|-------|--------------------------|
| Schema drift handling | Custom parser: detect field changes, emit alert, apply fallback mapping | Schema registry in Fivetran/Airbyte; auto-detection with field mapping UI |
| Code mapping (processor → internal) | Metadata table in warehouse or dbt seed file — version-controlled; ops team inserts a new row, no deployment needed | Vendor tool's built-in field mapping UI — limited to supported types; cannot encode custom business logic |
| SFTP ingestion | Python `paramiko` + GCS write; cron via Cloud Scheduler | Native SFTP connector (Fivetran, Airbyte, Stitch) |
| Idempotency | Custom: file manifest `(filename, md5_hash)` + row-level MERGE | Tool-managed: Fivetran tracks synced files automatically |
| Operational overhead | Engineer on-call for schema breaks | Self-service: ops team can update mappings without engineer |
| Cost at scale | ~$0/month (GCP compute only) | Fivetran: ~$500–$2,000/month depending on MAR (monthly active rows) |
| Time to first data | 1–2 weeks (build + test) | 1–3 days (connector config) |
| Custom business logic | Full control: apply DQ checks, enrich with dim data, flag anomalies inline | Limited: most tools do extract-load only; transform is separate (dbt) |

### Recommendation: **Build — with a metadata-driven code mapping layer**

**Rationale:**

1. **Schema instability is the decisive factor.** Off-shelf tools handle schema drift better in theory, but when the schema is *undocumented and changes without notice*, the tool's auto-detection still requires human review on every change. The operational overhead is similar — the only difference is where the engineer spends their time (YAML config vs Python code). A custom parser with explicit schema versioning and a fallback-to-raw-column mode is more transparent.

2. **Code mapping cannot be bought.** The transaction type mapping from processor-specific codes to internal taxonomy is business logic. No ETL tool ships with your internal taxonomy pre-loaded. You will write this mapping regardless — the question is whether it lives in a config file, a metadata table, or hardcoded in a vendor connector. A metadata table (with a simple admin UI or `dbt seed` file) is the cleanest solution and is only available with a build approach.

3. **This is not a novel problem.** The CSV-over-SFTP pattern is well-understood. The `vendor_ingestion.py` prototype in this repository demonstrates the complete pattern in ~150 lines: SFTP pull → file manifest check → schema normalisation → negative amount/orphan DQ → MERGE into warehouse. The engineering cost is low.

4. **Buy wins when**: there are many sources (10+), the ops team needs self-service connectors, and schemas are reasonably stable. At one new processor with a pathologically unstable schema, buy adds cost without reducing complexity.

### Build Architecture for This Processor

```
SFTP server (processor)
        │
        ▼ (daily at 02:00 UTC)
Cloud Scheduler → Cloud Run job: sftp_pull.py
  - Connects to SFTP with key auth
  - Downloads new files only (checks pipeline_file_manifest)
  - Writes raw CSV to GCS: gs://deriv-raw/payment_processor_x/YYYY-MM-DD/
        │
        ▼
Airflow DAG: payment_processor_x_ingestion
  - GCSObjectExistenceSensor
  - Task 1: schema_validate — detect schema drift vs registered schema v1
      • If drift: emit alert + write raw to quarantine bucket, stop
      • If match: proceed
  - Task 2: normalise — apply field renames, code mapping from metadata table
  - Task 3: dq_checks — negative amount, orphan client, null required fields
  - Task 4: load — MERGE into fct_deposit_staging
  - Task 5: dbt run — merge staging → fct_deposit production
```

#### Code Mapping Metadata Table

```sql
CREATE TABLE `deriv-warehouse.reference.payment_processor_code_map` (
    processor_name      STRING    NOT NULL
  , processor_code      STRING    NOT NULL
  , internal_tx_type    STRING    NOT NULL
  , effective_from      DATE      NOT NULL
  , effective_to        DATE                   -- NULL = current
  , notes               STRING
);

-- Query at ingest time:
SELECT internal_tx_type
  FROM `deriv-warehouse.reference.payment_processor_code_map`
 WHERE processor_name = 'processor_x'
   AND processor_code = @raw_code
   AND effective_from <= @file_date
   AND (effective_to IS NULL OR effective_to >= @file_date)
```

When the processor changes a code, the ops team inserts a new row with `effective_from = <change_date>`. Old rows remain for historical re-processing. No deployment required.

#### Schema Drift Handler

```python
def validate_schema(df: pd.DataFrame, expected_schema: dict, file_name: str) -> bool:
    """
    Returns True if schema matches registered version.
    On drift: logs diff, writes file to quarantine, returns False.
    """
    actual_cols = set(df.columns)
    expected_cols = set(expected_schema.keys())

    missing = expected_cols - actual_cols
    unexpected = actual_cols - expected_cols

    if missing or unexpected:
        logger.error(
            "Schema drift detected in %s: missing=%s, unexpected=%s",
            file_name, missing, unexpected
        )
        quarantine_file(file_name, reason="schema_drift")
        send_alert(
            f"Schema drift in payment_processor_x: {file_name}\n"
            f"Missing columns: {missing}\nNew columns: {unexpected}"
        )
        return False
    return True
```

The quarantine bucket retains the file for 30 days. After the schema is updated in the registry, a backfill DAG re-processes the quarantined file — no data is lost.

---

### Summary: Architecture Decisions

| Decision | Choice | Key reason |
|----------|--------|------------|
| Integration hub | Kafka | Decouples producers from consumers; each consumer runs at its own pace |
| Real-time processing | Flink | Stateful windowing, keyed state per client_id, < 5s end-to-end |
| State store for enrichment | Redis | Sub-10ms client dim lookup; BigQuery is too slow for per-event enrichment |
| Batch path | Unchanged BigQuery + dbt | Consistency > speed for finance reporting; no reason to migrate |
| External partner access | REST API over materialized view | Isolation, audit logging, rate limiting; 30 min staleness acceptable |
| Payment processor onboarding | Build | Schema instability + code mapping = build wins; one source, low volume |
| Code mapping | Metadata table | Business logic in version-controlled data, not code; ops team self-service |
