# Part 4 — Real-Time Architecture & Build vs Buy

## Part 4a — Real-Time Architecture: Fraud Detection + Existing Batch Analytics

### Requirement

Fraud detection must fire **within seconds** of a deposit event. The existing batch pipeline — which lands vendor CSVs, loads `fct_deposit`, and runs `agg_monthly_pnl_by_instrument` — must continue without disruption.

---

### Architecture: Lambda-style dual-path with shared Kafka backbone

```
                     ┌─────────────────────────────────────┐
                     │         Source Systems               │
                     │  Vendor deposit API  |  CDC stream   │
                     └──────────┬──────────┴───────┬────────┘
                                │                  │
                                ▼                  ▼
                     ┌──────────────────────────────────────┐
                     │           Kafka (Confluent)           │
                     │   topic: deposits.raw                 │
                     │   topic: client_profile.cdc           │
                     └───────┬──────────────┬───────────────┘
                             │              │
              ┌──────────────┘              └──────────────────┐
              │ Speed path (seconds)                           │ Batch path (hours)
              ▼                                                ▼
  ┌───────────────────────┐                     ┌─────────────────────────┐
  │  Flink / Cloud        │                     │  Existing batch jobs    │
  │  Dataflow (streaming) │                     │  (Cloud Composer/       │
  │                       │                     │   Airflow DAGs)         │
  │  Per-event:           │                     │                         │
  │  • Velocity check     │                     │  • Vendor CSV → landing │
  │  • Blacklist lookup   │                     │  • Staging DQ           │
  │  • Amount threshold   │                     │  • fct_deposit load     │
  │  • Country risk flag  │                     │  • agg_monthly rebuild  │
  └──────────┬────────────┘                     └──────────┬──────────────┘
             │                                             │
             ▼                                             ▼
  ┌───────────────────────┐                     ┌─────────────────────────┐
  │  fraud_alerts table   │                     │  BigQuery warehouse      │
  │  (BigQuery streaming  │                     │  (batch load path,       │
  │   inserts or          │                     │   unchanged)             │
  │   Firestore/Redis     │                     └─────────────────────────┘
  │   for sub-second      │
  │   lookup)             │
  └──────────┬────────────┘
             │
             ▼
  ┌───────────────────────┐
  │  Alert dispatch       │
  │  • Slack #fraud-ops   │
  │  • PubSub → case mgmt │
  │  • Block API call     │
  └───────────────────────┘
```

---

### Speed Path — Fraud Detection Design

**Technology:** Apache Flink (Cloud Dataflow Streaming or Confluent's managed Flink service)

**Input:** `deposits.raw` Kafka topic — one message per deposit event, published the moment the vendor API confirms the deposit

**Fraud rules evaluated per event (stateful streaming):**

| Rule | Window | Implementation |
|---|---|---|
| Velocity: >3 deposits in 10 min per client | 10-min tumbling window | Flink keyed state on `client_id` |
| Large amount: single deposit > $50k | Stateless filter | Field check on `amount_usd` |
| High-risk country | Stateless lookup | Broadcast state: country-risk reference table |
| Client blacklist | Stateless lookup | Redis/Memorystore: `client_id` → risk flag |
| Dormant client sudden activity | Session window | Flink session window with >30d gap detection |

**Output:** `fraud_alerts` — streamed to BigQuery (streaming inserts) and mirrored to Firestore for sub-second API lookups by the fraud case management system.

**SLA:** End-to-end latency from deposit event to alert dispatch: **< 5 seconds** (Kafka ingestion ~100ms, Flink window evaluation ~1–3s, alert dispatch ~500ms).

---

### Batch Path — Unchanged

The existing batch pipeline continues to read vendor CSV files from GCS on its current schedule. It is unaware of the streaming path. The `fct_deposit` table receives the same INSERT IF NOT EXISTS loads as designed in Part 1.

The Kafka `deposits.raw` topic is also consumed by a batch sink connector that writes raw events to `landing.deposits_streaming` — a separate landing table for the streaming source. This is distinct from `landing.vendor_deposits` (file-based). Both feed `fct_deposit` but through their own staging paths.

---

### Why Not Replace Batch With Streaming?

| Dimension | Streaming path | Batch path |
|---|---|---|
| Fraud detection (< 5s SLA) | Required | Cannot meet SLA |
| Monthly PnL aggregates (30 min SLA) | Overkill, complexity | Right tool |
| Vendor CSV reconciliation | N/A — file-based source | Handles naturally |
| Cost | Higher (always-on Flink cluster) | Lower (runs on schedule) |
| Failure recovery | Kafka offset replay | Re-run DAG from landing |

Running both paths in parallel ("lambda architecture") is the standard industry answer for this exact combination of requirements. The streaming path handles latency-sensitive decisions; the batch path handles completeness and consistency.

---

### State Management for Fraud Rules

The velocity rule requires per-client counts within a 10-minute window. This state must survive Flink task restarts:

- **Flink state backend:** RocksDB (persistent, spills to disk, survives checkpoint recovery)
- **Checkpointing:** every 60 seconds to GCS
- **On restart:** Flink resumes from last checkpoint, replays Kafka from the committed offset — no double-counting, no missed events

---

### Client Risk Lookup — Redis vs BigQuery

The blacklist and country-risk lookups must complete in < 50ms to stay within the 5s SLA. BigQuery query latency is 200ms–2s per query — too slow for per-event lookups.

**Fix:** Synchronise the `dim_client` risk flags and the country-risk reference table to Redis (Cloud Memorystore) on a 5-minute schedule. The Flink job looks up Redis at < 5ms. If Redis misses (new client not yet synced), fall back to a BigQuery point lookup with a 500ms timeout — flag as inconclusive rather than block.

---

## Part 4b — Build vs Buy: New Payment Processor Connector

### Context

A new payment processor has been signed. A custom connector to ingest their transaction data needs to be built (or bought). The choice is between:

- **Build:** Custom Python/Go connector, deployed as a Cloud Run service or Airflow operator
- **Buy:** Managed connector via Fivetran, Airbyte, or RudderStack

---

### Decision Framework

| Dimension | Build | Buy (Fivetran / Airbyte) |
|---|---|---|
| **Time to first data** | 2–6 weeks (development + testing) | 1–3 days (configure + activate) |
| **Maintenance burden** | Owned by DE team: API changes, auth rotation, pagination bugs, rate limits | Vendor-owned: API version upgrades handled automatically |
| **Schema evolution** | Manual — team must detect and handle upstream schema changes | Automatic schema migration (Fivetran) or configurable (Airbyte) |
| **Cost** | Engineering time (hidden cost); infra cost is low | Explicit per-row or per-connector pricing; predictable |
| **Customisation** | Full control — custom DQ, custom error handling, custom retry logic | Limited to what the vendor exposes |
| **Control over landing format** | Full — can land as JSON blob to match existing architecture | Fivetran lands in its own normalised format; may not match JSON-blob landing layer |
| **Compliance / data residency** | Full control | Vendor handles data in transit — requires DPA review |
| **Single-source APIs (proprietary)** | Sometimes required — vendor may not have a connector | Fivetran covers 500+ connectors; niche processors may not be listed |

---

### Decision Framework: Assess Future Requirements First

Before choosing any tool, the first question is: **what does the roadmap look like for this source?**

This is a single payment processor, a new requirement. Onboarding a managed connector tool (Fivetran, Airbyte) for a single use case introduces tooling overhead — procurement, DPA review, team onboarding, billing management — that is unlikely to pay off if the requirement is isolated.

**Decision tree:**

```
Is this a one-off / single source integration?
  │
  ├─ YES → Will managed tools provide the data in batch format
  │         AND does the batch schedule meet SLA?
  │           │
  │           ├─ YES → Build custom integration with AI-assisted code
  │           │         Run on existing infra (Cloud Run / Airflow operator)
  │           │         No new tool to onboard, no new billing relationship
  │           │
  │           └─ NO  → Build custom integration regardless
  │
  └─ NO (multiple sources or clear roadmap of future integrations)
      │
      └─ Does the API have complex structure / high maintenance surface?
            AND are there multiple future requirements anticipated?
              │
              ├─ YES → Consider Fivetran / Airbyte
              │         (complex API pagination, auth rotation, schema evolution
              │          justify the tooling overhead when spread across many sources)
              │
              └─ NO  → Build custom, keep it simple
```

---

### Recommendation: Build — AI-Assisted Custom Connector on Existing Infra

For this payment processor integration:

**Build a lightweight custom connector** — a Cloud Run service or Airflow operator that:
1. Polls the payment processor API (handles auth, pagination, retry)
2. Lands raw JSON responses as-is into `landing.payment_processor_raw` (same JSON-blob pattern as Part 1)
3. Runs on the existing infra — no new billing relationship, no new tool to operate

**Why not buy for this case:**
- Single use case — the ROI of onboarding a managed connector tool does not justify the overhead
- The batch data pattern is the same as the existing vendor deposit pipeline — reuse is straightforward
- AI-assisted code generation makes building a correct, idempotent connector fast (hours, not weeks)
- The team already owns the staging → target pipeline; the connector only needs to land raw data

**When to revisit the buy decision:**
- The organisation signs 5+ new payment processors or data vendors within a 6-month window
- The APIs have genuinely complex structures (dynamic schemas, nested pagination, OAuth2 with short-lived tokens)
- Maintenance cost of multiple custom connectors exceeds the cost of a managed platform

At that point, **Fivetran** is the right tool — it is designed precisely for high-volume, multi-source API ingestion with complex auth and schema evolution. That decision should be driven by a clear requirement roadmap, not by the existence of the first single integration.

---

### Build Architecture

```
Payment processor API
        │
        ▼
  Custom Cloud Run job (Python, AI-generated)
  — API auth, pagination, retry handled here
  — Lands raw JSON per batch as metadata_json blob
        │
        ▼
  landing.payment_processor_raw
  (file_name, metadata_json, insert_timestamp)
  Idempotency: SHA-256 checksum on response payload
        │
        ▼
  Existing staging → target pipeline
  (reuse DQ check suite from Part 1b, no changes)
```

Total new code: ~200 lines for the Cloud Run connector. Zero new tooling. Zero new vendor contracts. Runs in the same Airflow DAG as the existing vendor deposit pipeline.
