# Idempotent Pipeline Prototype

A runnable Python prototype demonstrating core data pipeline design patterns
for a trading-company data warehouse, using SQLite as a local BigQuery stand-in.

## How to run

```
cd code/
python pipeline.py
```

Run it a second time to confirm idempotency — output will show all files skipped
and zero new rows inserted.

To reset and re-run from scratch:

```
python pipeline.py --reset
```

Requires Python 3.10+. No external dependencies.

## What it demonstrates

| Pattern | Where |
|---|---|
| Immutable landing layer | Every source record stored as a raw JSON blob with `insert_timestamp`; rows are never updated |
| SHA-256 file checksum | File skipped entirely if checksum matches the last load — idempotency at the file level |
| DQ checks with error routing | Six rules checked per deposit; failures written to `staging_dq_errors`, not silently dropped |
| INSERT IF NOT EXISTS | `target_deposits` uses `INSERT OR IGNORE` on `(deposit_id, deposit_date)` — no MERGE/UPDATE |
| Watermark-based CDC | CDC events loaded as-is; replayed in LSN order above the stored watermark; safe to re-run |

## Source data loaded

- `client_deposit.json` — 20 internal deposits
- `deposits_vendor_20240301/02/03.csv` — three daily vendor files (contains overlaps + bad rows)
- `client_profile.json` — 30 client profiles (used for DQ referential check)
- `client_signup.json`, `client_trades.json` — additional reference data landed as-is
- `client_profile_changes.jsonl` — out-of-order CDC change-log (insert/update/delete)

## Database tables created

`pipeline.db` (SQLite) is created in this directory on first run.

- `landing_files` — one row per file load, keyed by (filename, checksum)
- `landing_raw` — immutable JSON blobs, one per source record
- `staging_deposits` — parsed, DQ-passed deposit records
- `staging_dq_errors` — failed DQ records with rule and raw value
- `target_deposits` — final deposits table; idempotency key `(deposit_id, deposit_date)`
- `landing_cdc_raw` — raw CDC events, keyed by (source_file, lsn)
- `target_client_profile` — current client state after CDC replay
- `pipeline_metadata` — watermark store (`cdc_max_lsn_applied`)
