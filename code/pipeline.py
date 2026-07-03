"""
pipeline.py — Idempotent Data Pipeline Prototype
=================================================
Demonstrates key design patterns for a trading-company data warehouse:

  1. LANDING     — ingest source files as immutable JSON blobs with SHA-256
                   checksum; skip if file unchanged (idempotency at file level)
  2. STAGING      — parse blobs, run DQ checks, route failures to error table
  3. TARGET       — INSERT IF NOT EXISTS on (deposit_id, deposit_date);
                   never updates an existing deposit row
  4. CDC          — load all change-log events raw, then replay in LSN order
                   using a watermark; safe to re-run, never re-processes old LSNs

SQLite stands in for BigQuery.  No external dependencies — stdlib only.

Usage:
    python pipeline.py            # first run  — loads everything
    python pipeline.py            # second run — all skipped (idempotent)
    python pipeline.py --reset    # drops DB and re-runs from scratch
"""

import csv
import hashlib
import io
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "..", "..", "data")
DB_PATH    = os.path.join(SCRIPT_DIR, "pipeline.db")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_string(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def log(msg: str) -> None:
    print(f"  {msg}")


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

DDL = """
-- ── landing ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS landing_files (
    source_file         TEXT        NOT NULL,
    file_checksum       TEXT        NOT NULL,
    insert_timestamp    TEXT        NOT NULL,
    record_count        INTEGER,
    PRIMARY KEY (source_file, file_checksum)
);

-- One row per record, stored as raw JSON blob (immutable once written)
CREATE TABLE IF NOT EXISTS landing_raw (
    landing_id          INTEGER     PRIMARY KEY AUTOINCREMENT,
    source_file         TEXT        NOT NULL,
    file_checksum       TEXT        NOT NULL,
    record_index        INTEGER     NOT NULL,   -- position within file
    metadata_json       TEXT        NOT NULL,   -- raw source blob
    insert_timestamp    TEXT        NOT NULL,
    UNIQUE (source_file, file_checksum, record_index)
);

-- ── staging ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging_deposits (
    staging_id          INTEGER     PRIMARY KEY AUTOINCREMENT,
    landing_id          INTEGER     NOT NULL REFERENCES landing_raw(landing_id),
    deposit_id          TEXT,
    client_id           TEXT,
    deposit_date        TEXT,
    amount_usd          REAL,
    payment_method      TEXT,
    currency_original   TEXT,
    exchange_rate       REAL,
    status              TEXT,
    processing_days     INTEGER,
    fee_usd             REAL,
    source_file         TEXT,
    staged_at           TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_dq_errors (
    error_id            INTEGER     PRIMARY KEY AUTOINCREMENT,
    landing_id          INTEGER     NOT NULL REFERENCES landing_raw(landing_id),
    source_file         TEXT,
    deposit_id          TEXT,
    dq_rule             TEXT        NOT NULL,
    raw_value           TEXT,
    error_at            TEXT        NOT NULL
);

-- ── target ────────────────────────────────────────────────────────────────
-- Idempotency key: (deposit_id, deposit_date)
-- We INSERT IF NOT EXISTS — never update an existing row.
CREATE TABLE IF NOT EXISTS target_deposits (
    deposit_id          TEXT        NOT NULL,
    deposit_date        TEXT        NOT NULL,
    client_id           TEXT,
    amount_usd          REAL,
    payment_method      TEXT,
    currency_original   TEXT,
    exchange_rate       REAL,
    status              TEXT,
    processing_days     INTEGER,
    fee_usd             REAL,
    source_file         TEXT,
    loaded_at           TEXT        NOT NULL,
    PRIMARY KEY (deposit_id, deposit_date)
);

-- ── CDC / client profiles ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS landing_cdc_raw (
    landing_id          INTEGER     PRIMARY KEY AUTOINCREMENT,
    source_file         TEXT        NOT NULL,
    lsn                 INTEGER     NOT NULL,
    raw_json            TEXT        NOT NULL,
    insert_timestamp    TEXT        NOT NULL,
    UNIQUE (source_file, lsn)          -- idempotency: same LSN never loaded twice
);

-- Processed change events (final state per client after watermark)
CREATE TABLE IF NOT EXISTS target_client_profile (
    client_id               TEXT        PRIMARY KEY,
    full_name               TEXT,
    date_of_birth           TEXT,
    nationality             TEXT,
    risk_category           TEXT,
    account_balance_usd     REAL,
    account_status          TEXT,
    currency                TEXT,
    preferred_language      TEXT,
    last_lsn_applied        INTEGER,
    last_op                 TEXT,
    updated_at              TEXT        NOT NULL
);

-- ── pipeline metadata (watermarks) ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    key                 TEXT        PRIMARY KEY,
    value               TEXT        NOT NULL,
    updated_at          TEXT        NOT NULL
);
"""


def bootstrap(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


# ---------------------------------------------------------------------------
# 1.  Landing layer  — immutable, checksum-gated
# ---------------------------------------------------------------------------

def landing_already_loaded(conn: sqlite3.Connection, source_file: str, checksum: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM landing_files WHERE source_file = ? AND file_checksum = ?",
        (source_file, checksum),
    ).fetchone()
    return row is not None


def insert_landing_file(conn: sqlite3.Connection, source_file: str, checksum: str, record_count: int) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO landing_files (source_file, file_checksum, insert_timestamp, record_count)
        VALUES (?, ?, ?, ?)
        """,
        (source_file, checksum, now_utc(), record_count),
    )


def insert_landing_raw_record(
    conn: sqlite3.Connection,
    source_file: str,
    checksum: str,
    idx: int,
    record_json: str,
) -> int:
    """Insert one raw blob; return the landing_id."""
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO landing_raw
            (source_file, file_checksum, record_index, metadata_json, insert_timestamp)
        VALUES (?, ?, ?, ?, ?)
        """,
        (source_file, checksum, idx, record_json, now_utc()),
    )
    if cur.lastrowid:
        return cur.lastrowid
    # Row already existed (IGNORE path) — look it up
    row = conn.execute(
        "SELECT landing_id FROM landing_raw WHERE source_file=? AND file_checksum=? AND record_index=?",
        (source_file, checksum, idx),
    ).fetchone()
    return row[0]


def load_json_to_landing(conn: sqlite3.Connection, filename: str) -> list[int]:
    """
    Load a JSON array file into landing_raw.
    Returns list of landing_ids for newly inserted records.
    Skips the entire file if checksum matches last load.
    """
    path = os.path.join(DATA_DIR, filename)
    checksum = sha256_file(path)

    if landing_already_loaded(conn, filename, checksum):
        log(f"SKIP  {filename}  (checksum unchanged)")
        return []

    with open(path, "r") as fh:
        records = json.load(fh)

    landing_ids = []
    for idx, record in enumerate(records):
        blob = json.dumps(record, ensure_ascii=False)
        lid = insert_landing_raw_record(conn, filename, checksum, idx, blob)
        landing_ids.append(lid)

    insert_landing_file(conn, filename, checksum, len(records))
    conn.commit()
    log(f"LOAD  {filename}  — {len(records)} records landed  (checksum: {checksum[:12]}…)")
    return landing_ids


def load_csv_to_landing(conn: sqlite3.Connection, filename: str) -> list[int]:
    """
    Load a CSV file into landing_raw (each row stored as JSON blob).
    Skips if checksum matches.
    """
    path = os.path.join(DATA_DIR, filename)
    checksum = sha256_file(path)

    if landing_already_loaded(conn, filename, checksum):
        log(f"SKIP  {filename}  (checksum unchanged)")
        return []

    with open(path, "r", newline="") as fh:
        reader = csv.DictReader(fh)
        records = list(reader)

    landing_ids = []
    for idx, record in enumerate(records):
        blob = json.dumps(record, ensure_ascii=False)
        lid = insert_landing_raw_record(conn, filename, checksum, idx, blob)
        landing_ids.append(lid)

    insert_landing_file(conn, filename, checksum, len(records))
    conn.commit()
    log(f"LOAD  {filename}  — {len(records)} records landed  (checksum: {checksum[:12]}…)")
    return landing_ids


# ---------------------------------------------------------------------------
# 2.  Staging  — parse blobs, DQ checks, route failures
# ---------------------------------------------------------------------------

VALID_PAYMENT_METHODS = {"bank_transfer", "credit_card", "e_wallet"}
VALID_STATUSES        = {"completed", "pending", "failed"}
MIN_DEPOSIT_USD       = 0.01
MAX_DEPOSIT_USD       = 500_000.00
KNOWN_CLIENTS         = None   # populated lazily from client_profile.json


def get_known_clients(conn: sqlite3.Connection) -> set:
    """Return all client_ids present in the client_profile landing data."""
    rows = conn.execute(
        """
        SELECT json_extract(metadata_json, '$.client_id')
          FROM landing_raw
         WHERE source_file = 'client_profile.json'
        """
    ).fetchall()
    return {r[0] for r in rows if r[0]}


def already_staged(conn: sqlite3.Connection, landing_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM staging_deposits WHERE landing_id = ?", (landing_id,)
    ).fetchone()
    return row is not None


def already_in_dq_errors(conn: sqlite3.Connection, landing_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM staging_dq_errors WHERE landing_id = ?", (landing_id,)
    ).fetchone()
    return row is not None


def run_dq_checks(record: dict, known_clients: set) -> list[tuple]:
    """
    Return a list of (rule_name, raw_value) for each failed check.
    Empty list means all checks passed.
    """
    failures = []

    # Rule 1: deposit_id must be non-null
    deposit_id = record.get("deposit_id")
    if not deposit_id:
        failures.append(("deposit_id_null", str(deposit_id)))

    # Rule 2: amount_usd must be positive
    try:
        amount = float(record.get("amount_usd", ""))
        if amount <= 0:
            failures.append(("amount_usd_not_positive", str(amount)))
        elif amount > MAX_DEPOSIT_USD:
            failures.append(("amount_usd_exceeds_max", str(amount)))
    except (TypeError, ValueError):
        failures.append(("amount_usd_not_numeric", str(record.get("amount_usd"))))

    # Rule 3: deposit_date must be parseable YYYY-MM-DD
    deposit_date = record.get("deposit_date", "")
    try:
        datetime.strptime(str(deposit_date), "%Y-%m-%d")
    except ValueError:
        failures.append(("deposit_date_invalid", str(deposit_date)))

    # Rule 4: payment_method must be in known values
    #         (some records use 'credit_card' as key name instead of 'payment_method')
    payment_method = record.get("payment_method") or record.get("credit_card")
    if payment_method not in VALID_PAYMENT_METHODS:
        failures.append(("payment_method_invalid", str(payment_method)))

    # Rule 5: client_id must exist in client_profile
    client_id = record.get("client_id")
    if known_clients and client_id not in known_clients:
        failures.append(("client_id_unknown", str(client_id)))

    # Rule 6: exchange_rate must be positive
    try:
        rate = float(record.get("exchange_rate", ""))
        if rate <= 0:
            failures.append(("exchange_rate_not_positive", str(rate)))
    except (TypeError, ValueError):
        failures.append(("exchange_rate_not_numeric", str(record.get("exchange_rate"))))

    return failures


def stage_deposit_records(conn: sqlite3.Connection, landing_ids: list[int], source_file: str) -> None:
    """Parse landing blobs for deposit files, DQ-check each, route pass/fail."""
    if not landing_ids:
        return

    known_clients = get_known_clients(conn)
    passed = failed = skipped = 0

    for lid in landing_ids:
        # Idempotency — skip if already staged or errored
        if already_staged(conn, lid) or already_in_dq_errors(conn, lid):
            skipped += 1
            continue

        row = conn.execute(
            "SELECT metadata_json FROM landing_raw WHERE landing_id = ?", (lid,)
        ).fetchone()
        if not row:
            continue

        record = json.loads(row[0])
        dq_failures = run_dq_checks(record, known_clients)

        if dq_failures:
            for rule, raw_val in dq_failures:
                conn.execute(
                    """
                    INSERT INTO staging_dq_errors
                        (landing_id, source_file, deposit_id, dq_rule, raw_value, error_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (lid, source_file, record.get("deposit_id"), rule, raw_val, now_utc()),
                )
            failed += 1
        else:
            conn.execute(
                """
                INSERT OR IGNORE INTO staging_deposits
                    (landing_id, deposit_id, client_id, deposit_date, amount_usd,
                     payment_method, currency_original, exchange_rate, status,
                     processing_days, fee_usd, source_file, staged_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lid,
                    record.get("deposit_id"),
                    record.get("client_id"),
                    record.get("deposit_date"),
                    float(record.get("amount_usd", 0)),
                    record.get("payment_method") or record.get("credit_card"),
                    record.get("currency_original"),
                    float(record.get("exchange_rate", 0)),
                    record.get("status"),
                    record.get("processing_days"),
                    float(record.get("fee_usd", 0)),
                    source_file,
                    now_utc(),
                ),
            )
            passed += 1

    conn.commit()
    if skipped:
        log(f"  staging {source_file}: {passed} passed | {failed} failed DQ | {skipped} already staged")
    else:
        log(f"  staging {source_file}: {passed} passed | {failed} failed DQ")


# ---------------------------------------------------------------------------
# 3.  Target  — INSERT IF NOT EXISTS on (deposit_id, deposit_date)
# ---------------------------------------------------------------------------

def load_target_deposits(conn: sqlite3.Connection) -> None:
    """
    Promote clean staged records to target_deposits.
    Uses INSERT OR IGNORE so re-running never creates duplicates.
    The PRIMARY KEY (deposit_id, deposit_date) is the idempotency key.
    We never UPDATE an existing deposit row — immutable once written.
    """
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO target_deposits
            (deposit_id, deposit_date, client_id, amount_usd, payment_method,
             currency_original, exchange_rate, status, processing_days,
             fee_usd, source_file, loaded_at)
        SELECT  s.deposit_id
              , s.deposit_date
              , s.client_id
              , s.amount_usd
              , s.payment_method
              , s.currency_original
              , s.exchange_rate
              , s.status
              , s.processing_days
              , s.fee_usd
              , s.source_file
              , ?
          FROM staging_deposits s
         WHERE NOT EXISTS (
               SELECT 1
                 FROM target_deposits t
                WHERE t.deposit_id   = s.deposit_id
                  AND t.deposit_date = s.deposit_date
         )
        """,
        (now_utc(),),
    )
    conn.commit()
    inserted = cur.rowcount
    log(f"target_deposits: {inserted} rows inserted (INSERT IF NOT EXISTS on deposit_id + deposit_date)")


# ---------------------------------------------------------------------------
# 4.  CDC  — client profile change-log (JSONL, out-of-order by LSN)
# ---------------------------------------------------------------------------

def load_cdc_to_landing(conn: sqlite3.Connection, filename: str) -> int:
    """
    Load CDC JSONL events into landing_cdc_raw.
    Each event is keyed by (source_file, lsn) — UNIQUE constraint prevents
    duplicate loads.  Returns count of newly inserted rows.
    """
    path = os.path.join(DATA_DIR, filename)

    # File-level checksum gate — identical to other landing sources
    checksum = sha256_file(path)
    if landing_already_loaded(conn, filename, checksum):
        log(f"SKIP  {filename}  (checksum unchanged)")
        return 0

    inserted = 0
    with open(path, "r") as fh:
        for raw_line in fh:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            event = json.loads(raw_line)
            lsn = int(event["lsn"])
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO landing_cdc_raw
                    (source_file, lsn, raw_json, insert_timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (filename, lsn, raw_line, now_utc()),
            )
            if cur.rowcount:
                inserted += 1

    insert_landing_file(conn, filename, checksum, inserted)
    conn.commit()
    log(f"LOAD  {filename}  — {inserted} CDC events landed")
    return inserted


def get_cdc_watermark(conn: sqlite3.Connection) -> int:
    """Return the highest LSN already applied to target_client_profile."""
    row = conn.execute(
        "SELECT value FROM pipeline_metadata WHERE key = 'cdc_max_lsn_applied'",
    ).fetchone()
    return int(row[0]) if row else 0


def set_cdc_watermark(conn: sqlite3.Connection, lsn: int) -> None:
    conn.execute(
        """
        INSERT INTO pipeline_metadata (key, value, updated_at)
        VALUES ('cdc_max_lsn_applied', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (str(lsn), now_utc()),
    )


def process_cdc_events(conn: sqlite3.Connection) -> None:
    """
    Replay CDC events in LSN order, starting from the current watermark.
    Applies insert / update / delete to target_client_profile.
    Advances the watermark after each batch — safe to re-run.
    """
    watermark = get_cdc_watermark(conn)
    log(f"CDC watermark (last applied LSN): {watermark}")

    rows = conn.execute(
        """
        SELECT lsn, raw_json
          FROM landing_cdc_raw
         WHERE lsn > ?
         ORDER BY lsn ASC
        """,
        (watermark,),
    ).fetchall()

    if not rows:
        log("CDC: no new events above watermark — nothing to process")
        return

    applied = 0
    max_lsn = watermark

    for lsn, raw_json in rows:
        event = json.loads(raw_json)
        op    = event.get("op")
        cid   = event.get("client_id")
        after = event.get("after") or {}

        if op == "insert":
            conn.execute(
                """
                INSERT OR IGNORE INTO target_client_profile
                    (client_id, full_name, date_of_birth, nationality,
                     risk_category, account_balance_usd, account_status,
                     currency, preferred_language, last_lsn_applied, last_op, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cid,
                    after.get("full_name"),
                    after.get("date_of_birth"),
                    after.get("nationality"),
                    after.get("risk_category"),
                    after.get("account_balance_usd"),
                    after.get("account_status"),
                    after.get("currency"),
                    after.get("preferred_language"),
                    lsn,
                    op,
                    now_utc(),
                ),
            )
        elif op == "update":
            # Build SET clause only for fields present in the after image
            updates = {k: v for k, v in after.items()}
            if not updates:
                continue
            set_parts = ", ".join(f"{k} = ?" for k in updates)
            values    = list(updates.values()) + [lsn, op, now_utc(), cid]
            conn.execute(
                f"""
                UPDATE target_client_profile
                   SET {set_parts}
                     , last_lsn_applied = ?
                     , last_op          = ?
                     , updated_at       = ?
                 WHERE client_id = ?
                """,
                values,
            )
        elif op == "delete":
            conn.execute(
                """
                UPDATE target_client_profile
                   SET account_status    = 'deleted'
                     , last_lsn_applied  = ?
                     , last_op           = ?
                     , updated_at        = ?
                 WHERE client_id = ?
                """,
                (lsn, op, now_utc(), cid),
            )

        max_lsn = lsn
        applied += 1

    set_cdc_watermark(conn, max_lsn)
    conn.commit()
    log(f"CDC: {applied} events applied  |  watermark advanced to LSN {max_lsn}")


# ---------------------------------------------------------------------------
# 5.  Summary report
# ---------------------------------------------------------------------------

def print_summary(conn: sqlite3.Connection) -> None:
    section("PIPELINE RUN SUMMARY")

    # Landing
    rows = conn.execute(
        "SELECT source_file, record_count, insert_timestamp FROM landing_files ORDER BY insert_timestamp"
    ).fetchall()
    log(f"{'Landing files loaded':40s}: {len(rows)}")
    for r in rows:
        log(f"    {r[0]:<45s}  {r[1]} records")

    # Staging pass/fail
    passed = conn.execute("SELECT COUNT(*) FROM staging_deposits").fetchone()[0]
    failed = conn.execute("SELECT COUNT(*) FROM staging_dq_errors").fetchone()[0]
    log("")
    log(f"{'Staging — passed DQ':40s}: {passed}")
    log(f"{'Staging — failed DQ (routed to error table)':40s}: {failed}")

    # DQ breakdown
    if failed:
        log("")
        log("  DQ failure breakdown:")
        for r in conn.execute(
            """
            SELECT dq_rule, source_file, deposit_id, raw_value
              FROM staging_dq_errors
             ORDER BY dq_rule, source_file
            """
        ).fetchall():
            log(f"    rule={r[0]}  file={r[1]}  deposit_id={r[2]}  raw_value={r[3]}")

    # Target deposits
    target_count = conn.execute("SELECT COUNT(*) FROM target_deposits").fetchone()[0]
    log("")
    log(f"{'target_deposits rows':40s}: {target_count}")

    # CDC
    cdc_staged  = conn.execute("SELECT COUNT(*) FROM landing_cdc_raw").fetchone()[0]
    profiles    = conn.execute("SELECT COUNT(*) FROM target_client_profile").fetchone()[0]
    watermark   = get_cdc_watermark(conn)
    log(f"{'CDC events in landing':40s}: {cdc_staged}")
    log(f"{'target_client_profile rows':40s}: {profiles}")
    log(f"{'CDC watermark (max LSN applied)':40s}: {watermark}")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    section("STAGE 1 — LANDING (immutable, checksum-gated)")

    conn = sqlite3.connect(DB_PATH)
    bootstrap(conn)

    # JSON source files
    log("Loading JSON source files...")
    load_json_to_landing(conn, "client_deposit.json")
    load_json_to_landing(conn, "client_profile.json")
    load_json_to_landing(conn, "client_signup.json")
    load_json_to_landing(conn, "client_trades.json")

    # Vendor deposit CSVs (three daily files)
    log("\nLoading vendor deposit CSV files...")
    csv_files = [
        "deposits_vendor_20240301.csv",
        "deposits_vendor_20240302.csv",
        "deposits_vendor_20240303.csv",
    ]
    for f in csv_files:
        load_csv_to_landing(conn, f)

    # CDC JSONL
    log("\nLoading CDC change-log...")
    load_cdc_to_landing(conn, "client_profile_changes.jsonl")

    # -------------------------------------------------------------------------
    section("STAGE 2 — STAGING + DQ (parse, check, route failures)")

    log("Staging client_deposit.json records...")
    deposit_lids = conn.execute(
        "SELECT landing_id FROM landing_raw WHERE source_file = 'client_deposit.json'"
    ).fetchall()
    stage_deposit_records(conn, [r[0] for r in deposit_lids], "client_deposit.json")

    log("Staging vendor deposit CSV records...")
    for f in csv_files:
        csv_lids = conn.execute(
            "SELECT landing_id FROM landing_raw WHERE source_file = ?", (f,)
        ).fetchall()
        stage_deposit_records(conn, [r[0] for r in csv_lids], f)

    # -------------------------------------------------------------------------
    section("STAGE 3 — TARGET (INSERT IF NOT EXISTS on deposit_id + deposit_date)")
    load_target_deposits(conn)

    # -------------------------------------------------------------------------
    section("STAGE 4 — CDC PROCESSING (watermark-based, LSN order)")
    process_cdc_events(conn)

    # -------------------------------------------------------------------------
    print_summary(conn)

    conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if "--reset" in sys.argv:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            print(f"Removed {DB_PATH} — starting fresh.")

    print(f"\nPipeline start: {now_utc()}")
    print(f"Database      : {DB_PATH}")
    print(f"Data directory: {DATA_DIR}")
    run_pipeline()
    print(f"\nPipeline end  : {now_utc()}")
