"""
Vendor CSV ingestion prototype — idempotent file-level + row-level dedup.

Design decisions:
  - File manifest keyed on (filename, md5_content_hash) prevents re-processing
    the same file even if it is re-delivered with the same name.
  - Row-level dedup uses MERGE on deposit_id — natural business key.
  - Schema normalisation handles the method → payment_method drift seen in
    the sample data without failing the pipeline.
  - DQ checks (negative amount, orphan client) run before any rows are loaded.
  - Soft-deletes: not applicable to vendor CSV (CDC handles deletes separately).

Run standalone (uses embedded sample data, no external deps):
    python vendor_ingestion.py
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema normalisation map
# Handles known field rename: vendor sometimes sends "method", sometimes
# "payment_method". Both are accepted; output is always "payment_method".
# ---------------------------------------------------------------------------
FIELD_ALIASES: dict[str, str] = {
    "method": "payment_method",
}

REQUIRED_FIELDS = {"deposit_id", "client_id", "amount_usd", "deposit_date"}


# ---------------------------------------------------------------------------
# In-memory stand-ins for warehouse tables
# ---------------------------------------------------------------------------
_file_manifest: dict[tuple[str, str], dict] = {}  # (filename, md5) → manifest row
_deposit_rows: dict[str, dict] = {}               # deposit_id → latest row
_known_clients: set[str] = {"CL001", "CL002", "CL003", "CL014", "CL019", "CL020"}


@dataclass
class IngestionResult:
    filename: str
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_rejected: int = 0
    skipped_duplicate_file: bool = False
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# File manifest check
# ---------------------------------------------------------------------------

def _md5(content: str) -> str:
    return hashlib.md5(content.encode()).hexdigest()


def is_already_processed(filename: str, content_hash: str) -> bool:
    return (filename, content_hash) in _file_manifest


def record_file_processed(filename: str, content_hash: str, row_count: int) -> None:
    _file_manifest[(filename, content_hash)] = {
        "filename": filename,
        "content_hash": content_hash,
        "row_count": row_count,
        "processed_at": datetime.utcnow().isoformat(),
    }
    logger.info("Manifest: recorded %s (%s, %d rows)", filename, content_hash[:8], row_count)


# ---------------------------------------------------------------------------
# Schema normalisation
# ---------------------------------------------------------------------------

def normalise_row(raw: dict[str, str]) -> dict[str, str]:
    normalised = {}
    for k, v in raw.items():
        canonical_key = FIELD_ALIASES.get(k.strip().lower(), k.strip().lower())
        normalised[canonical_key] = v.strip()
    return normalised


# ---------------------------------------------------------------------------
# DQ checks (pre-load)
# ---------------------------------------------------------------------------

def validate_row(row: dict, row_num: int) -> list[str]:
    errors: list[str] = []
    deposit_id = row.get("deposit_id", "")

    # VDEP004: required fields
    for f in REQUIRED_FIELDS:
        if not row.get(f):
            errors.append(f"Row {row_num} [{deposit_id}]: missing required field '{f}' (VDEP004)")

    # VDEP001: negative amount
    try:
        amount = float(row.get("amount_usd", 0) or 0)
        if amount < 0:
            errors.append(
                f"Row {row_num} [{deposit_id}]: negative amount_usd={amount} (VDEP001)"
            )
    except ValueError:
        errors.append(f"Row {row_num} [{deposit_id}]: non-numeric amount_usd (VDEP001)")

    # VDEP003: orphan client
    client_id = row.get("client_id", "")
    if client_id and client_id not in _known_clients:
        errors.append(
            f"Row {row_num} [{deposit_id}]: client_id={client_id} not in dim_client (VDEP003)"
        )

    return errors


# ---------------------------------------------------------------------------
# Row-level MERGE (upsert on deposit_id)
# ---------------------------------------------------------------------------

def merge_row(row: dict) -> str:
    deposit_id = row["deposit_id"]
    if deposit_id in _deposit_rows:
        _deposit_rows[deposit_id] = {**_deposit_rows[deposit_id], **row}
        return "updated"
    _deposit_rows[deposit_id] = row
    return "inserted"


# ---------------------------------------------------------------------------
# Main ingestion function
# ---------------------------------------------------------------------------

def ingest_vendor_file(filename: str, raw_csv: str) -> IngestionResult:
    result = IngestionResult(filename=filename)
    content_hash = _md5(raw_csv)

    if is_already_processed(filename, content_hash):
        logger.warning("Skipping %s — already processed (idempotent)", filename)
        result.skipped_duplicate_file = True
        return result

    reader = csv.DictReader(io.StringIO(raw_csv.strip()))
    rows_to_merge: list[dict] = []
    all_errors: list[str] = []

    for row_num, raw_row in enumerate(reader, start=1):
        result.rows_read += 1
        row = normalise_row(raw_row)
        row_errors = validate_row(row, row_num)
        if row_errors:
            all_errors.extend(row_errors)
            result.rows_rejected += 1
            continue
        rows_to_merge.append(row)

    if all_errors:
        result.errors = all_errors
        logger.error(
            "%s: %d DQ error(s) — %d row(s) rejected",
            filename, len(all_errors), result.rows_rejected,
        )
        for e in all_errors:
            logger.error("  %s", e)

    for row in rows_to_merge:
        action = merge_row(row)
        if action == "inserted":
            result.rows_inserted += 1
        else:
            result.rows_updated += 1

    record_file_processed(filename, content_hash, result.rows_read)
    logger.info(
        "%s: read=%d inserted=%d updated=%d rejected=%d",
        filename, result.rows_read, result.rows_inserted,
        result.rows_updated, result.rows_rejected,
    )
    return result


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

SAMPLE_FILE_1 = """\
deposit_id,client_id,amount_usd,deposit_date,method,status,fee_usd
DEP001,CL001,1000.00,2024-01-15,bank_transfer,completed,5.00
DEP002,CL002,500.00,2024-01-15,card,completed,2.50
DEP003,CL099,800.00,2024-01-16,bank_transfer,completed,4.00
DEP004,CL001,-200.00,2024-01-16,card,completed,0.00
"""

SAMPLE_FILE_2 = """\
deposit_id,client_id,amount_usd,deposit_date,payment_method,status,fee_usd
DEP001,CL001,1000.00,2024-01-15,bank_transfer,completed,5.00
DEP005,CL003,750.00,2024-01-17,e_wallet,completed,3.75
"""

if __name__ == "__main__":
    print("=" * 60)
    print("VENDOR INGESTION DEMO")
    print("=" * 60)

    print("\n--- Pass 1: file with schema 'method', orphan CL099, negative DEP004 ---")
    r1 = ingest_vendor_file("vendor_20240115.csv", SAMPLE_FILE_1)
    print(f"Result: {r1.rows_read} read, {r1.rows_inserted} inserted, {r1.rows_rejected} rejected")

    print("\n--- Pass 2: file with schema 'payment_method' (drift) ---")
    r2 = ingest_vendor_file("vendor_20240117.csv", SAMPLE_FILE_2)
    print(f"Result: {r2.rows_read} read, {r2.rows_inserted} inserted, {r2.rows_updated} updated")

    print("\n--- Pass 3: re-deliver Pass 1 file (idempotency) ---")
    r3 = ingest_vendor_file("vendor_20240115.csv", SAMPLE_FILE_1)
    print(f"Result: skipped_duplicate_file={r3.skipped_duplicate_file}")

    print("\n--- Final warehouse state ---")
    for dep_id, row in sorted(_deposit_rows.items()):
        print(f"  {dep_id}: client={row['client_id']} amount={row['amount_usd']}")
