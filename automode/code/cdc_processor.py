"""
CDC JSONL processor — sorts by LSN, applies events, demonstrates state chain.

Key design decisions:
  - Sort by lsn ASC before applying any events. CDC files arrive in delivery
    order (network/S3 flush), not LSN order. Applying out-of-order = corrupt state.
  - Soft delete: DELETE events set is_deleted=True + effective_to=commit_ts.
    The row is retained for regulatory audit trail (MiFID II / MAS).
  - SCD2 fork: UPDATE to tracked fields (risk_category, account_status) closes
    the current row (effective_to = commit_ts) and inserts a new row.
  - UPDATE to non-tracked fields (account_balance_usd) updates in-place (SCD1).

Run standalone:
    python cdc_processor.py
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

# Fields that require SCD2 historisation (compliance-critical)
SCD2_TRACKED_FIELDS = {"risk_category", "account_status"}

# ---------------------------------------------------------------------------
# Sample CDC JSONL — intentionally out of LSN order to demonstrate sorting
# ---------------------------------------------------------------------------

SAMPLE_CDC_JSONL = """\
{"lsn":1006,"commit_ts":"2024-01-15T10:35:00Z","operation":"UPDATE","table":"dim_client","record":{"client_id":"CL001","account_status":"under_review","account_balance_usd":1850}}
{"lsn":1004,"commit_ts":"2024-01-15T10:30:00Z","operation":"UPDATE","table":"dim_client","record":{"client_id":"CL001","risk_category":"high","account_balance_usd":1250}}
{"lsn":1005,"commit_ts":"2024-01-15T10:32:00Z","operation":"UPDATE","table":"dim_client","record":{"client_id":"CL001","account_balance_usd":1850}}
{"lsn":1010,"commit_ts":"2024-01-15T11:00:00Z","operation":"INSERT","table":"dim_client","record":{"client_id":"CL012","risk_category":"medium","account_status":"active","account_balance_usd":5200}}
{"lsn":1012,"commit_ts":"2024-01-15T11:05:00Z","operation":"DELETE","table":"dim_client","record":{"client_id":"CL012"}}
{"lsn":1008,"commit_ts":"2024-01-15T10:40:00Z","operation":"UPDATE","table":"dim_client","record":{"client_id":"CL014","account_balance_usd":12300}}
{"lsn":1009,"commit_ts":"2024-01-15T10:42:00Z","operation":"UPDATE","table":"dim_client","record":{"client_id":"CL014","risk_category":"medium"}}
"""

# ---------------------------------------------------------------------------
# State store (in-memory stand-in for BigQuery dim_client_scd)
# Each client_id → list of SCD rows, ordered by effective_from ASC
# ---------------------------------------------------------------------------

@dataclass
class SCDRow:
    client_id: str
    risk_category: str = "low"
    account_status: str = "active"
    account_balance_usd: float = 0.0
    effective_from: str = ""
    effective_to: str | None = None      # None = current row
    is_deleted: bool = False

    def to_dict(self) -> dict:
        return {
            "client_id": self.client_id,
            "risk_category": self.risk_category,
            "account_status": self.account_status,
            "account_balance_usd": self.account_balance_usd,
            "effective_from": self.effective_from,
            "effective_to": self.effective_to or "9999-12-31",
            "is_deleted": self.is_deleted,
        }


# client_id → list[SCDRow]
_warehouse: dict[str, list[SCDRow]] = {}

# Seed CL001 with a base row that pre-dates the CDC events
_warehouse["CL001"] = [
    SCDRow(
        client_id="CL001",
        risk_category="medium",
        account_status="active",
        account_balance_usd=1250.0,
        effective_from="2024-01-01T00:00:00Z",
    )
]


# ---------------------------------------------------------------------------
# Event processors
# ---------------------------------------------------------------------------

def _current_row(client_id: str) -> SCDRow | None:
    rows = _warehouse.get(client_id, [])
    for row in reversed(rows):
        if row.effective_to is None and not row.is_deleted:
            return row
    return None


def apply_insert(event: dict) -> None:
    rec = event["record"]
    client_id = rec["client_id"]
    commit_ts = event["commit_ts"]

    new_row = SCDRow(
        client_id=client_id,
        risk_category=rec.get("risk_category", "low"),
        account_status=rec.get("account_status", "active"),
        account_balance_usd=float(rec.get("account_balance_usd", 0)),
        effective_from=commit_ts,
    )
    _warehouse.setdefault(client_id, []).append(new_row)
    print(f"  INSERT  {client_id}: new row effective {commit_ts}")


def apply_update(event: dict) -> None:
    rec = event["record"]
    client_id = rec["client_id"]
    commit_ts = event["commit_ts"]
    lsn = event["lsn"]

    current = _current_row(client_id)
    if current is None:
        print(f"  WARN    LSN {lsn}: UPDATE for {client_id} but no current row — skipping")
        return

    changed_tracked = {
        f: rec[f] for f in SCD2_TRACKED_FIELDS if f in rec and rec[f] != getattr(current, f)
    }

    if changed_tracked:
        # SCD2: close current row, open new row
        current.effective_to = commit_ts

        new_row = SCDRow(
            client_id=client_id,
            risk_category=rec.get("risk_category", current.risk_category),
            account_status=rec.get("account_status", current.account_status),
            account_balance_usd=float(rec.get("account_balance_usd", current.account_balance_usd)),
            effective_from=commit_ts,
        )
        _warehouse[client_id].append(new_row)
        print(
            f"  SCD2    LSN {lsn}: {client_id} closed row @ {commit_ts}; "
            f"new row with changed: {changed_tracked}"
        )
    else:
        # SCD1: in-place update for non-tracked fields
        for k, v in rec.items():
            if k not in ("client_id",) and hasattr(current, k):
                setattr(current, k, float(v) if k == "account_balance_usd" else v)
        print(
            f"  SCD1    LSN {lsn}: {client_id} in-place update — "
            f"fields: {[k for k in rec if k != 'client_id']}"
        )


def apply_delete(event: dict) -> None:
    rec = event["record"]
    client_id = rec["client_id"]
    commit_ts = event["commit_ts"]
    lsn = event["lsn"]

    current = _current_row(client_id)
    if current is None:
        print(f"  WARN    LSN {lsn}: DELETE for {client_id} but no current row")
        return

    # Soft delete: close the current row, then INSERT a terminal "deleted" SCD row.
    # This preserves the full SCD2 chain — the terminal row shows the deleted state
    # with is_deleted=True and account_status='deleted', consistent with part1 §4
    # and part2 Q3 (CL012 delete walk-through).
    current.effective_to = commit_ts

    terminal = SCDRow(
        client_id=client_id,
        risk_category=current.risk_category,
        account_status="deleted",
        account_balance_usd=current.account_balance_usd,
        effective_from=commit_ts,
        effective_to=None,   # current (open) row
        is_deleted=True,
    )
    _warehouse[client_id].append(terminal)
    print(
        f"  DELETE  LSN {lsn}: {client_id} soft-deleted @ {commit_ts} "
        f"(current row closed; terminal 'deleted' SCD row inserted for regulatory audit)"
    )


HANDLERS = {
    "INSERT": apply_insert,
    "UPDATE": apply_update,
    "DELETE": apply_delete,
}


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------

def process_cdc_jsonl(raw: str) -> None:
    events = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if line:
            events.append(json.loads(line))

    # CRITICAL: sort by lsn ASC before applying.
    # File arrival order ≠ LSN order. Out-of-order application corrupts the
    # state chain (e.g. LSN 1006 applied before 1005 = wrong balance on delete).
    events.sort(key=lambda e: e["lsn"])
    print(f"Loaded {len(events)} CDC events, sorted by LSN ASC")
    print()

    for event in events:
        handler = HANDLERS.get(event["operation"])
        if handler:
            handler(event)
        else:
            print(f"  UNKNOWN operation: {event['operation']}")


def print_warehouse_state() -> None:
    print()
    print("=" * 70)
    print("FINAL WAREHOUSE STATE (dim_client_scd)")
    print("=" * 70)
    for client_id in sorted(_warehouse.keys()):
        rows = _warehouse[client_id]
        print(f"\n  {client_id} ({len(rows)} SCD row(s)):")
        for row in rows:
            d = row.to_dict()
            deleted_flag = " [SOFT-DELETED]" if row.is_deleted else ""
            current_flag = " ← CURRENT" if d["effective_to"] == "9999-12-31" else ""
            print(
                f"    risk={d['risk_category']:<8} "
                f"status={d['account_status']:<14} "
                f"balance={d['account_balance_usd']:>8.2f}  "
                f"from={d['effective_from'][:19]}  "
                f"to={d['effective_to'][:19]}"
                f"{deleted_flag}{current_flag}"
            )


if __name__ == "__main__":
    print("=" * 70)
    print("CDC PROCESSOR DEMO")
    print("=" * 70)
    print()
    process_cdc_jsonl(SAMPLE_CDC_JSONL)
    print_warehouse_state()

    print()
    print("=" * 70)
    print("LSN ORDER VALIDATION")
    print("=" * 70)
    print("CL001 state chain (should be: medium → high @ LSN1004 → under_review @ LSN1006):")
    for row in _warehouse.get("CL001", []):
        print(
            f"  risk={row.risk_category:<6} status={row.account_status:<14} "
            f"effective_from={row.effective_from[:19]}"
        )
    print()
    print("CL012 (soft-deleted): is_deleted flag should be True")
    for row in _warehouse.get("CL012", []):
        print(f"  is_deleted={row.is_deleted}  effective_to={row.effective_to}")
