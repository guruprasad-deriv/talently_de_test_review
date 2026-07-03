"""
DQ check runner for deposit and trade data.

Covers: negative amounts, orphan client refs, null required fields,
fee/amount ratio anomalies, and schema conformance.

Run standalone:
    python dq_checks.py

All checks return a list of DQResult with severity and failing rows.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from typing import Any


EXPECTED_DEPOSIT_SCHEMA = {
    "deposit_id",
    "client_id",
    "amount_usd",
    "deposit_date",
    "method",
    "status",
    "fee_usd",
}

REQUIRED_DEPOSIT_FIELDS = {"deposit_id", "client_id", "amount_usd", "deposit_date"}

MAX_FEE_RATIO = 0.10  # fee > 10% of amount is anomalous


@dataclass
class DQResult:
    check_id: str
    check_name: str
    severity: str          # Critical | Warning | Info
    passed: bool
    failing_rows: list[dict[str, Any]] = field(default_factory=list)
    message: str = ""

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        count = len(self.failing_rows)
        return (
            f"[{self.severity:<8}] [{status}] {self.check_id}: {self.check_name}"
            + (f" — {count} row(s): {self.message}" if not self.passed else "")
        )


def check_negative_amounts(rows: list[dict]) -> DQResult:
    failing = [r for r in rows if float(r.get("amount_usd", 0) or 0) < 0]
    return DQResult(
        check_id="VDEP001",
        check_name="No negative deposit amounts",
        severity="Critical",
        passed=len(failing) == 0,
        failing_rows=failing,
        message=f"deposit_ids={[r['deposit_id'] for r in failing]}",
    )


def check_orphan_clients(rows: list[dict], known_client_ids: set[str]) -> DQResult:
    failing = [r for r in rows if r.get("client_id") not in known_client_ids]
    return DQResult(
        check_id="VDEP003",
        check_name="No orphan client references",
        severity="Critical",
        passed=len(failing) == 0,
        failing_rows=failing,
        message=f"unknown client_ids={[r['client_id'] for r in failing]}",
    )


def check_null_required_fields(rows: list[dict]) -> DQResult:
    failing = []
    for r in rows:
        missing = [f for f in REQUIRED_DEPOSIT_FIELDS if not r.get(f)]
        if missing:
            failing.append({**r, "_missing_fields": missing})
    return DQResult(
        check_id="VDEP004",
        check_name="Required fields are non-null",
        severity="Critical",
        passed=len(failing) == 0,
        failing_rows=failing,
        message=f"{len(failing)} row(s) missing required fields",
    )


def check_fee_ratio(rows: list[dict]) -> DQResult:
    failing = []
    for r in rows:
        amount = float(r.get("amount_usd", 0) or 0)
        fee = float(r.get("fee_usd", 0) or 0)
        if amount > 0 and fee / amount > MAX_FEE_RATIO:
            failing.append({**r, "_fee_ratio": round(fee / amount, 4)})
    return DQResult(
        check_id="VDEP010",
        check_name=f"Fee/amount ratio <= {MAX_FEE_RATIO:.0%}",
        severity="Warning",
        passed=len(failing) == 0,
        failing_rows=failing,
        message=f"ratios={[r['_fee_ratio'] for r in failing]}",
    )


def check_schema_conformance(rows: list[dict]) -> DQResult:
    if not rows:
        return DQResult(
            check_id="VDEP011",
            check_name="Schema matches expected deposit schema",
            severity="Warning",
            passed=True,
        )
    actual_cols = set(rows[0].keys())
    missing = EXPECTED_DEPOSIT_SCHEMA - actual_cols
    unexpected = actual_cols - EXPECTED_DEPOSIT_SCHEMA
    passed = not missing and not unexpected
    return DQResult(
        check_id="VDEP011",
        check_name="Schema matches expected deposit schema",
        severity="Warning",
        passed=passed,
        message=f"missing={missing}, unexpected={unexpected}",
    )


def run_all_checks(
    rows: list[dict],
    known_client_ids: set[str],
) -> list[DQResult]:
    return [
        check_negative_amounts(rows),
        check_orphan_clients(rows, known_client_ids),
        check_null_required_fields(rows),
        check_fee_ratio(rows),
        check_schema_conformance(rows),
    ]


# ---------------------------------------------------------------------------
# Demo / self-test
# ---------------------------------------------------------------------------

SAMPLE_CSV = """\
deposit_id,client_id,amount_usd,deposit_date,method,status,fee_usd
DEP001,CL001,1000.00,2024-01-15,bank_transfer,completed,5.00
DEP002,CL002,-50.00,2024-01-15,card,completed,2.50
DEP003,CL099,800.00,2024-01-16,bank_transfer,completed,4.00
DEP004,CL001,200.00,2024-01-16,card,completed,25.00
DEP005,,500.00,2024-01-17,bank_transfer,completed,3.00
"""

KNOWN_CLIENTS = {"CL001", "CL002", "CL003", "CL014", "CL019"}


def load_csv(raw: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(raw.strip()))
    return list(reader)


if __name__ == "__main__":
    rows = load_csv(SAMPLE_CSV)
    results = run_all_checks(rows, KNOWN_CLIENTS)

    print("=" * 65)
    print("DQ CHECK RESULTS")
    print("=" * 65)

    critical_failures = 0
    warning_failures = 0
    for r in results:
        print(r)
        if not r.passed:
            if r.severity == "Critical":
                critical_failures += 1
            elif r.severity == "Warning":
                warning_failures += 1

    print("=" * 65)
    print(
        f"Summary: {sum(1 for r in results if r.passed)}/{len(results)} passed | "
        f"{critical_failures} critical failure(s) | {warning_failures} warning(s)"
    )
    if critical_failures > 0:
        print("ACTION REQUIRED: Critical DQ failures — file must not be loaded.")
