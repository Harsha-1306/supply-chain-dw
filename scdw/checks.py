"""Automated data-quality checks. Critical failures stop the pipeline (quality gate); warnings are recorded
in AUDIT.DQ_RESULTS and surfaced on the Power BI data-quality page."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .contracts import Contract
from .warehouse import Warehouse, ident

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
CRITICAL, WARNING = "critical", "warning"


class QualityGateError(RuntimeError):
    def __init__(self, results: list[CheckResult]):
        failed = [r for r in results if r.status == FAIL]
        super().__init__("; ".join(f"{r.check_name}[{r.table_name}]: {r.details}" for r in failed))
        self.results = results


@dataclass
class CheckResult:
    check_name: str
    table_name: str
    severity: str
    status: str
    metric: float
    threshold: float
    details: str


def read_header(path: Path) -> list[str]:
    with open(path, newline="", encoding="utf-8") as fh:
        return [h.strip() for h in next(csv.reader(fh))]


# ---- before loading ---------------------------------------------------------
def check_schema(path: Path, contract: Contract) -> CheckResult:
    """CSV header vs contract. Missing columns block the load; extra columns are ignored with a warning."""
    if not path.exists():
        return CheckResult("schema", contract.table, CRITICAL, FAIL, 1, 0, f"file {path.name} not found")
    header = read_header(path)
    expected = contract.column_names
    missing = [c for c in expected if c not in header]
    extra = [c for c in header if c not in expected]
    dupes = sorted({c for c in header if header.count(c) > 1})
    if missing or dupes:
        details = []
        if missing:
            details.append(f"missing columns {missing}")
        if dupes:
            details.append(f"duplicate header columns {dupes}")
        if extra:
            details.append(f"unexpected columns {extra} (renamed?)")
        return CheckResult("schema", contract.table, CRITICAL, FAIL, len(missing) + len(dupes), 0, "; ".join(details))
    if extra:
        return CheckResult("schema", contract.table, WARNING, WARN, len(extra), 0,
                           f"unexpected columns {extra} ignored")
    return CheckResult("schema", contract.table, CRITICAL, PASS, 0, 0, f"{len(expected)} columns match contract")


# ---- during staging ---------------------------------------------------------
def check_reject_rate(contract: Contract, raw_rows: int, rejected: int) -> CheckResult:
    rate = rejected / raw_rows if raw_rows else 0.0
    status = FAIL if rate > contract.max_reject_rate else PASS
    return CheckResult("reject_rate", contract.table, CRITICAL, status, round(rate, 4), contract.max_reject_rate,
                       f"{rejected} of {raw_rows} rows rejected (see STAGING.REJECTED_ROWS)")


def check_duplicates_in_batch(contract: Contract, exact_dups: int, key_dups: int, raw_rows: int) -> CheckResult:
    """Informational: how many duplicates arrived. They are resolved by the MERGE, so this never fails."""
    return CheckResult("duplicates_in_batch", contract.table, WARNING, WARN if key_dups else PASS, key_dups, 0,
                       f"{exact_dups} exact duplicate rows, {key_dups} duplicate keys resolved "
                       f"(latest {contract.version_column or '_loaded_at'} wins) out of {raw_rows} rows")


def check_unique_keys(wh: Warehouse, contract: Contract) -> CheckResult:
    """Snowflake does not enforce PRIMARY KEY constraints, so uniqueness is asserted after every MERGE."""
    keys = ", ".join(contract.primary_key)
    dupes = wh.scalar(
        f"SELECT COUNT(*) FROM (SELECT {keys} FROM STAGING.{ident(contract.table)} "
        f"GROUP BY {keys} HAVING COUNT(*) > 1) d"
    )
    return CheckResult("unique_primary_key", contract.table, CRITICAL, FAIL if dupes else PASS, dupes, 0,
                       f"{dupes} duplicated keys on ({keys})")


def check_reconciliation(wh: Warehouse, contract: Contract) -> CheckResult:
    """Every valid key of the batch must be present in STAGING after the MERGE."""
    t = ident(contract.table)
    on = " AND ".join(f"s.{k} = w.{k}" for k in contract.primary_key)
    missing = wh.scalar(
        f"SELECT COUNT(*) FROM (SELECT DISTINCT {', '.join(contract.primary_key)} FROM STAGING._WORK_{t} "
        f"WHERE _reject_reason IS NULL) w LEFT JOIN STAGING.{t} s ON {on} WHERE s.{contract.primary_key[0]} IS NULL"
    )
    return CheckResult("reconciliation", contract.table, CRITICAL, FAIL if missing else PASS, missing, 0,
                       f"{missing} valid batch keys missing from STAGING after MERGE")


# ---- after CORE is built ----------------------------------------------------
def post_load_checks(wh: Warehouse, min_total_rows: int = 100_000) -> list[CheckResult]:
    results: list[CheckResult] = []

    def ratio_check(name, table, numerator_sql, denominator_sql, warn_at, fail_at, what):
        num = wh.scalar(numerator_sql) or 0
        den = wh.scalar(denominator_sql) or 0
        rate = num / den if den else 0.0
        status = FAIL if rate > fail_at else WARN if rate > warn_at else PASS
        results.append(CheckResult(name, table, CRITICAL if status == FAIL else WARNING, status, round(rate, 4),
                                   fail_at, f"{num} of {den} {what}"))

    ratio_check("orphan_supplier", "CORE.FACT_PO_LINE",
                "SELECT COUNT(*) FROM CORE.FACT_PO_LINE WHERE supplier_id = 'UNKNOWN'",
                "SELECT COUNT(*) FROM CORE.FACT_PO_LINE", 0.0, 0.01, "PO lines reference an unknown supplier")
    ratio_check("orphan_shipment", "CORE.FACT_SHIPMENT",
                "SELECT COUNT(*) FROM CORE.FACT_SHIPMENT WHERE is_orphan = 1",
                "SELECT COUNT(*) FROM CORE.FACT_SHIPMENT", 0.0, 0.01, "shipments reference an unknown PO line")
    ratio_check("delivered_before_shipped", "CORE.FACT_SHIPMENT",
                "SELECT COUNT(*) FROM CORE.FACT_SHIPMENT WHERE delivered_date < ship_date",
                "SELECT COUNT(*) FROM CORE.FACT_SHIPMENT", 0.0, 0.001, "shipments delivered before ship date")
    ratio_check("over_delivery", "CORE.FACT_PO_LINE",
                "SELECT COUNT(*) FROM CORE.FACT_PO_LINE WHERE qty_delivered > qty_ordered * 1.1",
                "SELECT COUNT(*) FROM CORE.FACT_PO_LINE", 0.0, 0.01, "PO lines over-delivered by more than 10%")

    null_keys = wh.scalar(
        "SELECT (SELECT COUNT(*) FROM CORE.FACT_PO_LINE WHERE po_id IS NULL OR po_line IS NULL) + "
        "(SELECT COUNT(*) FROM CORE.FACT_SHIPMENT WHERE shipment_id IS NULL) + "
        "(SELECT COUNT(*) FROM CORE.FACT_INVENTORY_SNAPSHOT WHERE snapshot_date IS NULL)"
    )
    results.append(CheckResult("fact_null_keys", "CORE", CRITICAL, FAIL if null_keys else PASS, null_keys, 0,
                               f"{null_keys} fact rows with NULL keys"))

    total = wh.scalar(
        "SELECT (SELECT COUNT(*) FROM STAGING.purchase_orders) + (SELECT COUNT(*) FROM STAGING.shipments) + "
        "(SELECT COUNT(*) FROM STAGING.inventory_snapshots) + (SELECT COUNT(*) FROM STAGING.suppliers) + "
        "(SELECT COUNT(*) FROM STAGING.products) + (SELECT COUNT(*) FROM STAGING.warehouses)"
    )
    results.append(CheckResult("volume", "STAGING", WARNING, PASS if total >= min_total_rows else WARN, total,
                               min_total_rows, f"{total:,} validated records in STAGING"))
    return results


def record(wh: Warehouse, run_id: str, batch_id: str, results: list[CheckResult]) -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    wh.insert_rows(
        "AUDIT.DQ_RESULTS",
        ["run_id", "batch_id", "check_name", "table_name", "severity", "status", "metric", "threshold", "details",
         "checked_at"],
        [(run_id, batch_id, r.check_name, r.table_name, r.severity, r.status, float(r.metric), float(r.threshold),
          r.details[:4000], now) for r in results],
    )


def gate(results: list[CheckResult]) -> None:
    if any(r.status == FAIL for r in results):
        raise QualityGateError(results)
