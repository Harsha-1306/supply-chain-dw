"""Pipeline steps. Each step is a plain function so Airflow tasks, the CLI and the tests call the same code."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from . import checks, staging
from .contracts import load_contracts
from .warehouse import Warehouse, literal

log = logging.getLogger("scdw")
SQL_DIR = Path(__file__).resolve().parent / "sql"
# Master data first so the log reads in dependency order.
LOAD_ORDER = ["suppliers", "products", "warehouses", "purchase_orders", "shipments", "inventory_snapshots"]


def batch_id_for(path: Path) -> str:
    bid = re.sub(r"[^A-Za-z0-9_]", "_", path.name)
    if not bid:
        raise ValueError(f"cannot derive batch id from {path}")
    return bid


def new_run_id() -> str:
    return datetime.now(UTC).strftime("run_%Y%m%dT%H%M%S%f")


def md5(path: Path) -> str:
    h = hashlib.md5(usedforsecurity=False)
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---- 0. setup -----------------------------------------------------------------
def setup(wh: Warehouse) -> None:
    wh.run_script(staging.AUDIT_DDL)
    loaded_at = "TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()" if wh.name == "snowflake" else "TIMESTAMP"
    for c in load_contracts().values():
        wh.execute(staging.raw_ddl(c, loaded_at))
        wh.execute(staging.staging_ddl(c))


# ---- 1. schema gate -------------------------------------------------------------
def check_schemas(wh: Warehouse, batch_dir: Path, run_id: str) -> list[checks.CheckResult]:
    """Check every file of the batch before loading any of them: a batch is loaded completely or not at all."""
    contracts = load_contracts()
    results = [checks.check_schema(batch_dir / contracts[t].file, contracts[t]) for t in LOAD_ORDER]
    checks.record(wh, run_id, batch_id_for(batch_dir), results)
    checks.gate(results)
    return results


# ---- 2. land into RAW -----------------------------------------------------------
def ingest(wh: Warehouse, batch_dir: Path, run_id: str) -> dict[str, dict]:
    batch_id = batch_id_for(batch_dir)
    contracts = load_contracts()
    manifest: dict[str, dict] = {}
    for table in LOAD_ORDER:
        c = contracts[table]
        path = batch_dir / c.file
        digest = md5(path)
        already = wh.scalar(
            f"SELECT COUNT(*) FROM AUDIT.LOAD_LOG WHERE file_md5 = {literal(digest)} AND status = 'LOADED'"
        )
        if already:
            log.info("skip %s: identical file already loaded", path.name)
            manifest[table] = {"file": path.name, "md5": digest, "rows": 0, "skipped": True}
            continue
        wh.execute(f"DELETE FROM RAW.{table} WHERE _batch_id = {literal(batch_id)}")  # safe re-run of a task
        rows = wh.load_csv(path, table, checks.read_header(path), c.column_names, batch_id)
        manifest[table] = {"file": path.name, "md5": digest, "rows": rows, "skipped": False}
        log.info("RAW.%s <- %s (%s rows)", table, path.name, f"{rows:,}")
    return manifest


# ---- 3. RAW -> STAGING: type, reject, de-duplicate, MERGE --------------------------
def transform_staging(wh: Warehouse, batch_id: str, run_id: str, manifest: dict[str, dict]) -> dict[str, dict]:
    contracts = load_contracts()
    summary: dict[str, dict] = {}
    for table in LOAD_ORDER:
        info = manifest.get(table, {})
        if info.get("skipped"):
            continue
        c = contracts[table]
        wh.execute(staging.work_table_sql(c, batch_id))
        raw_rows, exact_dups, key_dups, rejected, distinct_keys = wh.query(staging.batch_metrics_sql(c))[0]

        results = [checks.check_reject_rate(c, raw_rows, rejected),
                   checks.check_duplicates_in_batch(c, exact_dups, key_dups, raw_rows)]
        wh.execute(staging.reject_insert_sql(c))
        if any(r.status == checks.FAIL for r in results):
            _log_load(wh, run_id, batch_id, table, info, raw_rows, exact_dups, key_dups, rejected, 0,
                      "FAILED_QUALITY_GATE")
            checks.record(wh, run_id, batch_id, results)
            wh.execute(staging.drop_work_sql(c))
            checks.gate(results)

        wh.execute(staging.merge_sql(c))
        results += [checks.check_unique_keys(wh, c), checks.check_reconciliation(wh, c)]
        checks.record(wh, run_id, batch_id, results)
        status = "FAILED_QUALITY_GATE" if any(r.status == checks.FAIL for r in results) else "LOADED"
        _log_load(wh, run_id, batch_id, table, info, raw_rows, exact_dups, key_dups, rejected, distinct_keys, status)
        wh.execute(staging.drop_work_sql(c))
        checks.gate(results)
        summary[table] = {"raw_rows": raw_rows, "exact_duplicate_rows": exact_dups, "key_duplicate_rows": key_dups,
                          "rejected_rows": rejected, "merged_keys": distinct_keys}
        log.info("STAGING.%s: %s", table, summary[table])
    return summary


def _log_load(wh, run_id, batch_id, table, info, raw_rows, exact, key, rejected, merged, status) -> None:
    wh.insert_rows(
        "AUDIT.LOAD_LOG",
        ["run_id", "batch_id", "table_name", "source_file", "file_md5", "raw_rows", "exact_duplicate_rows",
         "key_duplicate_rows", "rejected_rows", "merged_keys", "status", "loaded_at"],
        [(run_id, batch_id, table, info.get("file"), info.get("md5"), int(raw_rows), int(exact), int(key),
          int(rejected), int(merged), status, datetime.now(UTC).replace(tzinfo=None))],
    )


# ---- 4. CORE star schema ----------------------------------------------------------
def build_core(wh: Warehouse) -> None:
    wh.run_script((SQL_DIR / "core" / "01_dimensions.sql").read_text())
    build_dim_date(wh)
    wh.run_script((SQL_DIR / "core" / "02_facts.sql").read_text())


def build_dim_date(wh: Warehouse) -> None:
    lo, hi = wh.query("SELECT MIN(order_date), MAX(promised_date) FROM STAGING.purchase_orders")[0]
    lo = lo or date.today()
    hi = max(hi or lo, date.today()) + timedelta(days=90)
    start = date(lo.year, 1, 1)
    wh.execute("""CREATE OR REPLACE TABLE CORE.DIM_DATE (
        date_key INTEGER, date_day DATE, year INTEGER, quarter INTEGER, month INTEGER, month_name VARCHAR,
        year_month VARCHAR, iso_week INTEGER, day_of_week INTEGER, day_name VARCHAR, is_weekend INTEGER)""")
    rows, d = [], start
    while d <= hi:
        rows.append((int(d.strftime("%Y%m%d")), d, d.year, (d.month - 1) // 3 + 1, d.month, d.strftime("%B"),
                     d.strftime("%Y-%m"), d.isocalendar().week, d.isoweekday(), d.strftime("%A"),
                     int(d.isoweekday() >= 6)))
        d += timedelta(days=1)
    wh.insert_rows("CORE.DIM_DATE", ["date_key", "date_day", "year", "quarter", "month", "month_name", "year_month",
                                     "iso_week", "day_of_week", "day_name", "is_weekend"], rows)


# ---- 5. MART views + post-load checks -------------------------------------------------
def build_marts(wh: Warehouse) -> None:
    wh.run_script((SQL_DIR / "mart" / "01_kpi_views.sql").read_text())


def run_post_load_checks(wh: Warehouse, run_id: str, batch_id: str) -> list[checks.CheckResult]:
    results = checks.post_load_checks(wh)
    checks.record(wh, run_id, batch_id, results)
    checks.gate(results)
    return results


# ---- everything --------------------------------------------------------------------------
def run(wh: Warehouse, batch_dir: Path, run_id: str | None = None) -> dict:
    run_id = run_id or new_run_id()
    batch_id = batch_id_for(batch_dir)
    setup(wh)
    check_schemas(wh, batch_dir, run_id)
    manifest = ingest(wh, batch_dir, run_id)
    summary = transform_staging(wh, batch_id, run_id, manifest)
    build_core(wh)
    build_marts(wh)
    post = run_post_load_checks(wh, run_id, batch_id)
    return {"run_id": run_id, "batch_id": batch_id, "tables": summary,
            "post_checks": {r.check_name: r.status for r in post}}
