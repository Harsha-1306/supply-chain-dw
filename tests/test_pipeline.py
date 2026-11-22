"""End-to-end pipeline on DuckDB with >100k records and injected data problems."""

import csv
import shutil

import pytest

from scdw import pipeline
from scdw.checks import QualityGateError
from scdw.generate import generate


def test_volume_over_100k(loaded):
    _, result, stats, _ = loaded
    assert stats.total_rows > 100_000
    assert result["post_checks"]["volume"] == "PASS"


@pytest.mark.parametrize("table", ["purchase_orders", "shipments", "inventory_snapshots", "products"])
def test_exact_duplicates_detected(loaded, table):
    _, result, stats, _ = loaded
    assert result["tables"][table]["exact_duplicate_rows"] == stats.exact_duplicates[table]


@pytest.mark.parametrize("table", ["purchase_orders", "shipments"])
def test_superseded_versions_resolved(loaded, table):
    _, result, stats, _ = loaded
    t = result["tables"][table]
    assert t["key_duplicate_rows"] == stats.exact_duplicates[table] + stats.superseded_versions[table]


@pytest.mark.parametrize("table", ["purchase_orders", "shipments"])
def test_invalid_rows_rejected_with_reason(loaded, table):
    wh, result, stats, _ = loaded
    assert result["tables"][table]["rejected_rows"] == stats.invalid_rows[table]
    no_reason = wh.scalar(f"SELECT COUNT(*) FROM STAGING.REJECTED_ROWS WHERE table_name = '{table}' "
                          "AND (reason IS NULL OR reason = '')")
    assert no_reason == 0


def test_row_accounting_reconciles(loaded):
    wh, result, _, _ = loaded
    for table, t in result["tables"].items():
        assert t["raw_rows"] == t["merged_keys"] + t["key_duplicate_rows"] + t["rejected_rows"], table
        assert wh.scalar(f"SELECT COUNT(*) FROM STAGING.{table}") == t["merged_keys"], table


def test_staging_has_unique_keys(loaded):
    wh, *_ = loaded
    assert wh.scalar("SELECT COUNT(*) FROM (SELECT po_id, po_line FROM STAGING.purchase_orders "
                     "GROUP BY 1, 2 HAVING COUNT(*) > 1) d") == 0
    assert wh.scalar("SELECT COUNT(*) FROM (SELECT shipment_id FROM STAGING.shipments "
                     "GROUP BY 1 HAVING COUNT(*) > 1) d") == 0


def test_newest_version_wins(loaded):
    """Shipments that arrived as IN_TRANSIT and DELIVERED in the same extract must end up DELIVERED."""
    wh, _, _, batch = loaded
    statuses: dict[str, set] = {}
    with open(batch / "shipments.csv", newline="") as fh:
        for row in csv.DictReader(fh):
            statuses.setdefault(row["shipment_id"], set()).add(row["status"])
    both = [k for k, v in statuses.items() if v == {"IN_TRANSIT", "DELIVERED"}]
    assert both
    ids = ", ".join(f"'{k}'" for k in both)
    assert wh.scalar(f"SELECT COUNT(*) FROM STAGING.shipments WHERE shipment_id IN ({ids}) "
                     "AND status <> 'DELIVERED'") == 0


def test_orphans_flagged_not_dropped(loaded):
    wh, result, stats, _ = loaded
    assert result["post_checks"]["orphan_supplier"] == "WARN"
    assert result["post_checks"]["orphan_shipment"] == "WARN"
    assert wh.scalar("SELECT COUNT(*) FROM CORE.FACT_PO_LINE WHERE supplier_id = 'UNKNOWN'") == \
        stats.orphan_po_suppliers
    assert wh.scalar("SELECT COUNT(*) FROM CORE.FACT_SHIPMENT WHERE is_orphan = 1") == stats.orphan_shipments


def test_kpis_are_sane(loaded):
    wh, *_ = loaded
    rows = wh.query("SELECT otif_rate, on_time_rate, in_full_rate, avg_lead_time_days FROM MART.V_KPI_OTIF_MONTHLY")
    assert rows
    for otif, on_time, in_full, lead in rows:
        assert 0 <= otif <= min(on_time, in_full) <= 1
        assert lead > 0
    ratings = {r[0] for r in wh.query("SELECT DISTINCT rating FROM MART.V_KPI_SUPPLIER_SCORECARD")}
    assert ratings <= {"A", "B", "C"}
    assert wh.scalar("SELECT COUNT(*) FROM MART.V_KPI_INVENTORY_HEALTH WHERE stockout_rate > 1") == 0


def test_dq_scorecard_has_every_check(loaded):
    wh, *_ = loaded
    names = {r[0] for r in wh.query("SELECT DISTINCT check_name FROM MART.V_DQ_SCORECARD")}
    assert {"schema", "reject_rate", "duplicates_in_batch", "unique_primary_key", "reconciliation",
            "orphan_supplier", "fact_null_keys", "volume"} <= names
    assert wh.scalar("SELECT COUNT(*) FROM MART.V_DQ_SCORECARD WHERE status = 'FAIL'") == 0


def test_same_file_is_not_loaded_twice(loaded, tmp_path):
    """Re-delivered extracts (same MD5) are skipped, so a re-run cannot double count."""
    wh, _, _, batch = loaded
    copy = tmp_path / "batch_001_resent"
    shutil.copytree(batch, copy)
    before = wh.scalar("SELECT COUNT(*) FROM STAGING.purchase_orders")
    result = pipeline.run(wh, copy, run_id="run_resent")
    assert result["tables"] == {}
    assert wh.scalar("SELECT COUNT(*) FROM STAGING.purchase_orders") == before


def test_incremental_batch_merges(fresh_wh, tmp_path):
    first = tmp_path / "b1"
    generate(first, po_lines=2_000, seed=1, issue_rate=0)
    pipeline.run(fresh_wh, first)
    n1 = fresh_wh.scalar("SELECT COUNT(*) FROM STAGING.purchase_orders")
    second = tmp_path / "b2"
    generate(second, po_lines=2_000, seed=1, issue_rate=1)  # same keys + newer versions + extra problems
    result = pipeline.run(fresh_wh, second)
    assert fresh_wh.scalar("SELECT COUNT(*) FROM STAGING.purchase_orders") == n1  # updated, not duplicated
    assert result["tables"]["purchase_orders"]["rejected_rows"] > 0


def test_schema_drift_blocks_whole_batch(fresh_wh, tmp_path):
    batch = tmp_path / "drift"
    generate(batch, po_lines=500, schema_drift=True)
    with pytest.raises(QualityGateError) as exc:
        pipeline.run(fresh_wh, batch)
    assert "delivered_date" in str(exc.value) and "delivery_date" in str(exc.value)
    # all-or-nothing: no table of the batch was loaded
    assert fresh_wh.scalar("SELECT COUNT(*) FROM RAW.purchase_orders") == 0
    assert fresh_wh.scalar("SELECT status FROM AUDIT.DQ_RESULTS WHERE check_name = 'schema' "
                           "AND table_name = 'shipments'") == "FAIL"


def test_reject_rate_gate_keeps_staging_clean(fresh_wh, tmp_path):
    batch = tmp_path / "garbage"
    generate(batch, po_lines=500, issue_rate=0)
    path = batch / "purchase_orders.csv"
    rows = list(csv.DictReader(open(path, newline="")))
    for r in rows[: len(rows) // 5]:
        r["qty_ordered"] = "n/a"
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    with pytest.raises(QualityGateError, match="reject_rate"):
        pipeline.run(fresh_wh, batch)
    assert fresh_wh.scalar("SELECT COUNT(*) FROM STAGING.purchase_orders") == 0
    assert fresh_wh.scalar("SELECT status FROM AUDIT.LOAD_LOG WHERE table_name = 'purchase_orders'") == \
        "FAILED_QUALITY_GATE"
