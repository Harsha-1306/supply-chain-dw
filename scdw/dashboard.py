"""Build a static, self-contained KPI dashboard (site/index.html) from the MART views.

It is a stand-in for the Power BI report that can be hosted anywhere (e.g. Vercel) without a Power BI licence.
The data is embedded as JSON, so the page has no backend and no external requests except web fonts.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from .warehouse import Warehouse

TEMPLATE = Path(__file__).resolve().parent / "templates" / "dashboard.html"


def _num(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime | date):
        return v.isoformat()
    return v


def _rows(wh: Warehouse, sql: str, cols: list[str]) -> list[dict]:
    return [{c: _num(v) for c, v in zip(cols, r, strict=True)} for r in wh.query(sql)]


def collect(wh: Warehouse) -> dict:
    kpi = wh.query("""
        SELECT
            AVG(CASE WHEN is_closed = 1 THEN is_otif * 1.0 END),
            AVG(CASE WHEN is_closed = 1 THEN is_on_time * 1.0 END),
            AVG(CASE WHEN is_closed = 1 THEN is_in_full * 1.0 END),
            AVG(CASE WHEN is_closed = 1 THEN lead_time_days * 1.0 END),
            SUM(line_value_eur),
            SUM(CASE WHEN is_closed = 0 THEN line_value_eur ELSE 0 END),
            SUM(is_closed),
            SUM(CASE WHEN is_closed = 1 THEN is_otif ELSE 0 END),
            COUNT(*)
        FROM CORE.FACT_PO_LINE""")[0]
    loads = wh.query("""SELECT SUM(raw_rows), SUM(exact_duplicate_rows), SUM(key_duplicate_rows), SUM(rejected_rows),
                               SUM(merged_keys), MAX(loaded_at), MAX(batch_id), MAX(run_id)
                        FROM AUDIT.LOAD_LOG WHERE status = 'LOADED'""")[0]
    layer_counts = {
        "RAW": wh.scalar("SELECT (SELECT COUNT(*) FROM RAW.purchase_orders) + (SELECT COUNT(*) FROM RAW.shipments) + "
                         "(SELECT COUNT(*) FROM RAW.inventory_snapshots) + (SELECT COUNT(*) FROM RAW.suppliers) + "
                         "(SELECT COUNT(*) FROM RAW.products) + (SELECT COUNT(*) FROM RAW.warehouses)"),
        "STAGING": int(loads[4] or 0),
        "CORE": wh.scalar("SELECT (SELECT COUNT(*) FROM CORE.FACT_PO_LINE) + (SELECT COUNT(*) FROM CORE.FACT_SHIPMENT) "
                          "+ (SELECT COUNT(*) FROM CORE.FACT_INVENTORY_SNAPSHOT)"),
        "MART": 6,
    }
    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "kpi": {
            "otif": _num(kpi[0]), "on_time": _num(kpi[1]), "in_full": _num(kpi[2]), "lead_time": _num(kpi[3]),
            "spend": _num(kpi[4]), "open_value": _num(kpi[5]), "closed_lines": int(kpi[6]), "otif_lines": int(kpi[7]),
            "po_lines": int(kpi[8]),
        },
        "load": {
            "raw_rows": int(loads[0] or 0), "exact_dups": int(loads[1] or 0), "key_dups": int(loads[2] or 0),
            "rejected": int(loads[3] or 0), "merged": int(loads[4] or 0), "loaded_at": _num(loads[5]),
            "batch_id": loads[6], "run_id": loads[7], "layers": layer_counts,
        },
        "otif_monthly": _rows(wh, """SELECT month, closed_lines, otif_rate, on_time_rate, in_full_rate,
                                            avg_lead_time_days, spend_eur
                                     FROM MART.V_KPI_OTIF_MONTHLY ORDER BY month""",
                              ["month", "lines", "otif", "on_time", "in_full", "lead", "spend"]),
        "suppliers": _rows(wh, """SELECT supplier_id, supplier_name, country, tier_name, closed_lines, otif_rate,
                                         on_time_rate, in_full_rate, avg_lead_time_days, spend_eur, rating
                                  FROM MART.V_KPI_SUPPLIER_SCORECARD ORDER BY spend_eur DESC""",
                           ["id", "name", "country", "tier", "lines", "otif", "on_time", "in_full", "lead", "spend",
                            "rating"]),
        "inventory": _rows(wh, """SELECT warehouse_name, AVG(stockout_rate), AVG(below_reorder_point_rate),
                                         SUM(CASE WHEN snapshot_date = (SELECT MAX(snapshot_date)
                                                  FROM MART.V_KPI_INVENTORY_HEALTH) THEN stock_value_eur END)
                                  FROM MART.V_KPI_INVENTORY_HEALTH GROUP BY warehouse_name
                                  ORDER BY 3 DESC""",
                           ["warehouse", "stockout", "below_rop", "stock_value"]),
        "inventory_by_category": _rows(wh, """SELECT category, AVG(stockout_rate), AVG(below_reorder_point_rate)
                                              FROM MART.V_KPI_INVENTORY_HEALTH GROUP BY category ORDER BY 3 DESC""",
                                       ["category", "stockout", "below_rop"]),
        "inbound": _rows(wh, """SELECT warehouse_name, SUM(deliveries), SUM(qty_received),
                                       SUM(late_delivery_rate * deliveries) / SUM(deliveries),
                                       SUM(avg_transit_days * deliveries) / SUM(deliveries)
                                FROM MART.V_KPI_WAREHOUSE_INBOUND GROUP BY warehouse_name ORDER BY 4 DESC""",
                         ["warehouse", "deliveries", "qty", "late", "transit"]),
        "dq": _rows(wh, """SELECT check_name, table_name, severity, status, metric, threshold, details
                           FROM MART.V_DQ_SCORECARD
                           ORDER BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
                                    check_name, table_name""",
                    ["check", "table", "severity", "status", "metric", "threshold", "details"]),
        "loads": _rows(wh, """SELECT table_name, raw_rows, exact_duplicate_rows, key_duplicate_rows, rejected_rows,
                                     merged_keys, status
                              FROM MART.V_LOAD_SUMMARY WHERE status = 'LOADED' ORDER BY raw_rows DESC""",
                       ["table", "raw", "exact", "keys", "rejected", "merged", "status"]),
        "rejects": _rows(wh, """SELECT table_name, reason, COUNT(*) FROM STAGING.REJECTED_ROWS
                                GROUP BY table_name, reason ORDER BY 3 DESC, 1, 2""",
                         ["table", "reason", "rows"]),
    }


def build_site(wh: Warehouse, out_dir: Path) -> Path:
    data = json.dumps(collect(wh), separators=(",", ":")).replace("</", "<\\/")
    html = TEMPLATE.read_text(encoding="utf-8").replace("/*__DATA__*/null", data)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "index.html"
    path.write_text(html, encoding="utf-8")
    return path
