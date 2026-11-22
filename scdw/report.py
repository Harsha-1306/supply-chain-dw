"""Export the MART views (Power BI import / offline review) and render a KPI preview image."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from .warehouse import Warehouse

VIEWS = ["V_KPI_OTIF_MONTHLY", "V_KPI_SUPPLIER_SCORECARD", "V_KPI_INVENTORY_HEALTH", "V_KPI_WAREHOUSE_INBOUND",
         "V_DQ_SCORECARD", "V_LOAD_SUMMARY"]


def export_marts(wh: Warehouse, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for view in VIEWS:
        cols = [r[0] for r in wh.query(
            f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'MART' "
            f"AND table_name = '{view}' ORDER BY ordinal_position")]
        rows = wh.query(f"SELECT * FROM MART.{view}")
        path = out_dir / f"{view.lower()}.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(cols)
            w.writerows(rows)
        written.append(path)
    return written


def render_preview(wh: Warehouse, path: Path) -> Path:
    """A static preview of the dashboard pages, rendered from the same MART views Power BI reads."""
    import matplotlib

    matplotlib.use("Agg")
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    import matplotlib.pyplot as plt

    navy, teal, amber, red, grey = "#1f3a5f", "#2a9d8f", "#e9a23b", "#d1495b", "#8d99ae"
    otif = wh.query("SELECT month, otif_rate, on_time_rate, in_full_rate, avg_lead_time_days "
                    "FROM MART.V_KPI_OTIF_MONTHLY ORDER BY month")
    otif = [(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in otif]
    sup = wh.query("SELECT supplier_name, otif_rate FROM MART.V_KPI_SUPPLIER_SCORECARD "
                   "WHERE closed_lines >= 50 ORDER BY otif_rate LIMIT 10")
    sup = [(r[0], float(r[1])) for r in sup]
    inv = wh.query("SELECT warehouse_name, AVG(stockout_rate), AVG(below_reorder_point_rate) "
                   "FROM MART.V_KPI_INVENTORY_HEALTH GROUP BY warehouse_name ORDER BY 3 DESC")
    inv = [(r[0], float(r[1]), float(r[2])) for r in inv]
    dq = wh.query("SELECT status, COUNT(*) FROM MART.V_DQ_SCORECARD GROUP BY status")
    loads = wh.query("SELECT SUM(raw_rows), SUM(exact_duplicate_rows), SUM(key_duplicate_rows), SUM(rejected_rows) "
                     "FROM MART.V_LOAD_SUMMARY WHERE status = 'LOADED'")[0]
    kpi = [float(v) for v in wh.query("SELECT AVG(is_otif*1.0), AVG(lead_time_days*1.0), SUM(line_value_eur) "
                                      "FROM CORE.FACT_PO_LINE WHERE is_closed = 1")[0]]

    fig = plt.figure(figsize=(16, 9), facecolor="white")
    fig.suptitle("Supply Chain KPI Dashboard - preview rendered from MART views", x=0.02, ha="left",
                 fontsize=16, fontweight="bold", color=navy)
    cards = [("OTIF", f"{kpi[0]:.1%}"), ("Avg lead time", f"{kpi[1]:.1f} days"), ("Spend", f"EUR {kpi[2] / 1e6:.1f}M"),
             ("Records processed", f"{int(loads[0]):,}"),
             ("Duplicates removed", f"{int(loads[2]):,}"), ("Rows rejected", f"{int(loads[3]):,}")]
    for i, (label, value) in enumerate(cards):
        ax = fig.add_axes([0.02 + i * 0.163, 0.80, 0.15, 0.1])
        ax.axis("off")
        ax.add_patch(plt.Rectangle((0, 0), 1, 1, color="#f1f4f8", transform=ax.transAxes))
        ax.text(0.06, 0.62, label, fontsize=10, color=grey, transform=ax.transAxes)
        ax.text(0.06, 0.18, value, fontsize=17, fontweight="bold", color=navy, transform=ax.transAxes)

    ax1 = fig.add_axes([0.05, 0.43, 0.42, 0.30])
    months = [r[0].strftime("%b %y") for r in otif]
    ax1.plot(months, [r[1] for r in otif], color=navy, lw=2.5, marker="o", label="OTIF")
    ax1.plot(months, [r[2] for r in otif], color=teal, lw=1.5, ls="--", label="On time")
    ax1.plot(months, [r[3] for r in otif], color=amber, lw=1.5, ls="--", label="In full")
    ax1.set_ylim(0.5, 1.0)
    ax1.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax1.set_title("OTIF by promised month", loc="left", fontweight="bold", color=navy)
    ax1.legend(frameon=False, ncol=3, loc="lower left")
    ax1.tick_params(axis="x", rotation=45, labelsize=8)
    for s in ("top", "right"):
        ax1.spines[s].set_visible(False)

    ax2 = fig.add_axes([0.62, 0.43, 0.35, 0.30])
    ax2.barh([r[0].replace(" GmbH", "") for r in sup], [r[1] for r in sup],
             color=[red if r[1] < 0.75 else amber for r in sup])
    ax2.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax2.set_title("10 weakest suppliers (OTIF, >= 50 lines)", loc="left", fontweight="bold", color=navy)
    ax2.tick_params(axis="y", labelsize=8)
    for s in ("top", "right"):
        ax2.spines[s].set_visible(False)

    ax3 = fig.add_axes([0.05, 0.06, 0.42, 0.28])
    names = [r[0].replace("DC ", "") for r in inv]
    x = range(len(names))
    ax3.bar([i - 0.2 for i in x], [r[2] for r in inv], width=0.4, color=amber, label="Below reorder point")
    ax3.bar([i + 0.2 for i in x], [r[1] for r in inv], width=0.4, color=red, label="Stock-out")
    ax3.set_xticks(list(x), names, rotation=45, fontsize=8)
    ax3.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax3.set_title("Inventory health by warehouse (avg of daily snapshots)", loc="left", fontweight="bold", color=navy)
    ax3.set_ylim(0, max(r[2] for r in inv) * 1.45)
    ax3.legend(frameon=False, ncol=2, loc="upper left")
    for s in ("top", "right"):
        ax3.spines[s].set_visible(False)

    ax4 = fig.add_axes([0.62, 0.06, 0.35, 0.28])
    order = {"PASS": teal, "WARN": amber, "FAIL": red}
    dq_sorted = sorted(dq, key=lambda r: list(order).index(r[0]))
    ax4.bar([r[0] for r in dq_sorted], [r[1] for r in dq_sorted], color=[order[r[0]] for r in dq_sorted])
    ax4.set_title("Data-quality checks (latest run)", loc="left", fontweight="bold", color=navy)
    for s in ("top", "right"):
        ax4.spines[s].set_visible(False)

    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=110)
    plt.close(fig)
    return path
