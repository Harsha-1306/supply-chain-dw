"""Synthetic supply-chain source extracts (ERP / WMS / TMS style CSVs) with realistic data problems.

Deterministic for a given seed, so tests can assert exact counts. Injected problems:
  * exact duplicate rows            (re-sent extract lines)
  * superseded versions             (same key, later updated_at: e.g. IN_TRANSIT -> DELIVERED)
  * invalid rows                    (non-numeric qty, negative qty, impossible date, bad currency, missing key)
  * orphans                         (shipments for unknown POs, PO lines for unknown suppliers)
  * schema drift (optional)         (a renamed column in the shipments feed)
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from .contracts import load_contracts

COUNTRIES = ["DE", "NL", "PL", "CZ", "FR", "IT", "CN", "IN", "US", "TR"]
CITIES = [("Koblenz", "DE"), ("Hamburg", "DE"), ("Duisburg", "DE"), ("Leipzig", "DE"), ("Rotterdam", "NL"),
          ("Venlo", "NL"), ("Poznan", "PL"), ("Prague", "CZ"), ("Lyon", "FR"), ("Milan", "IT"),
          ("Vienna", "AT"), ("Antwerp", "BE")]
CATEGORIES = ["Electronics", "Packaging", "Spare Parts", "Raw Materials", "Chemicals", "Office", "Tools"]
CARRIERS = ["DHL", "DB Schenker", "Kuehne+Nagel", "DSV", "UPS", "Dachser"]
CURRENCIES = ["EUR"] * 7 + ["USD"] * 2 + ["GBP"]


@dataclass
class GenerationStats:
    rows: dict[str, int] = field(default_factory=dict)
    exact_duplicates: dict[str, int] = field(default_factory=dict)
    superseded_versions: dict[str, int] = field(default_factory=dict)
    invalid_rows: dict[str, int] = field(default_factory=dict)
    orphan_shipments: int = 0
    orphan_po_suppliers: int = 0
    schema_drift: bool = False

    @property
    def total_rows(self) -> int:
        return sum(self.rows.values())


def _ts(d: date, rng: random.Random) -> str:
    return (datetime.combine(d, datetime.min.time()) + timedelta(minutes=rng.randint(0, 1439))).isoformat(sep=" ")


def generate(out_dir: Path, *, po_lines: int = 55_000, seed: int = 42, end_date: date = date(2026, 11, 15),
             inventory_days: int = 45, schema_drift: bool = False, issue_rate: float = 1.0) -> GenerationStats:
    """Write one batch of source files to out_dir. issue_rate=0 produces perfectly clean data."""
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    stats = GenerationStats(schema_drift=schema_drift)
    start = end_date - timedelta(days=320)

    # ---- master data ------------------------------------------------------
    n_sup, n_prod = max(20, po_lines // 220), max(50, po_lines // 22)
    suppliers = [
        {"supplier_id": f"SUP-{i:04d}", "supplier_name": f"Supplier {i:04d} GmbH", "country": rng.choice(COUNTRIES),
         "tier": str(rng.choices([1, 2, 3], [0.3, 0.5, 0.2])[0]), "updated_at": _ts(start, rng)}
        for i in range(1, n_sup + 1)
    ]
    reliability = {s["supplier_id"]: rng.uniform(0.70, 0.99) for s in suppliers}
    products = [
        {"sku": f"SKU-{i:05d}", "product_name": f"Item {i:05d}", "category": rng.choice(CATEGORIES),
         "unit_cost": f"{min(2500.0, rng.lognormvariate(2.6, 1.1)):.2f}",
         "uom": rng.choice(["EA", "EA", "BOX", "PAL", "KG"]),
         "updated_at": _ts(start, rng)}
        for i in range(1, n_prod + 1)
    ]
    warehouses = [
        {"warehouse_id": f"WH-{i:02d}", "warehouse_name": f"DC {city}", "city": city, "country": country,
         "capacity_units": str(rng.randint(20, 120) * 1000), "updated_at": _ts(start, rng)}
        for i, (city, country) in enumerate(CITIES, start=1)
    ]

    # ---- purchase orders & shipments --------------------------------------
    pos, shipments = [], []
    po_no, line_in_po, po_header = 0, 0, None
    ship_no = 0
    for _ in range(po_lines):
        if po_header is None or line_in_po >= po_header["lines"]:
            po_no += 1
            line_in_po = 0
            order_date = start + timedelta(days=rng.randint(0, 300))
            po_header = {
                "po_id": f"PO-{po_no:06d}", "supplier_id": rng.choice(suppliers)["supplier_id"],
                "warehouse_id": rng.choice(warehouses)["warehouse_id"], "order_date": order_date,
                "promised_date": order_date + timedelta(days=rng.randint(5, 35)),
                "currency": rng.choice(CURRENCIES), "lines": rng.randint(1, 8),
            }
        line_in_po += 1
        product = rng.choice(products)
        qty = rng.randint(1, 500)
        po = {
            "po_id": po_header["po_id"], "po_line": str(line_in_po), "supplier_id": po_header["supplier_id"],
            "sku": product["sku"], "warehouse_id": po_header["warehouse_id"],
            "order_date": po_header["order_date"].isoformat(), "promised_date": po_header["promised_date"].isoformat(),
            "qty_ordered": str(qty), "unit_price": f"{float(product['unit_cost']) * rng.uniform(1.05, 1.4):.2f}",
            "currency": po_header["currency"], "updated_at": _ts(po_header["order_date"], rng),
        }
        pos.append(po)

        promised = po_header["promised_date"]
        on_time = rng.random() < reliability[po_header["supplier_id"]]
        delivered = promised - timedelta(days=rng.randint(0, 4)) if on_time else promised + timedelta(
            days=rng.randint(1, 12))
        roll = rng.random()
        if delivered > end_date or roll < 0.04:
            # not shipped yet / in transit
            if roll < 0.02:
                continue
            ship_no += 1
            shipments.append(_shipment(ship_no, po, rng.choice(CARRIERS), promised - timedelta(days=3), None, qty,
                                       "IN_TRANSIT", rng))
            continue
        parts = 2 if rng.random() < 0.12 else 1
        short = rng.random() < 0.06  # supplier delivers less than ordered
        remaining = qty - (max(1, qty // 10) if short and qty > 1 else 0)
        for p in range(parts):
            part_qty = remaining if p == parts - 1 else max(1, remaining // 2)
            remaining -= part_qty if p < parts - 1 else 0
            ship_date = delivered - timedelta(days=rng.randint(1, 5))
            ship_no += 1
            shipments.append(_shipment(ship_no, po, rng.choice(CARRIERS), ship_date, delivered, part_qty,
                                       "DELIVERED", rng))

    # ---- inventory --------------------------------------------------------
    inventory = []
    tracked = rng.sample(products, k=min(120, len(products)))
    for d in range(inventory_days):
        snap = end_date - timedelta(days=inventory_days - 1 - d)
        for wh in warehouses:
            for prod in tracked:
                rop = 50 + (int(prod["sku"][-3:]) % 150)
                on_hand = 0 if rng.random() < 0.03 else max(0, int(rng.gauss(rop * 2.2, rop)))
                inventory.append({"snapshot_date": snap.isoformat(), "warehouse_id": wh["warehouse_id"],
                                  "sku": prod["sku"], "on_hand_qty": str(on_hand), "reorder_point": str(rop)})

    # ---- inject data problems ---------------------------------------------
    # Order matters so that every injected problem is counted exactly once:
    # in-place edits -> exact duplicates of clean rows -> new rows with unique keys / distinct content.
    if issue_rate > 0:
        clean_pos, clean_ships = list(pos), list(shipments)
        stats.orphan_po_suppliers = _inject_orphan_suppliers(pos, rng, int(len(pos) * 0.001 * issue_rate))
        for name, rows, rate in (("purchase_orders", pos, 0.01), ("shipments", shipments, 0.01),
                                 ("inventory_snapshots", inventory, 0.005), ("products", products, 0.01)):
            n = int(len(rows) * rate * issue_rate)
            rows.extend(dict(r) for r in rng.sample(rows, k=n))
            stats.exact_duplicates[name] = n
        stats.orphan_shipments = _inject_orphan_shipments(shipments, clean_ships, rng,
                                                          int(len(clean_ships) * 0.002 * issue_rate), ship_no)
        ship_no += stats.orphan_shipments
        stats.superseded_versions["shipments"] = _inject_shipment_versions(
            shipments, clean_ships, rng, int(len(clean_ships) * 0.01 * issue_rate))
        stats.superseded_versions["purchase_orders"] = _inject_po_versions(
            pos, clean_pos, rng, int(len(clean_pos) * 0.005 * issue_rate))
        stats.invalid_rows["purchase_orders"] = _inject_invalid_pos(pos, clean_pos, rng,
                                                                    int(len(clean_pos) * 0.003 * issue_rate))
        stats.invalid_rows["shipments"] = _inject_invalid_shipments(shipments, clean_ships, rng,
                                                                    int(len(clean_ships) * 0.003 * issue_rate))

    tables = {"suppliers": suppliers, "products": products, "warehouses": warehouses, "purchase_orders": pos,
              "shipments": shipments, "inventory_snapshots": inventory}
    contracts = load_contracts()
    for name, rows in tables.items():
        rng.shuffle(rows)
        header = list(contracts[name].column_names)
        if schema_drift and name == "shipments":
            header[header.index("delivered_date")] = "delivery_date"
            rows = [{("delivery_date" if k == "delivered_date" else k): v for k, v in r.items()} for r in rows]
        _write_csv(out_dir / contracts[name].file, header, rows)
        stats.rows[name] = len(rows)

    (out_dir / "_manifest.json").write_text(json.dumps(asdict(stats) | {"total_rows": stats.total_rows}, indent=2))
    return stats


def _shipment(n, po, carrier, ship_date, delivered, qty, status, rng) -> dict:
    return {"shipment_id": f"SHP-{n:07d}", "po_id": po["po_id"], "po_line": po["po_line"], "carrier": carrier,
            "ship_date": ship_date.isoformat(), "delivered_date": delivered.isoformat() if delivered else "",
            "qty_shipped": str(qty), "status": status,
            "updated_at": _ts(delivered or ship_date, rng)}


def _inject_orphan_suppliers(pos, rng, n) -> int:
    for row in rng.sample(pos, k=n):
        row["supplier_id"] = "SUP-9999"
    return n


def _inject_orphan_shipments(shipments, clean, rng, n, last_no) -> int:
    for i in range(n):
        base = dict(rng.choice(clean))
        base.update(shipment_id=f"SHP-{last_no + i + 1:07d}", po_id=f"PO-9{i:05d}")
        shipments.append(base)
    return n


def _inject_shipment_versions(shipments, clean, rng, n) -> int:
    """Older IN_TRANSIT version of a delivered shipment, arriving in the same extract."""
    delivered = [s for s in clean if s["status"] == "DELIVERED"]
    for s in rng.sample(delivered, k=n):
        older = dict(s, status="IN_TRANSIT", delivered_date="",
                     updated_at=(datetime.fromisoformat(s["updated_at"]) - timedelta(days=2)).isoformat(sep=" "))
        shipments.append(older)
    return n


def _inject_po_versions(pos, clean, rng, n) -> int:
    """Older version of a PO line with a different quantity; the newer one must win."""
    for p in rng.sample(clean, k=n):
        older = dict(p, qty_ordered=str(int(p["qty_ordered"]) + 10),
                     updated_at=(datetime.fromisoformat(p["updated_at"]) - timedelta(hours=6)).isoformat(sep=" "))
        pos.append(older)
    return n


def _inject_invalid_pos(pos, clean, rng, n) -> int:
    breakers = [
        lambda r: r.update(qty_ordered="abc"),
        lambda r: r.update(qty_ordered="-5"),
        lambda r: r.update(order_date="2026-13-45"),
        lambda r: r.update(currency="XYZ"),
        lambda r: r.update(po_id=""),
    ]
    for i in range(n):
        row = dict(rng.choice(clean), po_id=f"PO-BAD{i:05d}")  # unique key so accounting stays exact
        breakers[i % len(breakers)](row)
        pos.append(row)
    return n


def _inject_invalid_shipments(shipments, clean, rng, n) -> int:
    breakers = [
        lambda r: r.update(qty_shipped="1O0"),
        lambda r: r.update(status="LOST??"),
        lambda r: r.update(ship_date="31/12/2026"),
    ]
    for i in range(n):
        row = dict(rng.choice(clean), shipment_id=f"SHP-BAD{i:05d}")
        breakers[i % len(breakers)](row)
        shipments.append(row)
    return n


def _write_csv(path: Path, header: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Generate a batch of synthetic supply-chain extracts")
    ap.add_argument("--out", type=Path, default=Path("data/landing/batch_001"))
    ap.add_argument("--po-lines", type=int, default=55_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--schema-drift", action="store_true")
    ap.add_argument("--clean", action="store_true", help="no injected data problems")
    args = ap.parse_args(argv)
    stats = generate(args.out, po_lines=args.po_lines, seed=args.seed, schema_drift=args.schema_drift,
                     issue_rate=0 if args.clean else 1)
    print(f"wrote {stats.total_rows:,} rows to {args.out}")
    for k, v in stats.rows.items():
        print(f"  {k:<22}{v:>9,}")


if __name__ == "__main__":
    main()
