import json
from pathlib import Path

import pytest
import yaml

from scdw import checks, dashboard, staging
from scdw.contracts import _parse, load_contracts
from scdw.generate import generate
from scdw.report import export_marts, render_preview
from scdw.warehouse import ident, literal, split_sql


def test_contracts_load():
    c = load_contracts()
    assert set(c) == {"suppliers", "products", "warehouses", "purchase_orders", "shipments", "inventory_snapshots"}
    assert c["purchase_orders"].primary_key == ("po_id", "po_line")
    assert c["products"].columns[3].type == "DECIMAL(12,2)"


@pytest.mark.parametrize(
    ("spec", "error"),
    [
        ({"file": "x.csv", "primary_key": ["id"], "columns": [{"name": "a", "type": "VARCHAR"}]}, "primary key"),
        ({"file": "x.csv", "primary_key": ["a"], "columns": [{"name": "a", "type": "BLOB", "nullable": False}]},
         "unsupported type"),
        ({"file": "x.csv", "primary_key": ["a"], "columns": [{"name": "a", "type": "VARCHAR"}]}, "non-nullable"),
        ({"file": "x.csv", "primary_key": ["a"], "version_column": "v",
          "columns": [{"name": "a", "type": "VARCHAR", "nullable": False}]}, "version column"),
    ],
)
def test_contract_validation(spec, error):
    with pytest.raises(ValueError, match=error):
        _parse("t", spec)


def test_generator_is_deterministic(tmp_path):
    a, b = generate(tmp_path / "a", po_lines=1_000, seed=7), generate(tmp_path / "b", po_lines=1_000, seed=7)
    assert a.rows == b.rows
    assert (tmp_path / "a" / "shipments.csv").read_bytes() == (tmp_path / "b" / "shipments.csv").read_bytes()
    manifest = json.loads((tmp_path / "a" / "_manifest.json").read_text())
    assert manifest["total_rows"] == a.total_rows


def test_clean_generation_has_no_issues(tmp_path):
    stats = generate(tmp_path, po_lines=1_000, issue_rate=0)
    assert not stats.exact_duplicates and not stats.invalid_rows and stats.orphan_shipments == 0


def test_schema_check(tmp_path):
    c = load_contracts()["shipments"]
    ok = tmp_path / "ok.csv"
    ok.write_text(",".join(reversed(c.column_names)) + "\n")  # order does not matter
    assert checks.check_schema(ok, c).status == checks.PASS
    extra = tmp_path / "extra.csv"
    extra.write_text(",".join(c.column_names + ["note"]) + "\n")
    assert checks.check_schema(extra, c).status == checks.WARN
    missing = tmp_path / "missing.csv"
    missing.write_text(",".join(c.column_names[:-1]) + "\n")
    res = checks.check_schema(missing, c)
    assert res.status == checks.FAIL and "updated_at" in res.details
    assert checks.check_schema(tmp_path / "nope.csv", c).status == checks.FAIL


def test_reject_rate_threshold():
    c = load_contracts()["purchase_orders"]
    assert checks.check_reject_rate(c, 1000, 50).status == checks.PASS
    assert checks.check_reject_rate(c, 1000, 51).status == checks.FAIL
    assert checks.check_reject_rate(c, 0, 0).status == checks.PASS


def test_generated_merge_sql_shape():
    sql = staging.merge_sql(load_contracts()["shipments"])
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY updated_at DESC" in sql
    assert "WHEN MATCHED AND src.updated_at > tgt.updated_at" in sql
    inv = staging.merge_sql(load_contracts()["inventory_snapshots"])
    assert "WHEN MATCHED THEN UPDATE" in inv  # no version column -> latest load wins


def test_reject_rules_cover_contract():
    sql = staging.work_table_sql(load_contracts()["purchase_orders"], "b1")
    for reason in ("qty_ordered: below minimum 1", "currency: not an allowed value", "order_date: not a valid DATE",
                   "po_id: required value missing"):
        assert reason in sql


def test_split_sql_respects_quotes():
    parts = split_sql("SELECT 'a;b';\n-- comment;\nSELECT 2;")
    assert parts == ["SELECT 'a;b'", "SELECT 2"]


@pytest.mark.parametrize("bad", ["x; DROP TABLE y", "a'b", "tab\nle"])
def test_identifier_and_literal_guards(bad):
    with pytest.raises(ValueError):
        literal(bad)
    with pytest.raises(ValueError):
        ident(bad)


def test_contract_yaml_has_no_unquoted_commas_in_types():
    raw = yaml.safe_load(Path(load_contracts.__wrapped__.__defaults__[0]).read_text())
    for spec in raw.values():
        for col in spec["columns"]:
            assert "(" not in col["type"] or ")" in col["type"]


def test_report_exports(loaded, tmp_path):
    wh, *_ = loaded
    files = export_marts(wh, tmp_path)
    assert {f.name for f in files} >= {"v_kpi_otif_monthly.csv", "v_dq_scorecard.csv"}
    assert all(f.stat().st_size > 0 for f in files)
    png = render_preview(wh, tmp_path / "preview.png")
    assert png.read_bytes()[:4] == b"\x89PNG"


def test_dashboard_site(loaded, tmp_path):
    wh, *_ = loaded
    data = dashboard.collect(wh)
    assert data["load"]["raw_rows"] > 100_000
    assert len(data["suppliers"]) > 200 and data["dq"] and data["otif_monthly"]
    for r in data["loads"]:
        assert r["raw"] == r["merged"] + r["keys"] + r["rejected"]
    html = dashboard.build_site(wh, tmp_path).read_text()
    assert "/*__DATA__*/null" not in html and '"otif_monthly"' in html and "</script>" in html
