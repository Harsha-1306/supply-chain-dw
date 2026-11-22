"""CLI: python -m scdw {generate,run,report,render-sql}"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import pipeline, report, staging
from .checks import QualityGateError
from .contracts import load_contracts
from .generate import main as generate_main
from .warehouse import connect


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "generate":
        generate_main(argv[1:])
        return 0

    ap = argparse.ArgumentParser(prog="scdw")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("generate", help="generate synthetic source extracts (see --help)")
    r = sub.add_parser("run", help="run the full pipeline for one landing batch")
    r.add_argument("batch_dir", type=Path)
    r.add_argument("--engine", choices=["duckdb", "snowflake"])
    r.add_argument("--duckdb-path", default="data/scdw.duckdb")
    rep = sub.add_parser("report", help="export MART views to CSV and render the KPI preview")
    rep.add_argument("--engine", choices=["duckdb", "snowflake"])
    rep.add_argument("--duckdb-path", default="data/scdw.duckdb")
    rep.add_argument("--out", type=Path, default=Path("exports"))
    rs = sub.add_parser("render-sql", help="print the generated RAW/STAGING SQL for one table")
    rs.add_argument("table", choices=sorted(load_contracts()))
    rs.add_argument("--batch-id", default="batch_001")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "render-sql":
        c = load_contracts()[args.table]
        for sql in (staging.raw_ddl(c), staging.staging_ddl(c), staging.work_table_sql(c, args.batch_id),
                    staging.reject_insert_sql(c), staging.merge_sql(c)):
            print(sql + ";\n")
        return 0

    if args.engine == "duckdb" or (args.engine is None and args.duckdb_path):
        Path(args.duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    wh = connect(args.engine, args.duckdb_path)
    try:
        if args.cmd == "run":
            result = pipeline.run(wh, args.batch_dir)
            print(json.dumps(result, indent=2, default=str))
        else:
            for p in report.export_marts(wh, args.out):
                print(f"exported {p}")
            print(f"preview  {report.render_preview(wh, args.out / 'dashboard_preview.png')}")
    except QualityGateError as exc:
        print(f"QUALITY GATE FAILED: {exc}", file=sys.stderr)
        return 2
    finally:
        wh.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
