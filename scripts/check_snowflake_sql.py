#!/usr/bin/env python3
"""Parse every SQL statement the pipeline sends to Snowflake with sqlfluff's Snowflake grammar.

Covers the hand-written files (snowflake/, scdw/sql/) and the contract-generated RAW/STAGING SQL, so a syntax
error is caught in CI instead of in the nightly Airflow run. (Syntax only - it does not need a Snowflake account.)
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scdw import staging  # noqa: E402
from scdw.contracts import load_contracts  # noqa: E402


def rendered() -> dict[str, str]:
    out = {"audit_ddl": staging.AUDIT_DDL}
    for name, c in load_contracts().items():
        out[name] = ";\n\n".join([
            staging.raw_ddl(c, "TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()"), staging.staging_ddl(c),
            staging.work_table_sql(c, "batch_001"), staging.reject_insert_sql(c), staging.merge_sql(c),
            staging.batch_metrics_sql(c), staging.drop_work_sql(c),
        ]) + ";\n"
    return out


def main() -> int:
    files = sorted(ROOT.glob("snowflake/*.sql")) + sorted(ROOT.glob("scdw/sql/**/*.sql"))
    with tempfile.TemporaryDirectory() as tmp:
        for name, sql in rendered().items():
            p = Path(tmp) / f"generated_{name}.sql"
            p.write_text(sql)
            files.append(p)
        failed = 0
        for f in files:
            res = subprocess.run(["sqlfluff", "parse", "--dialect", "snowflake", str(f)],
                                 capture_output=True, text=True)
            label = f.name if f.is_relative_to(Path(tmp)) else f.relative_to(ROOT)
            if res.returncode != 0:
                failed += 1
                print(f"FAIL {label}")
                print("\n".join(line for line in res.stdout.splitlines() if "unparsable" in line)[:2000])
            else:
                print(f"OK   {label}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
