"""Warehouse adapters. Snowflake is the target; DuckDB runs the identical SQL locally and in CI.

Only three things differ between engines and live here: connecting, bulk-loading a CSV into RAW
(PUT + COPY INTO vs read_csv) and inserting small generated tables. Every transformation is plain SQL
in scdw/sql/ written in the subset both engines share (MERGE, QUALIFY, TRY_CAST, DATEDIFF, DATE_TRUNC).
"""

from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any

SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_LITERAL = re.compile(r"^[A-Za-z0-9_\-:. ]*$")


def ident(name: str) -> str:
    if not SAFE_IDENT.match(name):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


def literal(value: str) -> str:
    """Quote a pipeline-generated value (batch id, table name). Rejects anything unexpected."""
    if not SAFE_LITERAL.match(value):
        raise ValueError(f"unsafe literal: {value!r}")
    return f"'{value}'"


class Warehouse(ABC):
    name: str

    @abstractmethod
    def execute(self, sql: str) -> None: ...

    @abstractmethod
    def query(self, sql: str) -> list[tuple]: ...

    @abstractmethod
    def load_csv(self, path: Path, table: str, header: list[str], columns: list[str], batch_id: str) -> int:
        """Load the contract columns of a CSV (matched by header name) into RAW.<table>. Returns rows loaded."""

    @abstractmethod
    def insert_rows(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None: ...

    def run_script(self, sql: str) -> None:
        for statement in split_sql(sql):
            self.execute(statement)

    def scalar(self, sql: str) -> Any:
        rows = self.query(sql)
        return rows[0][0] if rows else None

    @abstractmethod
    def close(self) -> None: ...


def split_sql(script: str) -> list[str]:
    """Split on semicolons outside quotes; drops comment-only chunks."""
    out, buf, quote = [], [], None
    for line in script.splitlines():
        stripped = line.strip()
        if quote is None and stripped.startswith("--"):
            continue
        for ch in line:
            if quote:
                if ch == quote:
                    quote = None
            elif ch in ("'", '"'):
                quote = ch
            elif ch == ";":
                stmt = "".join(buf).strip()
                if stmt:
                    out.append(stmt)
                buf = []
                continue
            buf.append(ch)
        buf.append("\n")
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


class DuckDBWarehouse(Warehouse):
    name = "duckdb"

    def __init__(self, path: str = ":memory:"):
        import duckdb

        self.conn = duckdb.connect(path)

    def execute(self, sql: str) -> None:
        self.conn.execute(sql)

    def query(self, sql: str) -> list[tuple]:
        return self.conn.execute(sql).fetchall()

    def load_csv(self, path: Path, table: str, header: list[str], columns: list[str], batch_id: str) -> int:
        select = ", ".join(f'"{c}"' for c in columns)
        before = self.scalar(f"SELECT COUNT(*) FROM RAW.{ident(table)}")
        self.conn.execute(
            f"INSERT INTO RAW.{ident(table)} ({', '.join(columns)}, _batch_id, _source_file, _loaded_at) "
            f"SELECT {select}, ?, ?, CURRENT_TIMESTAMP "
            "FROM read_csv(?, header = true, all_varchar = true, quote = '\"')",
            [batch_id, path.name, str(path)],
        )
        return self.scalar(f"SELECT COUNT(*) FROM RAW.{ident(table)}") - before

    def insert_rows(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
        placeholders = ", ".join("?" for _ in columns)
        self.conn.executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})", rows)

    def close(self) -> None:
        self.conn.close()


class SnowflakeWarehouse(Warehouse):
    """Uses snowflake-connector-python. In Airflow the connection comes from SnowflakeHook instead."""

    name = "snowflake"
    STAGE = "RAW.SCDW_LANDING"
    FILE_FORMAT = "RAW.SCDW_CSV"

    def __init__(self, conn=None):
        if conn is None:
            import snowflake.connector

            conn = snowflake.connector.connect(
                account=os.environ["SNOWFLAKE_ACCOUNT"],
                user=os.environ["SNOWFLAKE_USER"],
                password=os.environ.get("SNOWFLAKE_PASSWORD"),
                private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE"),
                role=os.environ.get("SNOWFLAKE_ROLE", "SCDW_LOADER"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "SCDW_WH"),
                database=os.environ.get("SNOWFLAKE_DATABASE", "SUPPLY_CHAIN_DW"),
            )
        self.conn = conn

    def execute(self, sql: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql)

    def query(self, sql: str) -> list[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def load_csv(self, path: Path, table: str, header: list[str], columns: list[str], batch_id: str) -> int:
        # Map contract columns to CSV positions by header name, so column order changes are harmless.
        positions = [header.index(c) + 1 for c in columns]
        prefix = f"{batch_id}/{table}"
        self.execute(f"PUT 'file://{path.resolve()}' @{self.STAGE}/{prefix} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        select = ", ".join(f"t.${p}" for p in positions)
        with self.conn.cursor() as cur:
            cur.execute(
                f"COPY INTO RAW.{ident(table)} ({', '.join(columns)}, _batch_id, _source_file) "
                f"FROM (SELECT {select}, {literal(batch_id)}, METADATA$FILENAME FROM @{self.STAGE}/{prefix} t) "
                f"FILE_FORMAT = (FORMAT_NAME = '{self.FILE_FORMAT}') ON_ERROR = 'ABORT_STATEMENT' FORCE = TRUE"
            )
            return sum(int(r[3]) for r in cur.fetchall() if len(r) > 3 and str(r[3]).isdigit())

    def insert_rows(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
        placeholders = ", ".join("%s" for _ in columns)
        with self.conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})", list(rows))

    def close(self) -> None:
        self.conn.close()


def connect(engine: str | None = None, duckdb_path: str | None = None) -> Warehouse:
    engine = (engine or os.getenv("SCDW_ENGINE", "duckdb")).lower()
    if engine == "duckdb":
        return DuckDBWarehouse(duckdb_path or os.getenv("SCDW_DUCKDB_PATH", "data/scdw.duckdb"))
    if engine == "snowflake":
        return SnowflakeWarehouse()
    raise ValueError(f"unknown engine {engine!r}")
