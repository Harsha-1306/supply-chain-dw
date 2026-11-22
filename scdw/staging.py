"""Contract-driven SQL for RAW and STAGING: DDL, typed casting, reject routing and de-duplicating MERGE.

The SQL is generated once per contract so six feeds share one tested pattern instead of six hand-copied
scripts. Run `python -m scdw render-sql` to see the exact statements that are sent to Snowflake.
"""

from __future__ import annotations

from .contracts import Column, Contract
from .warehouse import ident, literal

AUDIT_DDL = """
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS MART;
CREATE SCHEMA IF NOT EXISTS AUDIT;

CREATE TABLE IF NOT EXISTS AUDIT.LOAD_LOG (
    run_id        VARCHAR,
    batch_id      VARCHAR,
    table_name    VARCHAR,
    source_file   VARCHAR,
    file_md5      VARCHAR,
    raw_rows      INTEGER,
    exact_duplicate_rows INTEGER,
    key_duplicate_rows   INTEGER,
    rejected_rows INTEGER,
    merged_keys   INTEGER,
    status        VARCHAR,
    loaded_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS AUDIT.DQ_RESULTS (
    run_id      VARCHAR,
    batch_id    VARCHAR,
    check_name  VARCHAR,
    table_name  VARCHAR,
    severity    VARCHAR,
    status      VARCHAR,
    metric      DECIMAL(18,4),
    threshold   DECIMAL(18,4),
    details     VARCHAR,
    checked_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS STAGING.REJECTED_ROWS (
    batch_id    VARCHAR,
    table_name  VARCHAR,
    record_key  VARCHAR,
    reason      VARCHAR,
    raw_record  VARCHAR,
    rejected_at TIMESTAMP
);
"""


def raw_ddl(c: Contract, loaded_at_type: str = "TIMESTAMP") -> str:
    cols = ",\n    ".join(f"{ident(col.name)} VARCHAR" for col in c.columns)
    return (
        f"CREATE TABLE IF NOT EXISTS RAW.{ident(c.table)} (\n    {cols},\n"
        f"    _batch_id VARCHAR,\n    _source_file VARCHAR,\n    _loaded_at {loaded_at_type}\n)"
    )


def staging_ddl(c: Contract) -> str:
    # No PRIMARY KEY: Snowflake does not enforce it, so uniqueness is guaranteed by the MERGE and verified by
    # the duplicate check instead (see checks.py).
    cols = ",\n    ".join(f"{ident(col.name)} {col.type}" for col in c.columns)
    return (
        f"CREATE TABLE IF NOT EXISTS STAGING.{ident(c.table)} (\n    {cols},\n"
        f"    _batch_id VARCHAR,\n    _updated_in_dw TIMESTAMP\n)"
    )


def _typed(col: Column) -> str:
    cleaned = f"NULLIF(TRIM({col.name}), '')"
    return cleaned if col.type == "VARCHAR" else f"TRY_CAST({cleaned} AS {col.type})"


def _sql_value(v) -> str:
    return str(v) if isinstance(v, int | float) else "'" + str(v).replace("'", "''") + "'"


def _reject_rules(c: Contract) -> list[tuple[str, str]]:
    """(condition, reason) pairs, evaluated in order; the first matching rule is the reject reason."""
    rules: list[tuple[str, str]] = []
    for col in c.columns:
        raw = f"_raw_{col.name}"
        if not col.nullable:
            rules.append((f"{raw} IS NULL OR TRIM({raw}) = ''", f"{col.name}: required value missing"))
        if col.type != "VARCHAR":
            rules.append((f"{col.name} IS NULL AND {raw} IS NOT NULL AND TRIM({raw}) <> ''",
                          f"{col.name}: not a valid {col.type}"))
        if col.min is not None:
            rules.append((f"{col.name} < {col.min}", f"{col.name}: below minimum {col.min}"))
        if col.allowed is not None:
            values = ", ".join(_sql_value(v) for v in col.allowed)
            rules.append((f"{col.name} NOT IN ({values})", f"{col.name}: not an allowed value"))
    return rules


def work_table_sql(c: Contract, batch_id: str) -> str:
    """Typed copy of one RAW batch with a reject reason per row (NULL = valid)."""
    typed = ",\n        ".join(f"{_typed(col)} AS {col.name}" for col in c.columns)
    raw = ",\n        ".join(f"{col.name} AS _raw_{col.name}" for col in c.columns)
    cases = "\n            ".join(f"WHEN {cond} THEN '{reason}'" for cond, reason in _reject_rules(c))
    return f"""CREATE OR REPLACE TABLE STAGING._WORK_{ident(c.table)} AS
WITH typed AS (
    SELECT
        {typed},
        {raw},
        _batch_id,
        _loaded_at
    FROM RAW.{ident(c.table)}
    WHERE _batch_id = {literal(batch_id)}
)
SELECT
    *,
    CASE
            {cases}
    END AS _reject_reason
FROM typed"""


def reject_insert_sql(c: Contract) -> str:
    key = " || '|' || ".join(f"COALESCE(_raw_{k}, '')" for k in c.primary_key)
    record = " || '; ' || ".join(f"'{col.name}=' || COALESCE(_raw_{col.name}, '')" for col in c.columns)
    return f"""INSERT INTO STAGING.REJECTED_ROWS (batch_id, table_name, record_key, reason, raw_record, rejected_at)
SELECT _batch_id, {literal(c.table)}, {key}, _reject_reason, {record}, CURRENT_TIMESTAMP
FROM STAGING._WORK_{ident(c.table)}
WHERE _reject_reason IS NOT NULL"""


def _order_by(c: Contract) -> str:
    return f"{c.version_column} DESC, _loaded_at DESC" if c.version_column else "_loaded_at DESC"


def merge_sql(c: Contract) -> str:
    """Upsert the newest valid version of each key. Re-running a batch is a no-op (idempotent)."""
    cols = c.column_names
    on = " AND ".join(f"tgt.{k} = src.{k}" for k in c.primary_key)
    partition = ", ".join(c.primary_key)
    non_key = [x for x in cols if x not in c.primary_key]
    update_set = ",\n        ".join([f"{x} = src.{x}" for x in non_key] +
                                    ["_batch_id = src._batch_id", "_updated_in_dw = CURRENT_TIMESTAMP"])
    newer = f" AND src.{c.version_column} > tgt.{c.version_column}" if c.version_column else ""
    return f"""MERGE INTO STAGING.{ident(c.table)} AS tgt
USING (
    SELECT {', '.join(cols)}, _batch_id
    FROM STAGING._WORK_{ident(c.table)}
    WHERE _reject_reason IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {_order_by(c)}) = 1
) AS src
ON {on}
WHEN MATCHED{newer} THEN UPDATE SET
        {update_set}
WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}, _batch_id, _updated_in_dw)
    VALUES ({', '.join('src.' + x for x in cols)}, src._batch_id, CURRENT_TIMESTAMP)"""


def batch_metrics_sql(c: Contract) -> str:
    """raw rows, exact duplicate rows, key duplicate rows among valid rows, rejected rows, distinct valid keys."""
    raw_cols = ", ".join(f"_raw_{x}" for x in c.column_names)
    keys = ", ".join(c.primary_key)
    w = f"STAGING._WORK_{ident(c.table)}"
    return f"""SELECT
    (SELECT COUNT(*) FROM {w}) AS raw_rows,
    (SELECT COUNT(*) FROM {w}) - (SELECT COUNT(*) FROM (SELECT DISTINCT {raw_cols} FROM {w}) d) AS exact_dup_rows,
    (SELECT COUNT(*) FROM {w} WHERE _reject_reason IS NULL)
        - (SELECT COUNT(*) FROM (SELECT DISTINCT {keys} FROM {w} WHERE _reject_reason IS NULL) k) AS key_dup_rows,
    (SELECT COUNT(*) FROM {w} WHERE _reject_reason IS NOT NULL) AS rejected_rows,
    (SELECT COUNT(*) FROM (SELECT DISTINCT {keys} FROM {w} WHERE _reject_reason IS NULL) k) AS distinct_keys"""


def drop_work_sql(c: Contract) -> str:
    return f"DROP TABLE IF EXISTS STAGING._WORK_{ident(c.table)}"
