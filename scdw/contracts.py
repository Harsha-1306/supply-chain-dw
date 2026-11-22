"""Typed view over contracts/sources.yml."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

import yaml

CONTRACTS_FILE = Path(__file__).resolve().parent / "contracts" / "sources.yml"
SUPPORTED_TYPES = ("VARCHAR", "INTEGER", "DATE", "TIMESTAMP", "DECIMAL")


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    min: float | None = None
    allowed: tuple | None = None


@dataclass(frozen=True)
class Contract:
    table: str
    file: str
    primary_key: tuple[str, ...]
    version_column: str | None
    max_reject_rate: float
    columns: tuple[Column, ...] = field(default_factory=tuple)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


def _parse(table: str, spec: dict) -> Contract:
    columns = tuple(
        Column(
            name=c["name"],
            type=c["type"].upper(),
            nullable=c.get("nullable", True),
            min=c.get("min"),
            allowed=tuple(c["allowed"]) if c.get("allowed") is not None else None,
        )
        for c in spec["columns"]
    )
    contract = Contract(
        table=table,
        file=spec["file"],
        primary_key=tuple(spec["primary_key"]),
        version_column=spec.get("version_column"),
        max_reject_rate=float(spec.get("max_reject_rate", 0.05)),
        columns=columns,
    )
    _validate(contract)
    return contract


def _validate(c: Contract) -> None:
    names = c.column_names
    if len(names) != len(set(names)):
        raise ValueError(f"{c.table}: duplicate column names in contract")
    for key in c.primary_key:
        if key not in names:
            raise ValueError(f"{c.table}: primary key column '{key}' not declared")
    if c.version_column and c.version_column not in names:
        raise ValueError(f"{c.table}: version column '{c.version_column}' not declared")
    for col in c.columns:
        if not col.type.startswith(SUPPORTED_TYPES):
            raise ValueError(f"{c.table}.{col.name}: unsupported type {col.type}")
        if col.name in c.primary_key and col.nullable:
            raise ValueError(f"{c.table}.{col.name}: primary key columns must be non-nullable")


@cache
def load_contracts(path: Path = CONTRACTS_FILE) -> dict[str, Contract]:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return {table: _parse(table, spec) for table, spec in raw.items()}
