from __future__ import annotations

from pathlib import Path

import pytest

from scdw import pipeline
from scdw.generate import generate
from scdw.warehouse import DuckDBWarehouse


@pytest.fixture(scope="session")
def full_batch(tmp_path_factory) -> tuple[Path, object]:
    """Default-size batch (>100k records) with every kind of injected data problem."""
    batch = tmp_path_factory.mktemp("landing") / "batch_001"
    stats = generate(batch)
    return batch, stats


@pytest.fixture(scope="session")
def loaded(full_batch, tmp_path_factory):
    batch, stats = full_batch
    wh = DuckDBWarehouse(str(tmp_path_factory.mktemp("db") / "scdw.duckdb"))
    result = pipeline.run(wh, batch, run_id="run_test")
    yield wh, result, stats, batch
    wh.close()


@pytest.fixture
def fresh_wh():
    wh = DuckDBWarehouse(":memory:")
    yield wh
    wh.close()
