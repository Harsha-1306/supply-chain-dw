"""DAG integrity. Runs where Airflow is installed (CI job `airflow`); skipped otherwise."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dag():
    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    return bag.dags["supply_chain_dw"]


def test_tasks_present(dag):
    assert set(dag.task_ids) == {
        "batch_dir", "generate_demo_extracts", "wait_for_extract", "setup_warehouse", "ingest.schema_gate",
        "ingest.load_raw", "transform.raw_to_staging", "transform.build_core", "transform.build_marts",
        "post_load_checks", "publish.export_mart_snapshots", "publish.refresh_power_bi",
    }


def test_quality_gates_come_before_loading(dag):
    assert "ingest.schema_gate" in dag.get_task("ingest.load_raw").upstream_task_ids
    assert "setup_warehouse" in dag.get_task("ingest.schema_gate").upstream_task_ids
    assert "post_load_checks" in dag.get_task("publish.export_mart_snapshots").upstream_task_ids


def test_gate_tasks_do_not_retry(dag):
    for task_id in ("ingest.schema_gate", "transform.raw_to_staging", "post_load_checks"):
        assert dag.get_task(task_id).retries == 0


def test_schedule_and_safety(dag):
    assert dag.max_active_runs == 1
    assert not dag.catchup
