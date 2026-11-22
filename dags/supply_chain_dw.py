"""Daily supply-chain warehouse load: extracts -> schema gate -> RAW -> STAGING (dedupe/reject) -> CORE -> MART
-> post-load quality checks -> Power BI refresh.

Engine: Snowflake via the `snowflake_default` Airflow connection when SCDW_ENGINE=snowflake, otherwise a local
DuckDB file (same SQL) so the DAG can be run end-to-end without a Snowflake account.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException, AirflowSkipException

try:  # Airflow 3
    from airflow.sdk import Param, dag, task, task_group
except ImportError:  # pragma: no cover - Airflow 2.x
    from airflow.decorators import dag, task, task_group
    from airflow.models.param import Param

log = logging.getLogger(__name__)

LANDING_ROOT = Path(os.getenv("SCDW_LANDING_ROOT", "/opt/airflow/data/landing"))
EXPORT_ROOT = Path(os.getenv("SCDW_EXPORT_ROOT", "/opt/airflow/data/exports"))
ENGINE = os.getenv("SCDW_ENGINE", "duckdb")
DUCKDB_PATH = os.getenv("SCDW_DUCKDB_PATH", "/opt/airflow/data/scdw.duckdb")


def _warehouse():
    from scdw.warehouse import DuckDBWarehouse, SnowflakeWarehouse

    if ENGINE == "snowflake":
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        return SnowflakeWarehouse(SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn())
    return DuckDBWarehouse(DUCKDB_PATH)


def _quality_gate(fn, *args):
    """Data problems are not transient: fail immediately instead of retrying."""
    from scdw.checks import QualityGateError

    wh = _warehouse()
    try:
        return fn(wh, *args)
    except QualityGateError as exc:
        raise AirflowFailException(f"Quality gate failed: {exc}") from exc
    finally:
        wh.close()


def _on_failure(context) -> None:
    ti = context["task_instance"]
    log.error("SCDW task failed: dag=%s task=%s run=%s", ti.dag_id, ti.task_id, context.get("run_id"))
    # Hook point for Teams/Slack/PagerDuty alerting.


@dag(
    dag_id="supply_chain_dw",
    description="Load supply-chain extracts into Snowflake with duplicate and schema checks; refresh Power BI",
    schedule="0 5 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "on_failure_callback": _on_failure,
    },
    params={
        "generate_demo_data": Param(True, type="boolean",
                                    description="Write synthetic extracts instead of waiting for the ERP drop"),
        "po_lines": Param(55_000, type="integer", minimum=100),
    },
    tags=["supply-chain", "snowflake", "power-bi"],
    doc_md=__doc__,
)
def supply_chain_dw():
    @task
    def batch_dir(ds_nodash=None) -> str:
        return str(LANDING_ROOT / f"batch_{ds_nodash}")

    @task
    def generate_demo_extracts(path: str, params=None) -> str:
        if not params["generate_demo_data"]:
            raise AirflowSkipException("production mode: waiting for the ERP/WMS export instead")
        from scdw.generate import generate

        stats = generate(Path(path), po_lines=params["po_lines"])
        return f"{stats.total_rows} rows"

    @task.sensor(poke_interval=300, timeout=6 * 3600, mode="reschedule", trigger_rule="none_failed")
    def wait_for_extract(path: str) -> bool:
        # Upstream exports write _manifest.json last, so its presence means the batch is complete.
        return (Path(path) / "_manifest.json").exists()

    @task
    def setup_warehouse() -> None:
        from scdw import pipeline

        _quality_gate(pipeline.setup)

    @task_group(group_id="ingest")
    def ingest(path, upstream):
        @task(retries=0)
        def schema_gate(path: str, run_id=None) -> int:
            from scdw import pipeline

            return len(_quality_gate(pipeline.check_schemas, Path(path), run_id))

        @task
        def load_raw(path: str, run_id=None) -> dict:
            from scdw import pipeline

            return _quality_gate(pipeline.ingest, Path(path), run_id)

        gate = schema_gate(path)
        manifest = load_raw(path)
        upstream >> gate >> manifest
        return manifest

    @task_group(group_id="transform")
    def transform(path, manifest):
        @task(retries=0)
        def raw_to_staging(path: str, manifest: dict, run_id=None) -> dict:
            from scdw import pipeline

            return _quality_gate(pipeline.transform_staging, pipeline.batch_id_for(Path(path)), run_id, manifest)

        @task
        def build_core() -> None:
            from scdw import pipeline

            _quality_gate(pipeline.build_core)

        @task
        def build_marts() -> None:
            from scdw import pipeline

            _quality_gate(pipeline.build_marts)

        staged = raw_to_staging(path, manifest)
        core = build_core()
        marts = build_marts()
        staged >> core >> marts
        return marts

    @task(retries=0)
    def post_load_checks(path: str, run_id=None) -> dict:
        from scdw import pipeline

        results = _quality_gate(pipeline.run_post_load_checks, run_id, pipeline.batch_id_for(Path(path)))
        return {r.check_name: r.status for r in results}

    @task_group(group_id="publish")
    def publish():
        @task
        def export_mart_snapshots(ds_nodash=None) -> list[str]:
            from scdw import report

            wh = _warehouse()
            try:
                out = EXPORT_ROOT / ds_nodash
                files = [str(p) for p in report.export_marts(wh, out)]
                files.append(str(report.render_preview(wh, out / "dashboard_preview.png")))
                return files
            finally:
                wh.close()

        @task
        def refresh_power_bi() -> int:
            from scdw import powerbi

            if not powerbi.configured():
                raise AirflowSkipException("Power BI service principal not configured (PBI_* env vars)")
            status = powerbi.trigger_refresh()
            if status != 202:
                raise RuntimeError(f"Power BI refresh request returned HTTP {status}")
            return status

        export_mart_snapshots() >> refresh_power_bi()

    path = batch_dir()
    ready = wait_for_extract(path)
    generate_demo_extracts(path) >> ready
    setup = setup_warehouse()
    ready >> setup
    manifest = ingest(path, setup)
    marts = transform(path, manifest)
    checks = post_load_checks(path)
    marts >> checks >> publish()


supply_chain_dw()
