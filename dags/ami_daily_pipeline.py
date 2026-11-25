"""
AMI daily pipeline: smart-meter interval reads -> VEE -> usage, load and reliability -> Hive.

    resolve_data_date
      -> wait_for_files            sensor (reschedule mode): every region file + control file landed
      -> validate_file [mapped]    one task per region: header, row count, MD5 vs control file
      -> ingest                    valid files -> bronze Parquet (read_date/region partitions)
      -> vee                       validation, estimation, editing -> staged interval_reads
      -> usage | reliability       daily usage + feeder load  |  outages + SAIDI/SAIFI/CAIDI/MAIFI
      -> audit                     completeness per region, rejected files -> DQ report
      -> quality_gate              fails the run (no retries) if the day may not be published
      -> publish                   staged partitions -> curated, registered in Hive; updates the Asset

Write-audit-publish: nothing reaches the curated zone, Hive or downstream
DAGs unless the audit passes. A run for a day with a corrupt file fails at
the gate. Once the corrected file is re-sent, clearing or re-triggering that
day republishes it cleanly (every step is idempotent per day).

Reprocess any day: trigger with conf {"data_date": "YYYY-MM-DD"}.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, Param, PokeReturnValue, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from meter_pipeline import jobs
from meter_pipeline.alerts import notify_failure
from meter_pipeline.config import load_config
from meter_pipeline.files import files_ready

CFG = load_config()
LOCAL_TZ = "America/Los_Angeles"
CURATED_DAILY = Asset("ami_curated_daily")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="ami_daily_pipeline",
    description="Smart-meter interval reads: validate, VEE, aggregate, reliability, publish to Hive",
    schedule="30 6 * * *",  # 06:30 local, after head-end extracts land overnight
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "data_date": Param(
            None, type=["null", "string"], format="date", description="Day to process (default: yesterday)"
        )
    },
    tags=["ami", "spark", "hive", "utility"],
    doc_md=__doc__,
)
def ami_daily_pipeline():
    @task
    def resolve_data_date() -> str:
        ctx = get_current_context()
        if ctx["params"].get("data_date"):
            return ctx["params"]["data_date"]
        run_time = pendulum.instance(ctx["logical_date"]).in_timezone(LOCAL_TZ)
        return run_time.subtract(days=1).to_date_string()

    @task.sensor(poke_interval=300, timeout=6 * 3600, mode="reschedule", retries=0)
    def wait_for_files(data_date: str) -> PokeReturnValue:
        ready = files_ready(CFG, data_date)
        return PokeReturnValue(is_done=len(ready) == len(CFG.regions), xcom_value=ready)

    @task(retries=0)
    def validate_file(region: str, data_date: str) -> dict:
        return jobs.validate_file(data_date, region, CFG)

    @task
    def ingest(data_date: str, checks: list[dict]) -> dict:
        return jobs.ingest_day(data_date, list(checks), CFG)

    @task
    def vee(data_date: str) -> dict:
        return jobs.run_vee(data_date, CFG)

    @task
    def usage(data_date: str) -> dict:
        return jobs.build_usage(data_date, CFG)

    @task
    def reliability(data_date: str) -> dict:
        return jobs.build_reliability(data_date, CFG)

    @task
    def audit(data_date: str, checks: list[dict], vee_stats: dict) -> dict:
        return jobs.audit_day(data_date, list(checks), vee_stats, CFG)

    @task(retries=0)
    def quality_gate(report: dict) -> None:
        if not report["passed"]:
            # AirflowFailException fails immediately: retrying cannot fix bad data.
            raise AirflowFailException("Quality gate failed: " + "; ".join(report["failures"]))

    @task(outlets=[CURATED_DAILY])
    def publish(data_date: str) -> dict:
        return jobs.publish_day(data_date, CFG)

    data_date = resolve_data_date()
    landed = wait_for_files(data_date)
    checks = validate_file.partial(data_date=data_date).expand(region=CFG.regions)
    landed >> checks
    loaded = ingest(data_date, checks)
    stats = vee(data_date)
    loaded >> stats
    built = [usage(data_date), reliability(data_date)]
    stats >> built
    report = audit(data_date, checks, stats)
    built >> report
    gate = quality_gate(report)
    gate >> publish(data_date)


ami_daily_pipeline()
