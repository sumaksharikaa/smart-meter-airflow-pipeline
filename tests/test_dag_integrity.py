"""DAG integrity: the DAGs import cleanly and are wired the way operations depends on."""

import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")
DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def bag(tmp_path_factory):
    os.environ.setdefault("AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow")))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(bag):
    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {"ami_daily_pipeline", "ami_reliability_report"}


def test_daily_pipeline_shape(bag):
    dag = bag.dags["ami_daily_pipeline"]
    assert dag.max_active_runs == 1 and not dag.catchup
    ids = set(dag.task_ids)
    assert ids == {
        "resolve_data_date",
        "wait_for_files",
        "validate_file",
        "ingest",
        "vee",
        "usage",
        "reliability",
        "audit",
        "quality_gate",
        "publish",
    }
    # publish runs only after the quality gate; the gate only after the audit
    assert dag.get_task("publish").upstream_task_ids == {"quality_gate", "resolve_data_date"}
    assert "audit" in dag.get_task("quality_gate").upstream_task_ids
    assert {"usage", "reliability"} <= dag.get_task("audit").upstream_task_ids


def test_retry_policy(bag):
    dag = bag.dags["ami_daily_pipeline"]
    assert dag.get_task("vee").retries == 2 and dag.get_task("vee").retry_exponential_backoff
    # bad data cannot be fixed by retrying
    assert dag.get_task("quality_gate").retries == 0
    assert dag.get_task("validate_file").retries == 0
    assert dag.get_task("wait_for_files").mode == "reschedule"


def test_asset_wiring(bag):
    publish = bag.dags["ami_daily_pipeline"].get_task("publish")
    assert [o.name for o in publish.outlets] == ["ami_curated_daily"]
    report = bag.dags["ami_reliability_report"]
    assert "ami_curated_daily" in str(report.timetable.asset_condition)
