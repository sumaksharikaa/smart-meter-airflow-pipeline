"""
End-to-end run of the real Airflow DAGs with dag.test() (in-process, no scheduler needed).

Scenario, as operations would live it:
  1. 2026-11-02  normal day                                  -> publishes
  2. 2026-11-03  NORTH_COAST file truncated in transfer       -> rejected, quality gate fails, nothing published
  3. 2026-11-03  head-end re-sends the corrected file, re-run -> publishes
  4. 2026-11-04  normal day                                  -> publishes
  5. ami_reliability_report (Asset-triggered)                 -> month-to-date CSV

Exits non-zero if any outcome differs from the expectation.

    python scripts/run_e2e.py --root /tmp/ami-e2e
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def setup_env(root: Path) -> None:
    os.environ.update(
        {
            "AMI_DATA_ROOT": str(root / "data"),
            "AIRFLOW_HOME": str(root / "airflow"),
            "AIRFLOW__CORE__DAGS_FOLDER": str(REPO / "dags"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "PYTHONPATH": f"{REPO / 'src'}{os.pathsep}{os.environ.get('PYTHONPATH', '')}",
            "TZ": "UTC",
        }
    )
    sys.path.insert(0, str(REPO / "src"))


def generate(root: Path, *extra: str) -> None:
    subprocess.run(
        [sys.executable, str(REPO / "data_generator" / "generate_ami_data.py"), "--root", str(root / "data"), *extra],
        check=True,
    )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, default=Path("/tmp/ami-e2e"))
    a = p.parse_args()
    root = a.root.resolve()
    shutil.rmtree(root, ignore_errors=True)
    setup_env(root)

    generate(root, "--start-date", "2026-11-02", "--days", "3", "--truncate", "NORTH_COAST:2026-11-03")
    subprocess.run(["airflow", "db", "migrate"], check=True, stdout=subprocess.DEVNULL)

    import pendulum
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(REPO / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    daily, report = bag.dags["ami_daily_pipeline"], bag.dags["ami_reliability_report"]

    def run(dag, data_date: str | None, minute: int) -> tuple[str, dict]:
        when = pendulum.datetime(2026, 11, 20, 6, minute, tz="America/Los_Angeles")
        dr = dag.test(logical_date=when, run_conf={"data_date": data_date} if data_date else None)
        states = {ti.task_id: str(ti.state) for ti in dr.get_task_instances()}
        return str(dr.state), states

    results, problems = [], []

    def expect(label: str, outcome: tuple[str, dict], dag_state: str, task_states: dict) -> None:
        state, tasks = outcome
        results.append({"step": label, "dag_run": state, "tasks": tasks})
        if state != dag_state:
            problems.append(f"{label}: run {state}, expected {dag_state}")
        for task_id, want in task_states.items():
            if tasks.get(task_id) != want:
                problems.append(f"{label}: {task_id} {tasks.get(task_id)}, expected {want}")

    ok = {"quality_gate": "success", "publish": "success"}
    expect("2026-11-02", run(daily, "2026-11-02", 0), "success", ok)
    expect(
        "2026-11-03 truncated file",
        run(daily, "2026-11-03", 1),
        "failed",
        {"quality_gate": "failed", "publish": "upstream_failed"},
    )

    generate(root, "--resend", "NORTH_COAST:2026-11-03")
    expect("2026-11-03 after resend", run(daily, "2026-11-03", 2), "success", ok)
    expect("2026-11-04", run(daily, "2026-11-04", 3), "success", ok)
    expect("reliability report", run(report, None, 4), "success", {"build_month_to_date": "success"})

    data = root / "data"
    dq = json.loads((data / "reports" / "dq" / "2026-11-03.json").read_text())
    hive_parts = sorted(p.name for p in (data / "curated" / "interval_reads").glob("read_date=*"))
    report_csv = data / "reports" / "reliability" / "reliability_2026-11_mtd.csv"
    rejected = sorted(p.name for p in (data / "rejected").rglob("*.csv"))
    alerts = sorted(p.name for p in (data / "reports" / "alerts").glob("*.json"))

    if hive_parts != ["read_date=2026-11-02", "read_date=2026-11-03", "read_date=2026-11-04"]:
        problems.append(f"curated partitions: {hive_parts}")
    if rejected != ["ami_reads_NORTH_COAST_20261103.csv"]:
        problems.append(f"rejected files: {rejected}")
    if not alerts:
        problems.append("no failure alert was written for the blocked day")
    if not report_csv.exists() or "2026-11,3," not in report_csv.read_text():
        problems.append("month-to-date report missing or does not cover 3 days")

    print("\n=== End-to-end results ===")
    for r in results:
        print(f"{r['step']:<28} {r['dag_run']:<8} " + ", ".join(f"{k}={v}" for k, v in sorted(r["tasks"].items())))
    print(
        f"\nDQ report 2026-11-03 (after resend): passed={dq['passed']}, completeness="
        + ", ".join(f"{k} {v['completeness']:.2%}" for k, v in sorted(dq["regions"].items()))
    )
    print(f"Rejected: {rejected}   Alerts: {len(alerts)}   Curated days: {len(hive_parts)}")
    print(report_csv.read_text() if report_csv.exists() else "(no report)")
    if problems:
        print("FAILED:\n  " + "\n  ".join(problems))
        return 1
    print("ALL EXPECTATIONS MET")
    return 0


if __name__ == "__main__":
    sys.exit(main())
