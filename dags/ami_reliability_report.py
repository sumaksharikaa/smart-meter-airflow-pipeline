"""
Month-to-date reliability report, triggered by data (an Airflow Asset), not by a clock.

Each time ami_daily_pipeline publishes a day, this DAG rebuilds the
month-to-date SAIDI / SAIFI / CAIDI / MAIFI by region from the curated
reliability_daily table and writes a CSV for the regulatory reporting team.
"""

from __future__ import annotations

import csv
from collections import defaultdict

import pendulum
import pyarrow.dataset as pads
from airflow.sdk import Asset, dag, task

from meter_pipeline.alerts import notify_failure
from meter_pipeline.config import load_config

CFG = load_config()


@dag(
    dag_id="ami_reliability_report",
    schedule=[Asset("ami_curated_daily")],
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Los_Angeles"),
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 1, "on_failure_callback": notify_failure},
    tags=["ami", "reporting", "reliability"],
    doc_md=__doc__,
)
def ami_reliability_report():
    @task
    def build_month_to_date() -> dict:
        source = CFG.curated("reliability_daily")
        table = pads.dataset(str(source), format="parquet", partitioning="hive").to_table()
        rows = table.to_pylist()
        if not rows:
            return {}
        month = max(str(r["read_date"]) for r in rows)[:7]
        totals: dict[str, dict] = defaultdict(lambda: defaultdict(float))
        days = set()
        for r in rows:
            if not str(r["read_date"]).startswith(month):
                continue
            days.add(str(r["read_date"]))
            t = totals[r["region"]]
            t["customers_served"] = max(t["customers_served"], r["customers_served"])
            for k in ("customer_interruptions", "customer_minutes_interrupted", "momentary_interruptions"):
                t[k] += float(r[k] or 0)

        out = CFG.report("reliability", f"reliability_{month}_mtd.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        summary = {}
        with out.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(
                ["month", "days", "region", "customers_served", "saifi", "saidi_minutes", "caidi_minutes", "maifi"]
            )
            for region, t in sorted(totals.items()):
                n = t["customers_served"] or 1
                saifi = t["customer_interruptions"] / n
                saidi = t["customer_minutes_interrupted"] / n
                caidi = saidi / saifi if saifi else None
                maifi = t["momentary_interruptions"] / n
                w.writerow(
                    [
                        month,
                        len(days),
                        region,
                        int(n),
                        round(saifi, 5),
                        round(saidi, 3),
                        round(caidi, 2) if caidi else "",
                        round(maifi, 5),
                    ]
                )
                summary[region] = {"saidi": round(saidi, 3), "saifi": round(saifi, 5)}
        return {"month": month, "days": len(days), "file": str(out), "regions": summary}

    build_month_to_date()


ami_reliability_report()
