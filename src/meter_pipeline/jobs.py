"""
Job entry points: one function per Airflow task.

Each Spark job opens its own SparkSession and closes it, so a task can run
anywhere: in the Airflow worker (local mode, as here), or unchanged through
spark-submit / SparkSubmitOperator / EMR / Dataproc via the CLI below:

    python -m meter_pipeline.jobs vee --ds 2026-11-02
"""

from __future__ import annotations

import argparse
import json
from contextlib import contextmanager

from pyspark.sql import functions as F

from meter_pipeline import files, ingest, metrics, publish, quality, vee
from meter_pipeline.config import Config, load_config
from meter_pipeline.spark import get_spark


@contextmanager
def spark_session(job: str, ds: str, cfg: Config, hive: bool = False):
    spark = get_spark(f"ami-{job}-{ds}", cfg, hive=hive)
    try:
        yield spark
    finally:
        spark.stop()


def _write_staging(df, cfg: Config, table: str, ds: str) -> None:
    parts = publish.TABLES[table]
    (
        df.withColumn("read_date", F.lit(ds).cast("date"))
        .write.mode("overwrite")
        .partitionBy(*parts)
        .parquet(str(cfg.staging(table)))
    )


# ----------------------------------------------------------------- file checks
def validate_file(ds: str, region: str, cfg: Config | None = None) -> dict:
    cfg = cfg or load_config()
    return files.validate_region_file(cfg, ds, region).to_dict()


# --------------------------------------------------------------------- ingest
def ingest_day(ds: str, file_checks: list[dict], cfg: Config | None = None) -> dict:
    cfg = cfg or load_config()
    regions = files.valid_regions(file_checks)
    with spark_session("ingest", ds, cfg) as spark:
        return {
            "regions_loaded": regions,
            "read_rows": ingest.ingest_reads(spark, cfg, ds, regions),
            "event_rows": ingest.ingest_events(spark, cfg, ds),
        }


# ------------------------------------------------------------------------ VEE
def run_vee(ds: str, cfg: Config | None = None) -> dict:
    cfg = cfg or load_config()
    with spark_session("vee", ds, cfg) as spark:
        meters = ingest.read_meters(spark, cfg).cache()
        bronze = cfg.bronze("reads")
        reads = (
            spark.read.parquet(str(bronze)).filter(F.col("read_date") == ds)
            if bronze.exists()
            else spark.createDataFrame([], ingest.READ_SCHEMA)
        )
        raw_rows = reads.count()
        unknown = vee.unknown_meter_reads(reads, meters).count()
        deduped = vee.dedupe_reads(reads)
        grid = vee.expected_grid(spark, meters, ds, int(cfg.vee["intervals_per_day"]))
        intervals = vee.apply_vee(grid, deduped, cfg)
        _write_staging(intervals, cfg, "interval_reads", ds)
        return {"raw_rows": raw_rows, "duplicate_rows": raw_rows - deduped.count(), "unknown_meter_rows": unknown}


def _staged_intervals(spark, cfg: Config, ds: str):
    return spark.read.parquet(str(cfg.staging("interval_reads"))).filter(F.col("read_date") == ds).drop("read_date")


# -------------------------------------------------------------------- metrics
def build_usage(ds: str, cfg: Config | None = None) -> dict:
    cfg = cfg or load_config()
    with spark_session("usage", ds, cfg) as spark:
        intervals = _staged_intervals(spark, cfg, ds).cache()
        usage = metrics.meter_daily_usage(intervals)
        _write_staging(usage, cfg, "meter_daily_usage", ds)
        _write_staging(metrics.feeder_hourly_load(intervals), cfg, "feeder_hourly_load", ds)
        return {"meters": usage.count(), "billing_ready": usage.filter("is_billing_ready").count()}


def build_reliability(ds: str, cfg: Config | None = None) -> dict:
    cfg = cfg or load_config()
    with spark_session("reliability", ds, cfg) as spark:
        events = spark.read.parquet(str(cfg.bronze("events"))).filter(F.col("read_date") == ds)
        meters = ingest.read_meters(spark, cfg)
        outages = metrics.outage_intervals(events, ds).cache()
        indices = metrics.reliability_indices(outages, meters, float(cfg.reliability["momentary_threshold_minutes"]))
        _write_staging(outages, cfg, "outages", ds)
        _write_staging(indices, cfg, "reliability_daily", ds)
        rows = indices.orderBy("region").collect()
        return {r["region"]: {"saidi": r["saidi_minutes"], "saifi": r["saifi"]} for r in rows}


# ---------------------------------------------------------------------- audit
def audit_day(ds: str, file_checks: list[dict], vee_stats: dict, cfg: Config | None = None) -> dict:
    cfg = cfg or load_config()
    with spark_session("audit", ds, cfg) as spark:
        report = quality.audit(
            _staged_intervals(spark, cfg, ds),
            file_checks,
            vee_stats["unknown_meter_rows"],
            vee_stats["duplicate_rows"],
            cfg,
            ds,
        )
    out = cfg.report("dq", f"{ds}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    return report


# -------------------------------------------------------------------- publish
def publish_day(ds: str, cfg: Config | None = None) -> dict:
    cfg = cfg or load_config()
    for table in publish.TABLES:
        publish.promote(cfg, table, ds)
    with spark_session("publish", ds, cfg, hive=True) as spark:
        return publish.register_hive(spark, cfg)


def main() -> None:
    p = argparse.ArgumentParser(description="Run one pipeline job (for spark-submit style execution)")
    p.add_argument("job", choices=["vee", "usage", "reliability", "publish"])
    p.add_argument("--ds", required=True)
    a = p.parse_args()
    fn = {"vee": run_vee, "usage": build_usage, "reliability": build_reliability, "publish": publish_day}[a.job]
    print(json.dumps(fn(a.ds), indent=2, default=str))


if __name__ == "__main__":
    main()
