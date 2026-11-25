"""
Publish step of write-audit-publish.

Staged partitions for the day are moved into the curated zone, replacing
any earlier version of the same day, and registered in the Hive metastore so
Hive, Spark SQL, Presto/Trino or any other engine on the metastore can query them.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from meter_pipeline.config import Config

# table -> partition columns (the first is always read_date)
TABLES = {
    "interval_reads": ["read_date", "region"],
    "meter_daily_usage": ["read_date"],
    "feeder_hourly_load": ["read_date"],
    "outages": ["read_date"],
    "reliability_daily": ["read_date"],
}
HIVE_DB = "ami"


def promote(cfg: Config, table: str, ds: str) -> Path:
    src = cfg.staging(table) / f"read_date={ds}"
    dst = cfg.curated(table) / f"read_date={ds}"
    if not src.exists():
        raise FileNotFoundError(f"Nothing staged for {table} on {ds}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        shutil.rmtree(dst)  # re-publishing a day replaces it (e.g. after a corrected file was re-sent)
    shutil.move(str(src), str(dst))
    return dst


def register_hive(spark: SparkSession, cfg: Config) -> dict[str, int]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    partitions = {}
    for table in TABLES:
        location = cfg.curated(table)
        if not location.exists():
            continue
        # Schema and partition columns (read_date, region) are inferred from the
        # Hive-style folders under the location.
        spark.sql(f"CREATE TABLE IF NOT EXISTS {HIVE_DB}.{table} USING parquet LOCATION '{location}'")
        spark.sql(f"ALTER TABLE {HIVE_DB}.{table} RECOVER PARTITIONS")
        partitions[table] = spark.sql(f"SHOW PARTITIONS {HIVE_DB}.{table}").count()
    return partitions
