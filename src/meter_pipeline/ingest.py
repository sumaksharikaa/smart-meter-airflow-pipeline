"""Landing -> bronze: typed Parquet, partitioned like Hive, idempotent per day."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from meter_pipeline.config import Config

READ_SCHEMA = StructType([StructField(c, StringType()) for c in ["meter_id", "interval_end", "kwh", "read_quality"]])
EVENT_SCHEMA = StructType([StructField(c, StringType()) for c in ["event_id", "meter_id", "event_type", "event_ts"]])


def read_meters(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.read.option("header", "true").csv(str(cfg.meters_file))


def ingest_reads(spark: SparkSession, cfg: Config, ds: str, regions: list[str]) -> int:
    """Load the validated region files for one day into bronze.reads.

    Dynamic partition overwrite replaces only (read_date=ds, region=...)
    partitions, so re-running a day, or loading a re-sent file, never duplicates rows.
    """
    if not regions:
        return 0
    paths = [str(cfg.reads_file(ds, r)) for r in regions]
    df = (
        spark.read.option("header", "true")
        .schema(READ_SCHEMA)
        .csv(paths)
        .withColumn("region", F.regexp_extract(F.col("_metadata.file_path"), r"ami_reads_([A-Z_]+)_\d{8}\.csv", 1))
        .withColumn("interval_end", F.to_timestamp("interval_end"))
        .withColumn("kwh", F.col("kwh").cast("decimal(10,3)"))
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("read_date", F.lit(ds).cast("date"))
    )
    df.write.mode("overwrite").partitionBy("read_date", "region").parquet(str(cfg.bronze("reads")))
    return spark.read.parquet(str(cfg.bronze("reads"))).filter(F.col("read_date") == ds).count()


def ingest_events(spark: SparkSession, cfg: Config, ds: str) -> int:
    src = cfg.landing_events(ds)
    df = (
        spark.read.schema(EVENT_SCHEMA)
        .json(str(src))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .dropDuplicates(["event_id"])
        .withColumn("read_date", F.lit(ds).cast("date"))
    )
    df.write.mode("overwrite").partitionBy("read_date").parquet(str(cfg.bronze("events")))
    return df.count()
