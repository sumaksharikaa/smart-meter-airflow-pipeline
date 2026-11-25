"""
VEE: Validation, Estimation and Editing of interval reads.

VEE is the standard utility process that turns raw AMI reads into billing-
and analytics-grade data. For every active meter, every 15-minute interval
of the day must end up with a value and a quality flag:

    ACTUAL     read received and valid
    EDITED     read received but invalid (negative, or above the physical
               maximum for the service type), replaced with an estimate
    ESTIMATED  no read received; gap short enough to interpolate linearly
               between the valid reads on either side (or carried from the
               nearest read at the start/end of the day)
    MISSING    gap too long to estimate reliably; value left null

All of it runs as Spark window functions: no Python UDFs, no collect().
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from meter_pipeline.config import Config

QUALITY_FLAGS = ["ACTUAL", "EDITED", "ESTIMATED", "MISSING"]


def expected_grid(spark: SparkSession, meters: DataFrame, ds: str, intervals: int) -> DataFrame:
    """Every active meter x every interval of the day: what *should* exist."""
    idx = spark.range(intervals).select(F.col("id").cast("int").alias("interval_idx"))
    day_start = F.to_timestamp(F.lit(f"{ds} 00:00:00"))
    return (
        meters.filter(F.col("status") == "ACTIVE")
        .select("meter_id", "region", "feeder_id", "service_type")
        .crossJoin(idx)
        .withColumn("interval_end", day_start + F.make_dt_interval(mins=(F.col("interval_idx") + 1) * 15))
    )


def dedupe_reads(reads: DataFrame) -> DataFrame:
    """Head-ends re-send rows; keep one read per meter and interval."""
    w = Window.partitionBy("meter_id", "interval_end").orderBy(F.col("source_file"))
    return reads.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def apply_vee(grid: DataFrame, reads: DataFrame, cfg: Config) -> DataFrame:
    max_gap = int(cfg.vee["max_estimation_gap"])
    limits = cfg.vee["max_kwh_per_interval"]
    max_kwh = F.create_map(*[x for k, v in limits.items() for x in (F.lit(k), F.lit(float(v)))])

    df = grid.join(
        reads.select("meter_id", "interval_end", F.col("kwh").cast("double").alias("kwh_raw")),
        ["meter_id", "interval_end"],
        "left",
    )
    is_valid = (
        F.col("kwh_raw").isNotNull() & (F.col("kwh_raw") >= 0) & (F.col("kwh_raw") <= max_kwh[F.col("service_type")])
    )
    df = (
        df.withColumn("_valid", is_valid)
        .withColumn("_val", F.when(F.col("_valid"), F.col("kwh_raw")))
        .withColumn("_vidx", F.when(F.col("_valid"), F.col("interval_idx")))
    )

    by_meter = Window.partitionBy("meter_id").orderBy("interval_idx")
    before = by_meter.rowsBetween(Window.unboundedPreceding, -1)
    after = by_meter.rowsBetween(1, Window.unboundedFollowing)
    df = (
        df.withColumn("_prev_idx", F.last("_vidx", ignorenulls=True).over(before))
        .withColumn("_prev_val", F.last("_val", ignorenulls=True).over(before))
        .withColumn("_next_idx", F.first("_vidx", ignorenulls=True).over(after))
        .withColumn("_next_val", F.first("_val", ignorenulls=True).over(after))
    )

    gap = F.col("_next_idx") - F.col("_prev_idx") - 1
    interpolated = F.col("_prev_val") + (F.col("_next_val") - F.col("_prev_val")) * (
        F.col("interval_idx") - F.col("_prev_idx")
    ) / (F.col("_next_idx") - F.col("_prev_idx"))
    estimate = (
        F.when(F.col("_prev_idx").isNotNull() & F.col("_next_idx").isNotNull() & (gap <= max_gap), interpolated)
        # start or end of the day: carry the nearest valid read if it is close enough
        .when(F.col("_prev_idx").isNull() & (F.col("_next_idx") - F.col("interval_idx") <= max_gap), F.col("_next_val"))
        .when(F.col("_next_idx").isNull() & (F.col("interval_idx") - F.col("_prev_idx") <= max_gap), F.col("_prev_val"))
    )

    kwh = F.when(F.col("_valid"), F.col("kwh_raw")).otherwise(estimate)
    flag = (
        F.when(F.col("_valid"), "ACTUAL")
        .when(estimate.isNull(), "MISSING")
        .when(F.col("kwh_raw").isNotNull(), "EDITED")
        .otherwise("ESTIMATED")
    )
    return df.select(
        "meter_id",
        "region",
        "feeder_id",
        "service_type",
        "interval_idx",
        "interval_end",
        F.col("kwh_raw").cast("decimal(10,3)").alias("kwh_raw"),
        F.round(kwh, 3).cast("decimal(10,3)").alias("kwh"),
        flag.alias("quality_flag"),
    )


def unknown_meter_reads(reads: DataFrame, meters: DataFrame) -> DataFrame:
    return reads.join(meters.select("meter_id"), "meter_id", "left_anti")
