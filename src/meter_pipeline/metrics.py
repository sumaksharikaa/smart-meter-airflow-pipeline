"""
Daily usage, feeder load profiles and distribution reliability indices.

Reliability follows IEEE 1366:
    SAIFI  System Average Interruption Frequency Index  = customer interruptions / customers served
    SAIDI  System Average Interruption Duration Index   = customer minutes interrupted / customers served
    CAIDI  Customer Average Interruption Duration Index = SAIDI / SAIFI (average restoration time)
    MAIFI  Momentary Average Interruption Frequency     = momentary interruptions / customers served
Interruptions shorter than the momentary threshold (5 minutes) count only toward MAIFI.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def meter_daily_usage(intervals: DataFrame) -> DataFrame:
    w = Window.partitionBy("meter_id").orderBy(F.col("kwh").desc_nulls_last(), "interval_idx")
    peak = (
        intervals.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select("meter_id", F.col("kwh").alias("peak_interval_kwh"), F.col("interval_end").alias("peak_interval_end"))
    )
    counts = [
        F.sum(F.when(F.col("quality_flag") == q, 1).otherwise(0)).alias(f"intervals_{q.lower()}")
        for q in ("ACTUAL", "EDITED", "ESTIMATED", "MISSING")
    ]
    daily = intervals.groupBy("meter_id", "region", "feeder_id", "service_type").agg(
        F.sum("kwh").cast("decimal(12,3)").alias("total_kwh"), *counts
    )
    return daily.join(peak, "meter_id").withColumn("is_billing_ready", F.col("intervals_missing") == 0)


def feeder_hourly_load(intervals: DataFrame) -> DataFrame:
    hourly = (
        intervals.withColumn("hour", F.floor(F.col("interval_idx") / 4).cast("int"))
        .groupBy("region", "feeder_id", "hour")
        .agg(
            F.sum("kwh").cast("decimal(14,3)").alias("kwh"),
            F.countDistinct("meter_id").alias("meters"),
        )
    )
    w = Window.partitionBy("feeder_id").orderBy(F.col("kwh").desc())
    # kW demand for an hour = kWh delivered in that hour
    return hourly.withColumn("demand_kw", F.col("kwh")).withColumn("is_peak_hour", F.row_number().over(w) == 1)


def outage_intervals(events: DataFrame, ds: str) -> DataFrame:
    """Pair each POWER_OFF with the meter's next POWER_ON. An outage with no
    restoration by the end of the day is closed at midnight and marked ONGOING."""
    power = events.filter(F.col("event_type").isin("POWER_OFF", "POWER_ON"))
    w = Window.partitionBy("meter_id").orderBy("event_ts")
    paired = (
        power.withColumn("_next_type", F.lead("event_type").over(w))
        .withColumn("_next_ts", F.lead("event_ts").over(w))
        .filter(F.col("event_type") == "POWER_OFF")
    )
    day_end = F.to_timestamp(F.lit(f"{ds} 00:00:00")) + F.expr("INTERVAL 1 DAY")
    restored = F.when(F.col("_next_type") == "POWER_ON", F.col("_next_ts"))
    return paired.select(
        "meter_id",
        F.col("event_ts").alias("outage_start"),
        F.coalesce(restored, day_end).alias("outage_end"),
        F.when(restored.isNull(), "ONGOING").otherwise("RESTORED").alias("outage_status"),
    ).withColumn(
        "duration_minutes",
        F.round((F.unix_timestamp("outage_end") - F.unix_timestamp("outage_start")) / 60, 2),
    )


def reliability_indices(outages: DataFrame, meters: DataFrame, momentary_minutes: float) -> DataFrame:
    served = meters.filter(F.col("status") == "ACTIVE").groupBy("region").agg(F.count("*").alias("customers_served"))
    o = outages.join(meters.select("meter_id", "region"), "meter_id")
    sustained = F.col("duration_minutes") >= momentary_minutes
    agg = o.groupBy("region").agg(
        F.sum(F.when(sustained, 1).otherwise(0)).alias("customer_interruptions"),
        F.sum(F.when(sustained, F.col("duration_minutes")).otherwise(0)).alias("customer_minutes_interrupted"),
        F.sum(F.when(~sustained, 1).otherwise(0)).alias("momentary_interruptions"),
    )
    r = served.join(agg, "region", "left").fillna(0)
    return (
        r.withColumn("saifi", F.round(F.col("customer_interruptions") / F.col("customers_served"), 5))
        .withColumn("saidi_minutes", F.round(F.col("customer_minutes_interrupted") / F.col("customers_served"), 4))
        .withColumn(
            "caidi_minutes",
            F.when(F.col("saifi") > 0, F.round(F.col("saidi_minutes") / F.col("saifi"), 2)),
        )
        .withColumn("maifi", F.round(F.col("momentary_interruptions") / F.col("customers_served"), 5))
    )
