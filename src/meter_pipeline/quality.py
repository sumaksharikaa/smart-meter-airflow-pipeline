"""Audit step of write-audit-publish: measure the staged day and decide whether it may be published."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from meter_pipeline.config import Config


def audit(
    intervals: DataFrame, file_checks: list[dict], unknown_rows: int, duplicate_rows: int, cfg: Config, ds: str
) -> dict:
    by_region = (
        intervals.groupBy("region")
        .agg(
            F.count("*").alias("expected"),
            *[
                F.sum(F.when(F.col("quality_flag") == q, 1).otherwise(0)).alias(q.lower())
                for q in ("ACTUAL", "EDITED", "ESTIMATED", "MISSING")
            ],
        )
        .collect()
    )
    regions = {}
    for r in by_region:
        usable = r["actual"] + r["edited"] + r["estimated"]
        regions[r["region"]] = {
            "expected_intervals": r["expected"],
            "actual": r["actual"],
            "edited": r["edited"],
            "estimated": r["estimated"],
            "missing": r["missing"],
            "completeness": round(usable / r["expected"], 5) if r["expected"] else 0.0,
            "actual_rate": round(r["actual"] / r["expected"], 5) if r["expected"] else 0.0,
        }

    threshold = float(cfg.quality_gate["min_region_completeness"])
    failures = [
        f"{region}: completeness {m['completeness']:.2%} below {threshold:.0%}"
        for region, m in sorted(regions.items())
        if m["completeness"] < threshold
    ]
    rejected = [c for c in file_checks if c["status"] != "VALID"]
    if rejected and not cfg.quality_gate["allow_rejected_files"]:
        failures += [f"{c['region']}: file {c['status'].lower()} ({c['reason']})" for c in rejected]

    return {
        "read_date": ds,
        "regions": regions,
        "files": file_checks,
        "unknown_meter_rows": unknown_rows,
        "duplicate_rows_removed": duplicate_rows,
        "passed": not failures,
        "failures": failures,
    }
