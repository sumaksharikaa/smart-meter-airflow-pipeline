from datetime import datetime, timedelta

import pytest

from meter_pipeline import metrics, quality

DS = "2026-11-02"


def _meters(spark):
    return spark.createDataFrame(
        [(f"M{i}", "BAY_AREA" if i <= 4 else "SIERRA", "ACTIVE") for i in range(1, 9)],
        "meter_id string, region string, status string",
    )


def _events(spark, rows):
    base = datetime(2026, 11, 2)
    return spark.createDataFrame(
        [(m, t, base + timedelta(minutes=mins)) for m, t, mins in rows],
        "meter_id string, event_type string, event_ts timestamp",
    )


def test_outage_pairing_and_ongoing(spark):
    ev = _events(
        spark,
        [
            ("M1", "POWER_OFF", 60),
            ("M1", "POWER_ON", 120),  # 60 min
            ("M2", "POWER_OFF", 600),
            ("M2", "TAMPER", 610),  # tamper ignored
            ("M2", "POWER_ON", 603),  # 3 min -> momentary
            ("M3", "POWER_OFF", 1380),  # never restored -> ONGOING to midnight
        ],
    )
    out = {(r.meter_id, r.outage_status): r.duration_minutes for r in metrics.outage_intervals(ev, DS).collect()}
    assert out == {("M1", "RESTORED"): 60.0, ("M2", "RESTORED"): 3.0, ("M3", "ONGOING"): 60.0}


def test_reliability_indices_ieee_1366(spark):
    ev = _events(
        spark,
        [
            ("M1", "POWER_OFF", 60),
            ("M1", "POWER_ON", 120),  # sustained 60
            ("M2", "POWER_OFF", 60),
            ("M2", "POWER_ON", 90),  # sustained 30
            ("M3", "POWER_OFF", 60),
            ("M3", "POWER_ON", 62),  # momentary 2
        ],
    )
    out = metrics.reliability_indices(metrics.outage_intervals(ev, DS), _meters(spark), 5)
    r = {x.region: x for x in out.collect()}
    bay = r["BAY_AREA"]
    assert bay.customers_served == 4
    assert bay.saifi == pytest.approx(2 / 4)
    assert bay.saidi_minutes == pytest.approx(90 / 4)
    assert bay.caidi_minutes == pytest.approx(45.0)  # average restoration time
    assert bay.maifi == pytest.approx(1 / 4)
    assert r["SIERRA"].saifi == 0 and r["SIERRA"].caidi_minutes is None


def test_daily_usage_and_feeder_peak(spark):
    rows = [
        (
            "M1",
            "BAY_AREA",
            "F1",
            "RESIDENTIAL",
            i,
            datetime(2026, 11, 2) + timedelta(minutes=15 * (i + 1)),
            float(i),
            "ACTUAL",
        )
        for i in range(8)
    ]
    rows[2] = (*rows[2][:6], None, "MISSING")
    iv = spark.createDataFrame(
        rows,
        "meter_id string, region string, feeder_id string, service_type string, "
        "interval_idx int, interval_end timestamp, kwh double, quality_flag string",
    )
    u = metrics.meter_daily_usage(iv).first()
    assert float(u.total_kwh) == sum(range(8)) - 2 and u.peak_interval_kwh == 7.0
    assert u.intervals_missing == 1 and not u.is_billing_ready
    load = {r.hour: r for r in metrics.feeder_hourly_load(iv).collect()}
    assert float(load[1].kwh) == 4 + 5 + 6 + 7 and load[1].is_peak_hour


def test_audit_gate(spark, cfg):
    flags = ["ACTUAL"] * 98 + ["MISSING"] * 2
    iv = spark.createDataFrame(
        [("SIERRA", f) for f in flags] + [("BAY_AREA", "MISSING")] * 10, "region string, quality_flag string"
    )
    checks = [
        {"region": "SIERRA", "status": "VALID", "reason": ""},
        {"region": "BAY_AREA", "status": "REJECTED", "reason": "row count 1 != control file 2"},
    ]
    report = quality.audit(iv, checks, 0, 0, cfg, DS)
    assert report["regions"]["SIERRA"]["completeness"] == 0.98
    assert not report["passed"]
    assert any("BAY_AREA: completeness 0.00%" in f for f in report["failures"])
    assert any("file rejected" in f for f in report["failures"])
    ok = quality.audit(iv.filter("region = 'SIERRA'"), checks[:1], 0, 0, cfg, DS)
    assert ok["passed"]  # exactly at the 98% threshold passes
