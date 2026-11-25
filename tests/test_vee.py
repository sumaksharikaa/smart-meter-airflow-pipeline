from datetime import datetime, timedelta

from meter_pipeline import vee

DS = "2026-11-02"
METER_COLS = "meter_id string, region string, feeder_id string, service_type string, status string"


def _meters(spark):
    return spark.createDataFrame(
        [
            ("M1", "BAY_AREA", "F1", "RESIDENTIAL", "ACTIVE"),
            ("M2", "BAY_AREA", "F1", "RESIDENTIAL", "ACTIVE"),
            ("M3", "BAY_AREA", "F1", "RESIDENTIAL", "RETIRED"),
        ],
        METER_COLS,
    )


def _reads(spark, rows):
    start = datetime(2026, 11, 2)
    data = [(m, start + timedelta(minutes=15 * (i + 1)), kwh, "f.csv") for m, i, kwh in rows]
    return spark.createDataFrame(data, "meter_id string, interval_end timestamp, kwh double, source_file string")


def _run(spark, cfg, rows):
    grid = vee.expected_grid(spark, _meters(spark), DS, cfg.vee["intervals_per_day"])
    out = vee.apply_vee(grid, vee.dedupe_reads(_reads(spark, rows)), cfg)
    return {
        (r.meter_id, r.interval_idx): (float(r.kwh) if r.kwh is not None else None, r.quality_flag)
        for r in out.collect()
    }


def test_grid_covers_only_active_meters(spark, small_cfg):
    grid = vee.expected_grid(spark, _meters(spark), DS, 96)
    assert grid.count() == 2 * 96
    first = grid.filter("meter_id = 'M1' and interval_idx = 0").first()
    last = grid.filter("meter_id = 'M1' and interval_idx = 95").first()
    assert first.interval_end == datetime(2026, 11, 2, 0, 15)
    assert last.interval_end == datetime(2026, 11, 3, 0, 0)  # the interval ending at midnight belongs to the day


def test_interpolation_editing_and_missing(spark, small_cfg):
    rows = [
        ("M1", 0, 1.0),
        ("M1", 1, 2.0),  # idx 2, 3 missing -> interpolate 3.0, 4.0
        ("M1", 4, 5.0),
        ("M1", 5, 99.0),  # idx 5 spike (> 15 kWh residential) -> edited to 5.5
        ("M1", 6, 6.0),
        ("M1", 7, -1.0),  # idx 7 negative at end of day -> carry 6.0
        ("M1", 0, 1.0),  # duplicate re-send
    ]
    out = _run(spark, small_cfg, rows)
    assert out[("M1", 0)] == (1.0, "ACTUAL")
    assert out[("M1", 2)] == (3.0, "ESTIMATED") and out[("M1", 3)] == (4.0, "ESTIMATED")
    assert out[("M1", 5)] == (5.5, "EDITED")
    assert out[("M1", 7)] == (6.0, "EDITED")
    # M2 never reported: every interval missing, nothing invented
    assert all(out[("M2", i)] == (None, "MISSING") for i in range(8))


def test_gap_longer_than_limit_stays_missing(spark, small_cfg):
    rows = [("M1", 0, 1.0), ("M1", 4, 5.0), ("M1", 5, 1.0), ("M1", 6, 1.0), ("M1", 7, 1.0)]  # gap of 3 > 2
    out = _run(spark, small_cfg, rows)
    assert [out[("M1", i)][1] for i in (1, 2, 3)] == ["MISSING"] * 3


def test_zero_during_outage_is_a_valid_actual(spark, small_cfg):
    rows = [("M1", i, 0.0 if i in (3, 4) else 1.0) for i in range(8)]
    out = _run(spark, small_cfg, rows)
    assert out[("M1", 3)] == (0.0, "ACTUAL")


def test_unknown_meters(spark):
    reads = _reads(spark, [("M1", 0, 1.0), ("MTR9999999", 0, 0.25)])
    assert [r.meter_id for r in vee.unknown_meter_reads(reads, _meters(spark)).collect()] == ["MTR9999999"]
