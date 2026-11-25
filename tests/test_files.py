import hashlib
import json

from meter_pipeline.files import files_ready, validate_region_file

DS = "2026-11-02"
HEADER = "meter_id,interval_end,kwh,read_quality\n"


def _deliver(cfg, region, body, declared_rows=None, md5_of=None):
    csv = cfg.reads_file(DS, region)
    csv.parent.mkdir(parents=True, exist_ok=True)
    csv.write_text(body)
    rows = body.count("\n") - 1
    ctl = {
        "file_name": csv.name,
        "row_count": rows if declared_rows is None else declared_rows,
        "md5": hashlib.md5((md5_of or body).encode()).hexdigest(),
    }
    csv.with_suffix(".ctl").write_text(json.dumps(ctl))
    return csv


def test_valid_file(cfg):
    _deliver(cfg, "BAY_AREA", HEADER + "MTR1,2026-11-02 00:15:00,0.250,A\n")
    check = validate_region_file(cfg, DS, "BAY_AREA")
    assert check.status == "VALID" and check.rows == 1


def test_truncated_file_is_rejected_and_quarantined(cfg):
    full = HEADER + "MTR1,2026-11-02 00:15:00,0.250,A\nMTR1,2026-11-02 00:30:00,0.300,A\n"
    csv = _deliver(cfg, "SIERRA", HEADER + "MTR1,2026-11-02 00:15:00,0.250,A\n", declared_rows=2, md5_of=full)
    check = validate_region_file(cfg, DS, "SIERRA")
    assert check.status == "REJECTED" and "row count 1 != control file 2" in check.reason
    assert not csv.exists()
    assert (cfg.rejected(DS) / csv.name).exists()
    assert (cfg.rejected(DS) / f"{csv.stem}.reason.txt").read_text().startswith("row count")


def test_checksum_mismatch_with_same_row_count(cfg):
    body = HEADER + "MTR1,2026-11-02 00:15:00,0.250,A\n"
    _deliver(cfg, "NORTH_COAST", body, md5_of=body.replace("0.250", "0.260"))
    check = validate_region_file(cfg, DS, "NORTH_COAST", quarantine=False)
    assert check.status == "REJECTED" and "MD5" in check.reason


def test_wrong_header(cfg):
    _deliver(cfg, "BAY_AREA", "meter,ts,value,q\nMTR1,2026-11-02 00:15:00,0.250,A\n")
    assert validate_region_file(cfg, DS, "BAY_AREA", quarantine=False).status == "REJECTED"


def test_missing_file_and_sensor_readiness(cfg):
    assert validate_region_file(cfg, DS, "SIERRA").status == "MISSING"
    for region in cfg.regions:
        _deliver(cfg, region, HEADER + "MTR1,2026-11-02 00:15:00,0.250,A\n")
    assert files_ready(cfg, DS) == []  # events not landed yet
    ev = cfg.landing_events(DS)
    ev.mkdir(parents=True)
    (ev / "meter_events_20261102.jsonl").write_text("")
    assert files_ready(cfg, DS) == cfg.regions
