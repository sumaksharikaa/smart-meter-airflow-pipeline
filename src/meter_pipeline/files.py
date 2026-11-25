"""
File-level validation of head-end deliveries (no Spark needed).

Each region's CSV arrives with a control file declaring its row count and
MD5. A file is accepted only if it exists, its header is the expected one,
and both the row count and the checksum match the control file. Anything
else is moved to the rejected zone with a reason, so a truncated or
corrupted transfer never reaches the warehouse.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import asdict, dataclass

from meter_pipeline.config import Config


@dataclass
class FileCheck:
    region: str
    file: str
    status: str  # VALID | REJECTED | MISSING
    reason: str = ""
    rows: int = 0
    declared_rows: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def files_ready(cfg: Config, ds: str) -> list[str]:
    """Regions whose CSV and control file have both landed (used by the sensor)."""
    ready = []
    for region in cfg.regions:
        csv = cfg.reads_file(ds, region)
        if csv.exists() and csv.with_suffix(".ctl").exists():
            ready.append(region)
    events = list(cfg.landing_events(ds).glob("*.jsonl")) if cfg.landing_events(ds).exists() else []
    return ready if events else []


def validate_region_file(cfg: Config, ds: str, region: str, quarantine: bool = True) -> FileCheck:
    csv = cfg.reads_file(ds, region)
    ctl = csv.with_suffix(".ctl")
    if not csv.exists() or not ctl.exists():
        return FileCheck(region, csv.name, "MISSING", "data or control file not delivered")

    data = csv.read_bytes()
    control = json.loads(ctl.read_text())
    lines = data.decode().splitlines()
    header = lines[0].split(",") if lines else []
    rows = max(0, len(lines) - 1)
    check = FileCheck(region, csv.name, "VALID", rows=rows, declared_rows=int(control["row_count"]))

    if header != cfg.expected_header:
        check.status, check.reason = "REJECTED", f"unexpected header {header}"
    elif rows != check.declared_rows:
        check.status, check.reason = "REJECTED", f"row count {rows} != control file {check.declared_rows}"
    elif hashlib.md5(data).hexdigest() != control["md5"]:
        check.status, check.reason = "REJECTED", "MD5 checksum does not match control file"

    if check.status == "REJECTED" and quarantine:
        target = cfg.rejected(ds)
        target.mkdir(parents=True, exist_ok=True)
        for f in (csv, ctl):
            shutil.move(str(f), str(target / f.name))
        (target / f"{csv.stem}.reason.txt").write_text(check.reason + "\n")
    return check


def valid_regions(checks: list[dict]) -> list[str]:
    return [c["region"] for c in checks if c["status"] == "VALID"]
