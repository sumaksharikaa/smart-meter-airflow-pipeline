"""
Synthetic smart-meter (AMI) data, delivered the way utility head-end systems deliver it.

Per day:
    landing/ami/<date>/ami_reads_<REGION>_<YYYYMMDD>.csv   15-minute interval reads, one file per region
    landing/ami/<date>/ami_reads_<REGION>_<YYYYMMDD>.ctl   control file: expected row count + MD5
    landing/events/<date>/meter_events_<YYYYMMDD>.jsonl    power-off / power-on / tamper events
Once:
    reference/meters.csv                                   asset master (meter -> transformer -> feeder)

The data carries the problems a real AMI feed has: communication gaps,
meters missing a whole day, re-sent duplicate rows, impossible spikes,
negative reads, reads from meters not in the asset master, feeder outages
(zero usage plus power-off/on events) and momentary blips. With --truncate,
one region's file arrives cut short in transfer so it no longer matches its
control file. --resend then delivers the corrected file.

Every day is generated from its own seed, so a resend reproduces the same
complete file. All data is fake.

Usage:
    python data_generator/generate_ami_data.py --root data --start-date 2026-11-02 --days 3 \
        --truncate NORTH_COAST:2026-11-03
    python data_generator/generate_ami_data.py --root data --resend NORTH_COAST:2026-11-03
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
from datetime import date, datetime, timedelta
from pathlib import Path

REGIONS = ["BAY_AREA", "CENTRAL_VALLEY", "NORTH_COAST", "SIERRA"]
INTERVALS_PER_DAY = 96


def build_meters(seed: int, n_meters: int) -> list[dict]:
    rng = random.Random(seed)
    meters = []
    feeders = {r: [f"{r[:3]}-FDR-{i:02d}" for i in range(1, 11)] for r in REGIONS}
    for i in range(1, n_meters + 1):
        region = rng.choice(REGIONS)
        feeder = rng.choice(feeders[region])
        service = "COMMERCIAL" if rng.random() < 0.12 else "RESIDENTIAL"
        meters.append(
            {
                "meter_id": f"MTR{i:07d}",
                "premise_id": f"PRM{rng.randint(1_000_000, 9_999_999)}",
                "service_type": service,
                "region": region,
                "substation": f"{region[:3]}-SUB-{feeder[-2:]}",
                "feeder_id": feeder,
                "transformer_id": f"{feeder}-XFMR-{rng.randint(1, 40):03d}",
                "install_date": (date(2015, 1, 1) + timedelta(days=rng.randint(0, 3000))).isoformat(),
                "status": "ACTIVE" if rng.random() > 0.01 else "RETIRED",
            }
        )
    return meters


def load_shape(service: str, interval: int, base: float, rng: random.Random) -> float:
    hour = interval / 4
    if service == "COMMERCIAL":
        on = 7 <= hour < 19
        kwh = base * (4.0 if on else 1.2)
    else:
        morning = math.exp(-((hour - 7.5) ** 2) / 3)
        evening = 1.8 * math.exp(-((hour - 19) ** 2) / 5)
        kwh = base * (0.5 + morning + evening)
    return round(max(0.0, kwh * rng.uniform(0.85, 1.15)), 3)


def simulate_day(meters: list[dict], day: date, seed: int) -> tuple[dict[str, list[dict]], list[dict]]:
    rng = random.Random(f"{seed}-{day.isoformat()}")
    start = datetime.combine(day, datetime.min.time())
    active = [m for m in meters if m["status"] == "ACTIVE"]

    # Outages: whole feeders (storm, equipment) and single meters. Durations in minutes.
    outages: dict[str, list[tuple[datetime, datetime]]] = {}
    feeders = sorted({m["feeder_id"] for m in active})
    for feeder in rng.sample(feeders, k=rng.randint(1, 3)):
        off = start + timedelta(minutes=rng.randint(60, 1200))
        dur = rng.choice([2, 3, 25, 45, 90, 180, 240])  # 2-3 min = momentary
        for m in active:
            if m["feeder_id"] == feeder:
                outages.setdefault(m["meter_id"], []).append((off, off + timedelta(minutes=dur)))
    for m in rng.sample(active, k=max(1, len(active) // 200)):
        off = start + timedelta(minutes=rng.randint(0, 1300))
        outages.setdefault(m["meter_id"], []).append((off, off + timedelta(minutes=rng.randint(10, 120))))

    files: dict[str, list[dict]] = {r: [] for r in REGIONS}
    events: list[dict] = []
    for m in active:
        if rng.random() < 0.003:
            continue  # meter did not report at all today
        base = rng.uniform(0.08, 0.35) if m["service_type"] == "RESIDENTIAL" else rng.uniform(0.8, 2.5)
        gap = None
        if rng.random() < 0.03:  # communication gap of 1-12 intervals
            g0 = rng.randint(0, INTERVALS_PER_DAY - 1)
            gap = range(g0, min(INTERVALS_PER_DAY, g0 + rng.randint(1, 12)))
        for i in range(INTERVALS_PER_DAY):
            if gap and i in gap:
                continue
            ts_end = start + timedelta(minutes=15 * (i + 1))
            kwh = load_shape(m["service_type"], i, base, rng)
            for off, on in outages.get(m["meter_id"], []):
                if off < ts_end and on > ts_end - timedelta(minutes=15):
                    kwh = 0.0
            r = rng.random()
            if r < 0.0005:
                kwh = round(kwh * 50 + 20, 3)  # impossible spike
            elif r < 0.0007:
                kwh = -abs(kwh) - 0.5  # negative read
            files[m["region"]].append(
                {
                    "meter_id": m["meter_id"],
                    "interval_end": ts_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "kwh": f"{kwh:.3f}",
                    "read_quality": "A",
                }
            )
        for off, on in outages.get(m["meter_id"], []):
            for kind, ts in (("POWER_OFF", off), ("POWER_ON", on)):
                if ts.date() == day:
                    events.append(
                        {
                            "event_id": f"EV{rng.getrandbits(48):012x}",
                            "meter_id": m["meter_id"],
                            "event_type": kind,
                            "event_ts": (ts + timedelta(seconds=rng.randint(0, 20))).strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
        if rng.random() < 0.0005:
            events.append(
                {
                    "event_id": f"EV{rng.getrandbits(48):012x}",
                    "meter_id": m["meter_id"],
                    "event_type": "TAMPER",
                    "event_ts": (start + timedelta(minutes=rng.randint(0, 1439))).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    for rows in files.values():
        dupes = rng.sample(rows, k=max(1, len(rows) // 500))  # head-end re-sends
        rows.extend(dict(x) for x in dupes)
        rows.append(
            {
                "meter_id": "MTR9999999",
                "interval_end": (start + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
                "kwh": "0.250",
                "read_quality": "A",
            }
        )  # meter not in the asset master
        rng.shuffle(rows)
    events.sort(key=lambda e: e["event_ts"])
    return files, events


def _write_reads(path: Path, rows: list[dict], declared_rows: list[dict] | None = None) -> None:
    """Write the CSV and its control file. declared_rows, when given, is what the
    control file describes (used to simulate a file truncated in transfer)."""

    def render(rs: list[dict]) -> str:
        out = ["meter_id,interval_end,kwh,read_quality"]
        out += [f"{r['meter_id']},{r['interval_end']},{r['kwh']},{r['read_quality']}" for r in rs]
        return "\n".join(out) + "\n"

    path.parent.mkdir(parents=True, exist_ok=True)
    body = render(rows)
    declared = render(declared_rows) if declared_rows is not None else body
    path.write_text(body)
    ctl = {
        "file_name": path.name,
        "row_count": len(declared_rows if declared_rows is not None else rows),
        "md5": hashlib.md5(declared.encode()).hexdigest(),
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
    }
    path.with_suffix(".ctl").write_text(json.dumps(ctl, indent=2) + "\n")


def generate(
    root: Path,
    start: date,
    days: int,
    seed: int,
    n_meters: int,
    truncate: set[tuple[str, str]],
    resend: set[tuple[str, str]],
) -> None:
    meters = build_meters(seed, n_meters)
    ref = root / "reference" / "meters.csv"
    ref.parent.mkdir(parents=True, exist_ok=True)
    with ref.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(meters[0]))
        w.writeheader()
        w.writerows(meters)

    day_list = (
        [start + timedelta(days=i) for i in range(days)]
        if not resend
        else sorted({date.fromisoformat(d) for _, d in resend})
    )
    for day in day_list:
        files, events = simulate_day(meters, day, seed)
        tag = day.strftime("%Y%m%d")
        for region, rows in files.items():
            key = (region, day.isoformat())
            if resend and key not in resend:
                continue
            path = root / "landing" / "ami" / day.isoformat() / f"ami_reads_{region}_{tag}.csv"
            if key in truncate:
                _write_reads(path, rows[: len(rows) - 500], declared_rows=rows)
                print(f"  {region} {day}: TRUNCATED in transfer ({len(rows) - 500} of {len(rows)} rows)")
            else:
                _write_reads(path, rows)
        if not resend:
            ev = root / "landing" / "events" / day.isoformat() / f"meter_events_{tag}.jsonl"
            ev.parent.mkdir(parents=True, exist_ok=True)
            ev.write_text("".join(json.dumps(e) + "\n" for e in events))
        print(
            f"{day}: reads={sum(len(r) for r in files.values()):>7} events={len(events):>5}"
            + (" (resend)" if resend else "")
        )


def _pairs(values: list[str]) -> set[tuple[str, str]]:
    out = set()
    for v in values or []:
        region, day = v.split(":")
        out.add((region.upper(), day))
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--root", type=Path, default=Path("data"))
    p.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 11, 2))
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--seed", type=int, default=11)
    p.add_argument("--meters", type=int, default=2000)
    p.add_argument("--truncate", nargs="*", help="REGION:YYYY-MM-DD files to cut short in transfer")
    p.add_argument("--resend", nargs="*", help="REGION:YYYY-MM-DD files to deliver again, complete")
    a = p.parse_args()
    generate(a.root, a.start_date, a.days, a.seed, a.meters, _pairs(a.truncate), _pairs(a.resend))


if __name__ == "__main__":
    main()
