"""Pipeline configuration and the folder layout of the data root."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


@dataclass(frozen=True)
class Config:
    root: Path
    regions: list[str]
    expected_header: list[str]
    vee: dict[str, Any]
    reliability: dict[str, Any]
    quality_gate: dict[str, Any]

    # ------------------------------------------------------------------ zones
    def landing_reads(self, ds: str) -> Path:
        return self.root / "landing" / "ami" / ds

    def landing_events(self, ds: str) -> Path:
        return self.root / "landing" / "events" / ds

    def rejected(self, ds: str) -> Path:
        return self.root / "rejected" / ds

    @property
    def meters_file(self) -> Path:
        return self.root / "reference" / "meters.csv"

    def bronze(self, table: str) -> Path:
        return self.root / "bronze" / table

    def staging(self, table: str) -> Path:
        """Write-audit-publish: tables are written here, audited, then published."""
        return self.root / "staging" / table

    def curated(self, table: str) -> Path:
        return self.root / "curated" / table

    def report(self, kind: str, name: str) -> Path:
        return self.root / "reports" / kind / name

    @property
    def hive_dir(self) -> Path:
        return self.root / "hive"

    def reads_file(self, ds: str, region: str) -> Path:
        return self.landing_reads(ds) / f"ami_reads_{region}_{ds.replace('-', '')}.csv"


def load_config(path: str | Path | None = None, root: str | Path | None = None) -> Config:
    raw = yaml.safe_load(Path(path or os.environ.get("AMI_CONFIG") or DEFAULT_CONFIG).read_text())
    data_root = Path(root or os.environ.get("AMI_DATA_ROOT") or raw["data_root"]).resolve()
    return Config(
        root=data_root,
        regions=list(raw["regions"]),
        expected_header=list(raw["files"]["expected_header"]),
        vee=raw["vee"],
        reliability=raw["reliability"],
        quality_gate=raw["quality_gate"],
    )
