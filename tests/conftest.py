import dataclasses
import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def cfg(tmp_path):
    from meter_pipeline.config import load_config

    return load_config(root=tmp_path)


@pytest.fixture
def small_cfg(cfg):
    """8 intervals per day, gaps of up to 2 intervals estimated."""
    vee = {**cfg.vee, "intervals_per_day": 8, "max_estimation_gap": 2}
    return dataclasses.replace(cfg, vee=vee)
