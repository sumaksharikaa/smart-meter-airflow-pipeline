"""Failure alerting for Airflow callbacks: a JSON record on disk, plus a webhook if configured."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

from meter_pipeline.config import load_config

log = logging.getLogger(__name__)


def notify_failure(context: dict) -> None:
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    alert = {
        "at": datetime.now(timezone.utc).isoformat(),
        "dag_id": getattr(dag_run, "dag_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "task_id": getattr(ti, "task_id", None),
        "data_date": (getattr(dag_run, "conf", None) or {}).get("data_date"),
        "reason": str(context.get("exception") or context.get("reason") or "failed"),
    }
    log.error("ALERT %s", alert)
    try:
        out = load_config().report("alerts", f"{alert['run_id'] or 'run'}.json".replace(":", "_"))
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(alert, indent=2))
    except OSError:
        log.exception("Could not write alert file")

    url = os.environ.get("ALERT_WEBHOOK_URL")  # e.g. a Slack or Teams incoming webhook
    if url:
        body = json.dumps({"text": f":rotating_light: {alert['dag_id']}.{alert['task_id']} failed: {alert['reason']}"})
        req = urllib.request.Request(url, data=body.encode(), headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except OSError:
            log.exception("Webhook alert failed")
