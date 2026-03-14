"""
Settings Sync DAG
=================
Triggered externally (via Airflow API) when settings change in the backend.

Reads the 'schedule' settings document from MongoDB and writes
~/airflow/schedule_config.json, which the daily_update and sheet_editor
DAGs read at parse time to determine their cron schedules.

Airflow Variables required:
  config.dev  — JSON config for dev environment
  config.prod — JSON config for prod environment (default)
"""

import json
import logging
from pathlib import Path

import pendulum
from airflow.models.param import Param
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk import dag, task
from croniter import croniter

from meals._common.config import get_config, get_mongo_conn_id

logger = logging.getLogger(__name__)

SCHEDULE_CONFIG_PATH = Path.home() / "airflow" / "schedule_config.json"
SHEET_UPDATE_OFFSET_MINUTES = 5


def _add_minutes_to_cron(cron_str: str, delta: int) -> str:
    """Add delta minutes to a cron expression's time fields with rollover."""
    parts = cron_str.split()
    if len(parts) < 5:
        raise ValueError(f"Invalid cron expression: {cron_str!r}")
    total = int(parts[0]) + delta
    extra_h, new_min = divmod(total, 60)
    parts[0] = str(new_min)
    parts[1] = str((int(parts[1]) + extra_h) % 24)
    result = " ".join(parts)
    # Validate with croniter
    croniter(result, pendulum.now().naive())
    return result


@dag(
    dag_id="settings_sync",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2},
    tags=["meals", "settings"],
    description="Sync schedule settings from MongoDB to local schedule_config.json",
    params={
        "env": Param("prod", enum=["dev", "prod"], description="Environment to read from"),
    },
)
def settings_sync():

    @task()
    def sync_settings(**context) -> None:
        env = context["params"]["env"]
        config = get_config(env)

        hook = MongoHook(mongo_conn_id=get_mongo_conn_id(env))
        client = hook.get_conn()
        db = client[config["MONGO_DB"]]

        schedule_doc = db.settings.find_one({"_id": "schedule"})
        client.close()

        if not schedule_doc:
            logger.warning("No 'schedule' settings document found in MongoDB — skipping")
            return

        crons = schedule_doc.get("crons", [])
        if not crons:
            logger.warning("'schedule' document has no crons — skipping")
            return

        # Validate all cron strings
        for c in crons:
            try:
                croniter(c, pendulum.now().naive())
            except Exception as e:
                raise ValueError(f"Invalid cron expression {c!r}: {e}") from e

        # Derive sheet_update crons by adding SHEET_UPDATE_OFFSET_MINUTES to each
        sheet_crons = [_add_minutes_to_cron(c, SHEET_UPDATE_OFFSET_MINUTES) for c in crons]

        schedule_config = {
            "daily_update": {"crons": crons},
            "sheet_update": {"crons": sheet_crons},
        }

        SCHEDULE_CONFIG_PATH.write_text(json.dumps(schedule_config, indent=2))
        logger.info("Written schedule_config.json: %s", json.dumps(schedule_config))

    sync_settings()


settings_sync()
