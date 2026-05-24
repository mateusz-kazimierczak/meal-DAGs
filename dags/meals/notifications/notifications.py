import json
import os
import logging
import subprocess
from pathlib import Path

import pendulum
from airflow.models.param import Param
from airflow.sdk import dag, task
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from meals._common.config import get_config, get_mongo_conn_id
from meals.notifications.get_relevant_users.get_relevant_users import get_relevant_users_task

logger = logging.getLogger(__name__)

dag_file_directory    = Path(__file__).parent
email_node_project_path  = dag_file_directory / "send_emails"
mobile_node_project_path = dag_file_directory / "mobile_notifications"

# Stable absolute path for the inter-task handoff file
NOTIFICATIONS_FILE = dag_file_directory / "notifications.json"
EMAIL_NOTIFICATION_RESULTS_FILE = dag_file_directory / "email_notification_results.json"
MOBILE_NOTIFICATION_RESULTS_FILE = dag_file_directory / "mobile_notification_results.json"
NOTIFICATION_BQ_TABLE = "NOTIFICATION_HISTORY"

local_tz = pendulum.timezone("America/Toronto")


def get_notification_window() -> str:
    current_hour = pendulum.now(local_tz).hour

    if current_hour < 10:
        return "morning"
    elif current_hour < 17:
        return "noon"

    return "evening"


def load_notification_results(results_file: Path, sender: str, notification_window: str, exit_code: int) -> dict:
    if results_file.exists():
        try:
            results = json.loads(results_file.read_text())
            results.setdefault("sender", sender)
            results.setdefault("notification_window", notification_window)
            results.setdefault("generated_at", pendulum.now(local_tz).to_iso8601_string())
            results.setdefault("rows", [])
            results.setdefault("hadErrors", False)
            results["exit_code"] = exit_code
            return results
        except json.JSONDecodeError as error:
            return {
                "sender": sender,
                "notification_window": notification_window,
                "generated_at": pendulum.now(local_tz).to_iso8601_string(),
                "rows": [],
                "hadErrors": True,
                "exit_code": exit_code,
                "error": {
                    "name": error.__class__.__name__,
                    "message": str(error),
                },
            }

    return {
        "sender": sender,
        "notification_window": notification_window,
        "generated_at": pendulum.now(local_tz).to_iso8601_string(),
        "rows": [],
        "hadErrors": True,
        "exit_code": exit_code,
        "error": {
            "name": "MissingNotificationResults",
            "message": f"Notification results file not found: {results_file}",
        },
    }


@dag(
    schedule=MultipleCronTriggerTimetable(
        "30 7 * * *",   # 7:30 AM
        "0 12 * * *",   # 12:00 PM
        "30 19 * * *",  # 7:30 PM
        timezone=local_tz,
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["meals"],
    params={
        "env": Param("prod", enum=["dev", "prod"], description="Environment to run against"),
    },
)
def meal_notifications():

    @task()
    def get_relevant_users(**context):
        env    = context["params"]["env"]
        config = get_config(env)

        users = get_relevant_users_task(
            mongo_conn_id=get_mongo_conn_id(env),
            db_name=config["MONGO_DB"],
        )

        print("Relevant users:")
        print(json.dumps(users, indent=4))

        with open(NOTIFICATIONS_FILE, 'w') as f:
            json.dump(users, f)

    @task()
    def send_email_notifications(**context):
        env    = context["params"]["env"]
        config = get_config(env)
        notification_window = get_notification_window()

        env_vars = os.environ.copy()
        env_vars.update({
            "RESEND_API_KEY": config["RESEND_API_KEY"],
            "NOTIFICATIONS_PATH": str(NOTIFICATIONS_FILE),
            "NOTIFICATION_RESULTS_PATH": str(EMAIL_NOTIFICATION_RESULTS_FILE),
            "NOTIFICATION_WINDOW": notification_window,
        })

        completed = subprocess.run(
            [config["NODE_BIN"], "dist/index.js"],
            env=env_vars,
            cwd=email_node_project_path,
            check=False,
        )

        return load_notification_results(
            EMAIL_NOTIFICATION_RESULTS_FILE,
            sender="email",
            notification_window=notification_window,
            exit_code=completed.returncode,
        )

    @task()
    def send_mobile_notifications(**context):
        env    = context["params"]["env"]
        config = get_config(env)
        notification_window = get_notification_window()

        env_vars = os.environ.copy()
        env_vars.update({
            "NOTIFICATIONS_PATH": str(NOTIFICATIONS_FILE),
            "NOTIFICATION_RESULTS_PATH": str(MOBILE_NOTIFICATION_RESULTS_FILE),
            "NOTIFICATION_WINDOW": notification_window,
        })

        completed = subprocess.run(
            [config["NODE_BIN"], "index.js"],
            env=env_vars,
            cwd=mobile_node_project_path,
            check=False,
        )

        return load_notification_results(
            MOBILE_NOTIFICATION_RESULTS_FILE,
            sender="push",
            notification_window=notification_window,
            exit_code=completed.returncode,
        )

    def insert_notification_rows(results: dict, env: str) -> None:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        config = get_config(env)
        rows = results.get("rows", [])

        if not rows and not results.get("hadErrors") and results.get("exit_code", 0) == 0:
            logger.info("No notification rows to insert — skipping BigQuery insert")
            return

        credentials = service_account.Credentials.from_service_account_info(
            config["GCP_AUTH"]
        )
        bq_client = bigquery.Client(project=config["GCP_PROJECT"], credentials=credentials)

        errors = bq_client.insert_rows_json(
            bq_client.dataset(config["BQ_DATASET"]).table(NOTIFICATION_BQ_TABLE),
            rows,
        )
        if errors:
            raise RuntimeError(f"Notification BigQuery insert errors: {json.dumps(errors)}")

        logger.info("Inserted %d notification row(s) to BigQuery", len(rows))

        if results.get("hadErrors") or results.get("exit_code", 0) != 0:
            raise RuntimeError(
                f"Notification sender '{results.get('sender', 'unknown')}' reported errors"
            )

    @task()
    def insert_email_notification_logs(results: dict, **context) -> None:
        env = context["params"]["env"]
        insert_notification_rows(results, env)

    @task()
    def insert_mobile_notification_logs(results: dict, **context) -> None:
        env = context["params"]["env"]
        insert_notification_rows(results, env)

    relevant_users = get_relevant_users()
    email_results = send_email_notifications()
    mobile_results = send_mobile_notifications()

    relevant_users >> [email_results, mobile_results]
    insert_email_notification_logs(email_results)
    insert_mobile_notification_logs(mobile_results)


dag = meal_notifications()

if __name__ == "__main__":
    dag.test()
