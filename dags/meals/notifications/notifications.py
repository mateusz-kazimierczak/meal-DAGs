import json
import subprocess
from pathlib import Path

import pendulum
from airflow.models.param import Param
from airflow.sdk import dag, task
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from meals._common.config import get_config, get_mongo_conn_id
from meals.notifications.get_relevant_users.get_relevant_users import get_relevant_users_task

dag_file_directory    = Path(__file__).parent
email_node_project_path  = dag_file_directory / "send_emails"
mobile_node_project_path = dag_file_directory / "mobile_notifications"

# Stable absolute path for the inter-task handoff file
NOTIFICATIONS_FILE = dag_file_directory / "notifications.json"

local_tz = pendulum.timezone("America/Toronto")


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

        subprocess.run(
            [config["NODE_BIN"], "dist/index.js"],
            env={
                "RESEND_API_KEY":      config["RESEND_API_KEY"],
                "NOTIFICATIONS_PATH":  str(NOTIFICATIONS_FILE),
            },
            cwd=email_node_project_path,
            check=True,
        )

    @task()
    def send_mobile_notifications(**context):
        env    = context["params"]["env"]
        config = get_config(env)

        subprocess.run(
            [config["NODE_BIN"], "index.js"],
            env={
                "NOTIFICATIONS_PATH": str(NOTIFICATIONS_FILE),
            },
            cwd=mobile_node_project_path,
            check=True,
        )

    get_relevant_users() >> [send_email_notifications(), send_mobile_notifications()]


dag = meal_notifications()

if __name__ == "__main__":
    dag.test()
