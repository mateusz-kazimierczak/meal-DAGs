import json
import os

from pathlib import Path
import pendulum
from dotenv import load_dotenv
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

from meals.notifications.get_relevant_users.get_relevant_users import get_relevant_users_task
from airflow.timetables.trigger import MultipleCronTriggerTimetable


load_dotenv()

RESEND_API_KEY = os.getenv("RESEND_API_KEY")

dag_file_directory = Path(__file__).parent
email_node_project_path = dag_file_directory / "send_emails"
mobile_node_project_path = dag_file_directory / "mobile_notifications"

local_tz = pendulum.timezone("America/New_York")

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
)
def meal_notifications():

    @task()
    def get_relevant_users():
        users = get_relevant_users_task()

        # print formatted json
        print("Relevant users:")
        print(json.dumps(users, indent=4))

        # save dict with notifications to json
        with open('notifications.json', 'w') as f:
            json.dump(users, f)

    send_mobile_notifications_task = BashOperator(
        task_id="send_mobile_notifications",
        bash_command=f"/home/mateusz/.nvm/versions/node/v22.19.0/bin/node index.js",
        env={
            "NOTIFICATIONS_PATH": os.path.abspath('notifications.json')
        },
        cwd=mobile_node_project_path,
    )
    send_email_notifications_task = BashOperator(
        task_id="send_notifications",
        bash_command=f"/home/mateusz/.nvm/versions/node/v22.19.0/bin/node dist/index.js",
        env={
            "RESEND_API_KEY": RESEND_API_KEY,
            "NOTIFICATIONS_PATH": os.path.abspath('notifications.json')
        },
        cwd=email_node_project_path,
    )

    get_relevant_users() >> [send_email_notifications_task, send_mobile_notifications_task]


dag = meal_notifications()

if __name__ == "__main__":
    dag.test()


