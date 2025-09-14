
import json


import pendulum
from airflow.sdk import dag, task

from meals.notifications.get_relevant_users.get_relevant_users import get_relevant_users_task
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test"],
)
def meal_notifications():

    @task()
    def get_relevant_users():
        users = get_relevant_users_task()

        # save dict with notifications to json
        with open('notifications.json', 'w') as f:
            json.dump(users, f)

    get_relevant_users()


dag = meal_notifications()

if __name__ == "__main__":
    dag.test()


