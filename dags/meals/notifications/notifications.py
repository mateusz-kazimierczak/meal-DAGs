
import json


import pendulum
from airflow.sdk import dag, task

from meals.notifications.get_relevant_users.get_relevant_users import get_relevant_users_task
from meals.notifications.get_relevant_users.generate_notification_objects import generate_notification_objects_task

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

        print("Relevant users:")
        for user in users:
            print(f"{user.firstName} ({user.email})")

        return users

    def generate_notifications(users):
        return generate_notification_objects_task(users)

    get_relevant_users() >> generate_notifications()


dag = meal_notifications()

if __name__ == "__main__":
    dag.test()


