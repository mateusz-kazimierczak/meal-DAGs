from airflow.sdk import DAG, task
from airflow.models.param import Param
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from googleapiclient.errors import HttpError

import pendulum
import json
from meals._common.config import get_config, get_mongo_conn_id
from meals._common.mongo_utils.get_future_meals import get_future_meals
from meals._common.mongo_utils.get_meals_per_day import get_meals_per_day
from meals.sheet_editor.sheet_utils.ensure_sheet_exists import ensure_sheet_exists
from meals.sheet_editor.sheet_utils.get_last_sheet_row import get_last_row_number_in_column
from meals.sheet_editor.sheet_utils.get_service import get_sheets_service
from meals.sheet_editor.sheet_utils.create_meal_diet_template import create_meal_template


local_tz = pendulum.timezone("America/Toronto")

with DAG(
        dag_id="meal_sheet_update_dag",
        description="Fetch meal data from MongoDB and update Google Sheets",
        schedule=MultipleCronTriggerTimetable(
            "35 8 * * *",
            timezone=local_tz,
        ),
        start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
        catchup=False,
        tags=["meals", "google_sheets", "mongodb"],
        params={
            "env": Param(
                default="prod",
                enum=["dev", "prod"],
                description="Environment to run against",
            ),
            "dry_run": Param(
                default=False,
                type="boolean",
                description="If true, only extract and print data without updating the Google Sheet.",
            ),
        },
) as dag:

    @task()
    def extract_meal_data(dag_run=None):
        env    = dag_run.conf.get("env", "prod")
        config = get_config(env)

        conn_id = get_mongo_conn_id(env)
        db_name = config["MONGO_DB"]

        today = pendulum.instance(dag_run.logical_date).in_timezone("America/Toronto")
        today_meals = get_meals_per_day(today, mongo_conn_id=conn_id, db_name=db_name)

        tomorrow = today.add(days=1)
        tomorrow_meals = get_future_meals(tomorrow, mongo_conn_id=conn_id, db_name=db_name)

        return {"env": env, "date": today, "today": today_meals, "tomorrow": tomorrow_meals}

    @task()
    def update_google_sheet(data_bundle, dag_run=None):
        dry_run = dag_run.conf.get("dry_run", False)

        if dry_run:
            print("=== DRY RUN — Skipping sheet update ===")
            printable_data = {
                "date": data_bundle['date'].to_iso8601_string(),
                "today": data_bundle['today'],
                "tomorrow": data_bundle['tomorrow'],
            }
            print("\nExtracted Meal Data:")
            print(json.dumps(printable_data, indent=2, default=str))
            return

        env    = data_bundle["env"]
        config = get_config(env)

        service = get_sheets_service(config["GCP_AUTH"])
        if not service:
            return

        spreadsheet_id = config["SPREADSHEET_ID"]

        try:
            date       = data_bundle["date"]
            sheet_name = date.format('MMMM (YYYY)')

            ensure_sheet_exists(service, spreadsheet_id, sheet_name)

            start_row = get_last_row_number_in_column(
                service, spreadsheet_id, sheet_name, column='B'
            ) + 3

            create_meal_template(service, spreadsheet_id, sheet_name, start_row, data_bundle)

        except HttpError as err:
            print(f"An error occurred: {err}")

    extracted_data = extract_meal_data()
    update_google_sheet(extracted_data)
