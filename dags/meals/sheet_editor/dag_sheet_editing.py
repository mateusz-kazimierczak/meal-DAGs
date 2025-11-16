from airflow.sdk import DAG, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from googleapiclient.errors import HttpError

import pendulum
import datetime
from meals._common.mongo_utils.get_future_meals import get_future_meals
from meals._common.mongo_utils.get_meals_per_day import get_meals_per_day
from meals.sheet_editor.sheet_utils.ensure_sheet_exists import ensure_sheet_exists
from meals.sheet_editor.sheet_utils.get_last_sheet_row import get_last_row_number_in_column
from meals.sheet_editor.sheet_utils.get_service import get_sheets_service
from meals.sheet_editor.sheet_utils.create_meal_diet_template import create_meal_template



# Timezone
local_tz = pendulum.timezone("America/New_York")

# DAG Definition
with DAG(
        dag_id="meal_sheet_update_dag",
        description="Fetch meal data from MongoDB and update Google Sheets",
        schedule=MultipleCronTriggerTimetable(
        "45 8 * * *",   # 8:45 AM
        timezone=local_tz,
    ),
        start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
        catchup=False,
        tags=["meals", "google_sheets", "mongodb"],
) as dag:

    # =====================
    # Task 1: Extract meal data
    # =====================
    @task()
    def extract_meal_data(dag_run=None):

        today = pendulum.instance(dag_run.logical_date).in_timezone("America/Toronto")
        today_meals = get_meals_per_day(today)

        tomorrow = today.add(days=1)
        tomorrow_meals = get_future_meals(tomorrow)

        return {"date": today, "today": today_meals, "tomorrow": tomorrow_meals}

    # =====================
    # Task 2: Update Google Sheet
    # =====================
    @task()
    def update_google_sheet(data_bundle):

        SPREADSHEET_ID = '1Cwby6QGITHfPMirBkiHpLxk8nv16ql5TuerRXrzqv_I'

        """Appends data to the specified Google Sheet."""
        service = get_sheets_service()
        
        if not service:
            return

        try:

            # check if the sheet corresponding to the month exists, if not create it
            date = data_bundle["date"]
            sheet_name = date.format('MMMM (YYYY)')

            ensure_sheet_exists(service, SPREADSHEET_ID, sheet_name)
            
            # 2. Find cell to start appending data, 3 cells below the last filled row in the sheet
            start_row = get_last_row_number_in_column(service, SPREADSHEET_ID, sheet_name, column='B') + 3
            
            # 3. Add the meal block to the sheet
            create_meal_template(service, SPREADSHEET_ID, sheet_name, start_row, data_bundle)

        except HttpError as err:
            print(f"An error occurred: {err}")


    # Task flow
    extracted_data = extract_meal_data()
    update_google_sheet(extracted_data)