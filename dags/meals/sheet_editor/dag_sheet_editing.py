from airflow.sdk import DAG, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from googleapiclient.errors import HttpError

import pendulum
import datetime
from meals.sheet_editor.sheet_utils import ensure_sheet_exists
from meals.sheet_editor.sheet_utils.get_last_sheet_row import get_last_row_number_in_column
from meals.sheet_editor.sheet_utils.get_service import get_sheets_service
from meals.sheet_editor.sheet_utils.create_meal_diet_template import add_meal_block, create_meal_template



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

        hook = MongoHook(mongo_conn_id="mongoid")
        client = hook.get_conn()

        try:
            client.admin.command("ping")
            print("✅ Pinged your deployment. Successfully connected to MongoDB!\n")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise

        db = client["test"]
        meal_collection = db["days"]

        date = pendulum.instance(dag_run.logical_date).in_timezone("America/Toronto")

        doc = meal_collection.find_one({"date": date.format("D/M/YYYY")})
        if not doc:
            raise ValueError(f"No document found for date {date.format('D/M/YYYY')}")

        dataDictionary = {
            "B": {"number": len(doc["meals"][0]), "hasDiet": []},
            "L": {"number": len(doc["meals"][1]), "hasDiet": []},
            "S": {"number": len(doc["meals"][2]), "hasDiet": []},
            "P1": {"number": len(doc["packedMeals"][0]), "hasDiet": []},
            "P2": {"number": len(doc["packedMeals"][1]), "hasDiet": []},
            "PS": {"number": len(doc["packedMeals"][2]), "hasDiet": []},
        }

        for key, meal_list in zip(
                ["B", "L", "S", "P1", "P2", "PS"],
                doc["meals"] + doc["packedMeals"],
        ):
            for user in meal_list:
                if user["diet"] is not None:
                    dataDictionary[key]["hasDiet"].append(user["diet"])

        print(dataDictionary)

        return {"date": date, "data": dataDictionary}

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