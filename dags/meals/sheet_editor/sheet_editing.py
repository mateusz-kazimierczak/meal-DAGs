from airflow.sdk import DAG, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from google.oauth2.service_account import Credentials # this one
import gspread # this one
import pendulum
import datetime

#TODO: Add all important dependencies to the u.v. lock file using the article Mateusz sent

#TODO: After the DAG shows up on the server, put credentials on rasberry pi. (Later not on my own)



# Timezone
local_tz = pendulum.timezone("America/New_York")

# DAG Definition
with DAG(
        dag_id="meal_sheet_update_dag",
        description="Fetch meal data from MongoDB and update Google Sheets",
        schedule=MultipleCronTriggerTimetable(
        "30 7 * * *",   # 7:30 AM
        "0 12 * * *",   # 12:00 PM
        "30 19 * * *",  # 7:30 PM
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
    def extract_meal_data(ds=None):
        print("✅ Packages ready to go!")

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

        date_obj = datetime.datetime.strptime(ds, "%Y-%m-%d")
        date = date_obj.strftime("%d/%m/%Y")


        doc = meal_collection.find_one({"date": date})
        if not doc:
            raise ValueError(f"No document found for date {date}")

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
        print("\n==========================================")
        print("==============PART1-COMPLETE==============")
        print("==========================================\n")

        return {"date": date, "data": dataDictionary}

    # =====================
    # Task 2: Update Google Sheet
    # =====================
    @task()
    def update_google_sheet(data_bundle):
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        SERVICE_ACCOUNT_FILE = "TODO: Place proper json file for login credentials"

        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)

        date = data_bundle["date"]
        dataDictionary = data_bundle["data"]

        emDashDate = date.replace("/", "-")
        clientIndex = int(emDashDate.split("-")[1]) - 8
        monthSheet = client.open("Testing of EC Meal Sheet 2025").get_worksheet(clientIndex)

        cell = monthSheet.findall(str(emDashDate))[0]
        originRow, originCol = cell.row, cell.col

        dietIndices = {"lactose free": 1, "seafood": 2, "no fish sesame": 3, "no peanuts": 4, "no white fish": 5}

        def update_meal_block(meal_key, row_offset):

            if monthSheet.cell(originRow + row_offset, originCol).value is not None:
                return

            monthSheet.update_cell(originRow + row_offset, originCol, dataDictionary[meal_key]["number"])
            for diet in dataDictionary[meal_key]["hasDiet"]:
                monthSheet.update_cell(originRow + row_offset, originCol + dietIndices[diet], True)

        update_meal_block("L", 1)
        update_meal_block("S", 2)
        update_meal_block("P1", 5)
        update_meal_block("P2", 6)
        update_meal_block("PS", 7)

        print("✅ Sheet updated successfully!")
        print("\n==========================================")
        print("==============PART2-COMPLETE==============")
        print("==========================================\n")

    # Task flow
    extracted_data = extract_meal_data()
    update_google_sheet(extracted_data)