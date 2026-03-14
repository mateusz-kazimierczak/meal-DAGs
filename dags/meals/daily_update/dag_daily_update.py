"""
Daily Meals Update DAG
======================
Schedule is loaded from ~/airflow/schedule_config.json (written by settings_sync DAG).
Falls back to hardcoded defaults if the file is missing.

Migrated from ec-meals-backend/src/app/api/internal/dailyUpdate/route.js.
Email/notification sending is intentionally excluded — handled by meal_notifications DAG.

Steps:
  1. fetch_data     — Pull active users + relevant Day documents from MongoDB
  2. process_users  — Advance meal matrices, build meal lists, collect BigQuery audit rows
  3. (parallel)
       insert_bigquery — Write meal-change audit rows to BigQuery
       update_days     — Upsert today/tomorrow/next-week Day documents in MongoDB
  4. save_users     — Persist updated user.meals arrays back to MongoDB

Airflow Variables required:
  config.dev  — JSON config for dev environment
  config.prod — JSON config for prod environment (default)

Airflow Connections required:
  mongoid_dev  — dev MongoDB connection
  mongoid_prod — prod MongoDB connection
"""

import json
import logging
from pathlib import Path

import pendulum
from airflow.models.param import Param
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk import dag, task
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from meals._common.config import get_config, get_mongo_conn_id

logger = logging.getLogger(__name__)

TORONTO_TZ = pendulum.timezone("America/Toronto")

# ---------------------------------------------------------------------------
# Schedule loading — reads from schedule_config.json written by settings_sync
# ---------------------------------------------------------------------------

_SCHEDULE_CONFIG_PATH = Path.home() / "airflow" / "schedule_config.json"
_SCHEDULE_DEFAULTS = {
    "daily_update": {"crons": ["30 8 * * 1-5", "15 9 * * 0,6"]},
    "sheet_update":  {"crons": ["35 8 * * 1-5", "20 9 * * 0,6"]},
}


def _load_schedule(key: str) -> dict:
    try:
        return json.loads(_SCHEDULE_CONFIG_PATH.read_text())[key]
    except Exception:
        return _SCHEDULE_DEFAULTS[key]


_daily_sched = _load_schedule("daily_update")


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def day_string(dt) -> str:
    """Date as D/M/YYYY with no leading zeros, matching the app convention."""
    return f"{dt.day}/{dt.month}/{dt.year}"


def get_app_day_index(dt) -> int:
    """Monday=0 ... Sunday=6, matching the JS app's weekday convention."""
    return dt.weekday()


def construct_meal_user_object(user: dict) -> dict:
    return {
        "name": f"{user['firstName']} {user['lastName']}",
        "_id": str(user["_id"]),
        "diet": user.get("diet"),
    }


def is_user_in_list(user_id: str, lst: list) -> bool:
    return any(str(m["_id"]) == str(user_id) for m in lst)


def meal_list_merge(existing: list, new_meals: list) -> None:
    """Merge new_meals into existing in-place, skipping users already present."""
    for i, new_category in enumerate(new_meals):
        while len(existing) <= i:
            existing.append([])
        existing_ids = {str(m["_id"]) for m in existing[i]}
        for new_meal in new_category:
            if str(new_meal["_id"]) not in existing_ids:
                existing[i].append(new_meal)
                existing_ids.add(str(new_meal["_id"]))


def serialize_bson(obj):
    """Recursively convert ObjectIds, datetimes, and other BSON types to JSON-safe Python."""
    from bson import ObjectId
    from datetime import datetime
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, list):
        return [serialize_bson(item) for item in obj]
    if isinstance(obj, dict):
        return {k: serialize_bson(v) for k, v in obj.items()}
    return obj


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="daily_meals_update",
    schedule=MultipleCronTriggerTimetable(
        *_daily_sched["crons"],
        timezone=TORONTO_TZ,
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 1},
    tags=["meals", "daily"],
    description="Daily meal matrix update: advance schedules, upsert Day records, audit to BigQuery",
    params={
        "env": Param("prod", enum=["dev", "prod"], description="Environment to run against"),
    },
)
def daily_meals_update():

    # ------------------------------------------------------------------
    # STEP 1 — Fetch data
    # ------------------------------------------------------------------
    @task()
    def fetch_data(**context) -> dict:
        env    = context["params"]["env"]
        config = get_config(env)

        now = pendulum.now(TORONTO_TZ)

        today_str              = day_string(now)
        tomorrow_str           = day_string(now.add(days=1))
        next_week_today_str    = day_string(now.add(days=7))
        next_week_tomorrow_str = day_string(now.add(days=8))

        logger.info(
            "[%s] Date window — today: %s | tomorrow: %s | +7: %s | +8: %s",
            env, today_str, tomorrow_str, next_week_today_str, next_week_tomorrow_str,
        )

        hook   = MongoHook(mongo_conn_id=get_mongo_conn_id(env))
        client = hook.get_conn()
        db     = client[config["MONGO_DB"]]

        users = list(db.users.find(
            {"active": True},
            {"name": 1, "meals": 1, "preferences": 1, "email": 1,
             "firstName": 1, "lastName": 1, "diet": 1},
        ))
        logger.info("Fetched %d active user(s)", len(users))

        days = list(db.days.find({
            "date": {"$in": [today_str, tomorrow_str, next_week_today_str, next_week_tomorrow_str]},
        }))
        logger.info("Fetched %d Day record(s): %s", len(days), [d["date"] for d in days])

        client.close()

        return json.loads(json.dumps({
            "env":                     env,
            "users":                   [serialize_bson(u) for u in users],
            "days":                    [serialize_bson(d) for d in days],
            "today_str":               today_str,
            "tomorrow_str":            tomorrow_str,
            "next_week_today_str":     next_week_today_str,
            "next_week_tomorrow_str":  next_week_tomorrow_str,
            "now":                     now.isoformat(),
        }))

    # ------------------------------------------------------------------
    # STEP 2 — Process users
    # ------------------------------------------------------------------
    @task()
    def process_users(data: dict) -> dict:
        now        = pendulum.parse(data["now"])
        tomorrow   = now.add(days=1)
        day_index      = get_app_day_index(now)
        next_day_index = get_app_day_index(tomorrow)

        today_str              = data["today_str"]
        tomorrow_str           = data["tomorrow_str"]
        next_week_today_str    = data["next_week_today_str"]
        next_week_tomorrow_str = data["next_week_tomorrow_str"]

        days  = data["days"]
        users = data["users"]

        today              = next((d for d in days if d["date"] == today_str), None)
        day_tomorrow       = next((d for d in days if d["date"] == tomorrow_str), None)
        next_week_today    = next((d for d in days if d["date"] == next_week_today_str), None)
        next_week_tomorrow = next((d for d in days if d["date"] == next_week_tomorrow_str), None)

        if today is None:
            today = {"date": today_str, "meals": [[], [], []], "packedMeals": [[], [], []],
                     "guests": [], "unmarked": [], "_id": None}
        if day_tomorrow is None:
            day_tomorrow = {"date": tomorrow_str, "meals": [[], [], []], "packedMeals": [[], [], []],
                            "guests": [], "unmarked": [], "_id": None}

        today_packed_user_ids = {
            str(m["_id"])
            for meal_list in today.get("packedMeals", [])
            for m in meal_list
        }

        meals        = [[], [], []]
        packed_meals = [[], [], []]
        no_meals     = []
        unmarked     = []
        meal_changes = []

        for user in users:
            user_id    = str(user["_id"])
            user_meals = user["meals"]
            old_meals  = [day[:] for day in user_meals]

            if user_meals[day_index][6]:
                no_meals.append(construct_meal_user_object(user))

            for i, selected in enumerate(user_meals[day_index][:3]):
                if selected:
                    meals[i].append(construct_meal_user_object(user))

            for i, selected in enumerate(user_meals[next_day_index][3:6]):
                if selected:
                    packed_meals[i].append(construct_meal_user_object(user))

            if next_week_today:
                for i, meal_list in enumerate(next_week_today.get("meals", [])):
                    for entry in list(meal_list):
                        if str(entry["_id"]) == user_id:
                            meal_list.remove(entry)
                            user_meals[day_index][i] = True
                            logger.info("Reconciled %s from nextWeekToday meals[%d]",
                                        user["firstName"], i)
                            break

            meals_today    = user_meals[day_index]
            meals_tomorrow = user_meals[next_day_index]

            marked_today = (
                is_user_in_list(user_id, no_meals)
                or any(meals_today[:3])
                or user_id in today_packed_user_ids
            )

            if not marked_today:
                unmarked.append(construct_meal_user_object(user))

            if not user.get("preferences", {}).get("persistMeals", False):
                user_meals[day_index] = (
                    [False, False, False]
                    + user_meals[day_index][3:6]
                    + [False]
                )
                user_meals[next_day_index] = (
                    user_meals[next_day_index][:3]
                    + [False, False, False]
                    + [user_meals[next_day_index][6]]
                )

            new_meals = [day[:] for day in user_meals]
            if json.dumps(old_meals) != json.dumps(new_meals):
                meal_changes.append({
                    "USER_ID":          user_id,
                    "CHANGE_TIME":      now.isoformat(),
                    "IS_SYSTEM_CHANGE": True,
                    "OLD_MEALS":        json.dumps(old_meals),
                    "NEW_MEALS":        json.dumps(new_meals),
                })

        meal_list_merge(today["meals"], meals)
        meal_list_merge(day_tomorrow.setdefault("packedMeals", [[], [], []]), packed_meals)
        today["unmarked"] = unmarked
        today["noMeals"]  = no_meals

        logger.info(
            "meals: %s | packed: %s | noMeals: %d | unmarked: %d | BQ changes: %d",
            [len(m) for m in meals], [len(m) for m in packed_meals],
            len(no_meals), len(unmarked), len(meal_changes),
        )

        result = {
            "env":                 data["env"],
            "user_updates":        [{"_id": u["_id"], "meals": u["meals"]} for u in users],
            "today":               today,
            "tomorrow":            day_tomorrow,
            # Use {} sentinel instead of None — Airflow 3.0.6 bug (#55062): xcom_push
            # with a None value sends an empty HTTP body, causing a 422 from the API server.
            # update_days checks `if not day_doc` to handle both None and {}.
            "next_week_today":     next_week_today if next_week_today is not None else {},
            "next_week_tomorrow":  next_week_tomorrow if next_week_tomorrow is not None else {},
            "meal_changes":        meal_changes,
        }
        # Force JSON round-trip to catch any non-serializable types before Airflow
        # tries to push to XCom API.
        return json.loads(json.dumps(result))

    # ------------------------------------------------------------------
    # STEP 3a — BigQuery audit insert
    # ------------------------------------------------------------------
    @task()
    def insert_bigquery(results: dict) -> None:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        env    = results["env"]
        config = get_config(env)

        meal_changes = results["meal_changes"]
        if not meal_changes:
            logger.info("No meal changes — skipping BigQuery insert")
            return

        credentials = service_account.Credentials.from_service_account_info(
            config["GCP_AUTH"]
        )
        bq_client = bigquery.Client(project=config["GCP_PROJECT"], credentials=credentials)

        errors = bq_client.insert_rows_json(
            bq_client.dataset(config["BQ_DATASET"]).table(config["BQ_TABLE"]),
            meal_changes,
        )
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {json.dumps(errors)}")

        logger.info("Inserted %d change record(s) to BigQuery", len(meal_changes))

    # ------------------------------------------------------------------
    # STEP 3b — Upsert Day documents
    # ------------------------------------------------------------------
    @task()
    def update_days(results: dict) -> None:
        from bson import ObjectId

        env    = results["env"]
        config = get_config(env)

        hook   = MongoHook(mongo_conn_id=get_mongo_conn_id(env))
        client = hook.get_conn()
        db     = client[config["MONGO_DB"]]

        def upsert_day(day_doc):
            if not day_doc:  # handles None and {} sentinel (Airflow 3.0.6 XCom bug workaround)
                return
            doc_id = day_doc.get("_id")
            body   = {k: v for k, v in day_doc.items() if k != "_id"}
            if doc_id:
                db.days.replace_one({"_id": ObjectId(doc_id)}, body)
                logger.info("Updated Day record for %s", day_doc["date"])
            else:
                db.days.insert_one(body)
                logger.info("Created Day record for %s", day_doc["date"])

        for day_doc in [
            results["today"],
            results["tomorrow"],
            results["next_week_today"],
            results["next_week_tomorrow"],
        ]:
            upsert_day(day_doc)

        client.close()

    # ------------------------------------------------------------------
    # STEP 4 — Persist updated user meal arrays
    # ------------------------------------------------------------------
    @task()
    def save_users(results: dict) -> None:
        from bson import ObjectId

        env    = results["env"]
        config = get_config(env)

        hook   = MongoHook(mongo_conn_id=get_mongo_conn_id(env))
        client = hook.get_conn()
        db     = client[config["MONGO_DB"]]

        for user in results["user_updates"]:
            db.users.update_one(
                {"_id": ObjectId(user["_id"])},
                {"$set": {"meals": user["meals"]}},
            )

        client.close()
        logger.info("Saved meal arrays for %d user(s)", len(results["user_updates"]))

    # ------------------------------------------------------------------
    # Wire up the pipeline
    # ------------------------------------------------------------------
    raw       = fetch_data()
    processed = process_users(raw)

    bq_task   = insert_bigquery(processed)
    days_task = update_days(processed)

    [bq_task, days_task] >> save_users(processed)


daily_meals_update()
