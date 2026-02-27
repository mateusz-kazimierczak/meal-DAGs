from airflow.providers.mongo.hooks.mongo import MongoHook
from collections import defaultdict


def get_all_diets(mongo_conn_id="mongoid", db_name="test", collection_name="diets"):
    """Fetch all diet names from the specified MongoDB collection.

    Args:
        mongo_conn_id (str): Airflow connection ID for MongoDB.
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the collection containing diet documents.

    Returns:
        list: A list of all diet names.
    """

    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    meal_collection = client[db_name][collection_name]

    # Get all active users
    all_diets = meal_collection.find()

    return [diet['name'] for diet in all_diets]


def get_diet_user_counts(mongo_conn_id="mongoid", db_name="test"):
    """Count the number of active users assigned to each diet.

    Queries the 'users' collection for active users with a non-null diet field,
    and counts how many users are assigned to each diet.

    Args:
        mongo_conn_id (str): Airflow connection ID for MongoDB.
        db_name (str): Name of the MongoDB database.

    Returns:
        dict: A dictionary mapping diet names to their active user counts.
              e.g. {"Vegan": 3, "Vegetarian": 1}
    """

    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    users_collection = client[db_name]["users"]

    # Find all active users that have a diet assigned
    active_users_with_diet = users_collection.find({
        "active": True,
        "diet": {"$ne": None}
    })

    diet_counts = defaultdict(int)
    for user in active_users_with_diet:
        diet_name = user.get("diet")
        if diet_name:
            diet_counts[diet_name] += 1

    return dict(diet_counts)