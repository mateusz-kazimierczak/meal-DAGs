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