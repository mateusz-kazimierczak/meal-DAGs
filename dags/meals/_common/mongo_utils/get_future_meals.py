from airflow.providers.mongo.hooks.mongo import MongoHook
from collections import defaultdict


def get_future_meals(date, mongo_conn_id="mongoid", db_name="test", collection_name="users"):
    """
    Retrieves the meal counts for breakfast, lunch and supper for a future date within a week
    
    Args:
        date: Pendulum datetime object representing the date to query
        mongo_conn_id: Airflow connection ID for MongoDB
        db_name: Name of the MongoDB database
        collection_name: Name of the collection containing meal documents
        
    Returns:
        dict: Dictionary with meal types as keys, containing meal counts and diet breakdowns
        
    Raises:
        ValueError: If no document is found for the specified date
    """

    # Get index of the day in the week (0=Monday, 6=Sunday)
    day_index = date.day_of_week
    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    meal_collection = client[db_name][collection_name]

    # Get all active users
    active_users = meal_collection.find({"active": True})

    total_counts = {"B": 0, "L": 0, "S": 0}

    print(f"Calculating future meals for date: {date.to_date_string()} (Day index: {day_index})")
    
    for user in active_users:

        print(user)

        # Get the meals for the specified day
        meals_for_day = user['meals'][day_index]
        
        if meals_for_day[0]:
            total_counts["B"] += 1
        if meals_for_day[1]:
            total_counts["L"] += 1
        if meals_for_day[2]:
            total_counts["S"] += 1


    return total_counts
