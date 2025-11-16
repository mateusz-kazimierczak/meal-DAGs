from airflow.providers.mongo.hooks.mongo import MongoHook
from collections import defaultdict


def get_meals_per_day(date, mongo_conn_id="mongoid", db_name="test", collection_name="days"):
    """
    Retrieves the meal document for a specific date from the MongoDB collection
    and counts users per diet for each meal type.
    
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
    # Connect to MongoDB
    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    meal_collection = client[db_name][collection_name]

    # Query for the meal document
    date_str = date.format('D/M/YYYY')
    doc = meal_collection.find_one({"date": date_str})
    
    if not doc:
        raise ValueError(f"No document found for date {date_str}")

    # Define meal type keys
    MEAL_TYPES = ["B", "L", "S", "P1", "P2", "PS"]
    
    # Initialize data dictionary
    data_dict = {
        meal_type: {"number": 0, "hasDiet": {}}
        for meal_type in MEAL_TYPES
    }
    
    # Combine all meal lists
    all_meals = doc.get("meals", []) + doc.get("packedMeals", [])
    
    # Process each meal type
    for meal_type, meal_list in zip(MEAL_TYPES, all_meals):
        data_dict[meal_type]["number"] = len(meal_list)
        
        # Count users per diet
        diet_counts = defaultdict(int)
        for user in meal_list:
            diet = user.get("diet")
            if diet is not None:
                diet_counts[diet] += 1
        
        data_dict[meal_type]["hasDiet"] = dict(diet_counts)

    return data_dict
