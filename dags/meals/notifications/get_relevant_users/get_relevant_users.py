from airflow.providers.mongo.hooks.mongo import MongoHook
import pendulum

from meals.notifications.get_relevant_users.extract_user_info import extract_user_info



def get_relevant_users_task():
    """
    Get the users that should be notified about the meals.
    """
    
    # configure the MongoDB connection
    hook = MongoHook(mongo_conn_id='mongoid')
    client = hook.get_conn()
    db = client.test
    users=db.users
    
    # get current day of the week
    current_day = (pendulum.now().day_of_week - 1) % 7  # pendulum returns 0=Tuesday, 6=Monday, so we need to


    # find if should run morning noon or evening
    notification_time = "morning"
    current_hour = pendulum.now().hour

    if current_hour < 10:
        notification_time = "morning"
    elif current_hour < 17:
        notification_time = "noon"
    else:
        notification_time = "evening"
    
    print(f"Current day: {current_day}, Notification time: {notification_time}")

    # find users that should be notified

    relevant_users = []

    user_query = {
        "$and": [
            # make sure the user has the email field enabled
            {"notifications.notificationTypes.email": True},
            {"active": True}
        ]
    }

    for user in users.find(user_query):
        extract_user_info(user)
    