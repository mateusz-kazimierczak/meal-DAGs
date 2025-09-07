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

    print(f"Current day: {current_day}, Notification time: {notification_time}. Hour: {current_hour}. New code!")

    # find users that should be notified

    relevant_users = []

    user_query = {
        "$and": [
            {"active": True},
            # make sure the user is active
            {"$or": [
                {"notifications.notificationTypes.email": True},
                {"notifications.notificationTypes.mobile": True}
            ]},
            # check that he has selected the current notification window
            {f"notifications.schedule.{notification_time}.{current_day}": True},
        ]
    }

    for user in users.find(user_query):
        extract_user_info(user)
    