from airflow.providers.mongo.hooks.mongo import MongoHook
import pendulum



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
    current_day = (pendulum.now("America/Toronto").day_of_week)


    # find if should run morning noon or evening
    notification_time = "morning"
    current_hour = pendulum.now("America/Toronto").hour

    if current_hour < 10:
        notification_time = "morning"
    elif current_hour < 17:
        notification_time = "noon"
    else:
        notification_time = "evening"

    print(f"Current day: {current_day}, Notification time: {notification_time}. Hour: {current_hour}. New code!")

    # find users that should be notified

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

    now = pendulum.now("America/Toronto")

    notification_objects = []

    day_offset = 0 if notification_time == 'morning' else 1

    relevant_day_date = now.add(days=day_offset)
    relevant_next_day_date = now.add(days=day_offset + 1)

    # have to fetch meals for the relevant days
    relevant_meals_cursor = db.meals.find({
        "$or": [
            {"date": relevant_day_date.format('D/M/YYYY')},
            {"date": relevant_next_day_date.format('D/M/YYYY')}
        ]
    })

    relevant_meals = list(relevant_meals_cursor)

    relevant_day_meals = relevant_meals.filter(lambda meal: meal['date'] == relevant_day_date.format('D/M/YYYY'))
    relevant_next_day_meals = relevant_meals.filter(lambda meal: meal['date'] == relevant_next_day_date.format('D/M/YYYY'))

    print(relevant_next_day_meals)

    for user in users.find(user_query):
        # first handle notifications on the relvant day
        if user['notifications']['schema']['any_meals'][relevant_day_date.day_of_week]:
            signed_up = False

            # first check if the user is signed up for normal meals
            for meal in user.meals[relevant_day_date.day_of_week][:3]:
                if meal:
                    signed_up = True
                    break
            
            # # if it is not signed up for normal meals, check packed meals
            # if not signed_up: