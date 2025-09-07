from airflow.providers.mongo.hooks.mongo import MongoHook
import pendulum

def ensure_user_in_dict(dict, user_id):
    if user_id not in dict:
        dict[user_id] = []

def is_user_in_week_meals(user, relevant_day_date):
    is_user_in_meals = False

    print(user)

    for meal in user.meals[relevant_day_date.day_of_week][:3]:
        if meal:
            is_user_in_meals = True
            break

    return is_user_in_meals

def handle_user_day_notification(user, relevant_day_date, relevant_day_label, relevant_meal_type, is_user_in_meals_relevant, is_user_in_packed_relevant, notification_objects):
    if user['notifications']['schema']['any_meals'][relevant_day_date.day_of_week]:
        if not is_user_in_meals_relevant and not is_user_in_packed_relevant:
            ensure_user_in_dict(notification_objects, user._id)
            notification_objects[user._id].append({
                "type": "any",
                "on": relevant_day_label
            })
    else:
        if user['notifications']['schema'][relevant_meal_type][relevant_day_date.day_of_week]:
            if not (is_user_in_meals_relevant if relevant_meal_type == 'meals' else is_user_in_packed_relevant):
                ensure_user_in_dict(notification_objects, user._id)
                notification_objects[user._id].append({
                    "type": relevant_meal_type,
                    "on": relevant_day_label
                })

def is_user_in_day_meals(user, day_id_list):
    return user._id in day_id_list

def get_relevant_users_task():
    """
    Get the users that should be notified about the meals.
    """
    
    # configure the MongoDB connection
    hook = MongoHook(mongo_conn_id='mongoid')
    client = hook.get_conn()
    db = client.test
    users=db.users
    meals = db.days
    
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

    relevant_day_label = "today" if day_offset == 0 else "tomorrow"
    relevant_day_label_next = "tomorrow" if day_offset == 0 else relevant_next_day_date.format('dddd')

    print("getting relevant days :", relevant_day_date.format('D/M/YYYY'), relevant_next_day_date.format('D/M/YYYY'))

    # have to fetch meals for the relevant days
    relevant_meals_cursor = meals.find({
        "$or": [
            {"date": relevant_day_date.format('D/M/YYYY')},
            {"date": relevant_next_day_date.format('D/M/YYYY')}
        ]
    })

    relevant_meals = list(relevant_meals_cursor)

    print("Relevant meals:", relevant_meals)

    try:
        relevant_day_meals = [meal for meal in relevant_meals if meal['date'] == relevant_day_date.format('D/M/YYYY')][0]
    except IndexError:
        relevant_day_meals = []
    try:
        relevant_next_day_meals = [meal for meal in relevant_meals if meal['date'] == relevant_next_day_date.format('D/M/YYYY')][0]
    except IndexError:
        relevant_next_day_meals = None

    try:
        relevant_day_packed_meals = [packed._id for packed in relevant_day_meals.packed_meals]
    except AttributeError:
        relevant_day_packed_meals = []

    try:
        relevant_next_day_packed_meals = [packed._id for packed in relevant_next_day_meals.packed_meals]
    except AttributeError:
        relevant_next_day_packed_meals = []

    for user in users.find(user_query):

        # First check what meals the user is signed up for
        is_user_in_meals_relevant = is_user_in_week_meals(user, relevant_day_date)
        is_user_in_meals_relevant_next = is_user_in_week_meals(user, relevant_next_day_date)

        is_user_in_packed_relevant = is_user_in_day_meals(user, relevant_day_packed_meals)
        is_user_in_packed_relevant_next = is_user_in_day_meals(user, relevant_next_day_packed_meals)


        # First handle alerts for the current day
        handle_user_day_notification(user, relevant_day_date, relevant_day_label, "meals", is_user_in_meals_relevant, is_user_in_packed_relevant, notification_objects)

        # handle the next day
        handle_user_day_notification(user, relevant_next_day_date, relevant_day_label_next, "packed_meals", is_user_in_meals_relevant_next, is_user_in_packed_relevant_next, notification_objects)

    print(notification_objects)