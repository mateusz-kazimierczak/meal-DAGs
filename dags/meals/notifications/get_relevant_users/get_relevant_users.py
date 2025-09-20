from airflow.providers.mongo.hooks.mongo import MongoHook
import pendulum

def ensure_user_in_dict(dict, user):
    user_id = str(user["_id"])

    if user_id not in dict:
        user_email = user.get("email")
        user_name = user.get("firstName")
        user_send_email = user.get("notifications", {}).get("notificationTypes", {}).get("email", False)

        dict[user_id] = {
            'send_email': user_send_email,
            'email': user_email,
            'name': user_name,
            'notifications': [],
            'report': None
        }

def is_user_in_week_meals(user, relevant_day_date):
    is_user_in_meals = False


    for meal in user['meals'][relevant_day_date.day_of_week][:3]:
        if meal:
            is_user_in_meals = True
            break

    return is_user_in_meals

def is_user_in_week_packed_meals(user, relevant_day_date):
    is_user_in_meals = False


    for meal in user['meals'][relevant_day_date.day_of_week][3:6]:
        if meal:
            is_user_in_meals = True
            break

    return is_user_in_meals

def get_user_packed_meals(day, user):
    pass

def handle_user_day_notification(user, relevant_day_date, relevant_day_label, relevant_meal_type, is_user_in_meals_relevant, is_user_in_packed_relevant, notification_objects):
    if user['notifications']['schema']['any_meals'][relevant_day_date.day_of_week] and not user['meals'][relevant_day_date.day_of_week][6]:
        if not is_user_in_meals_relevant and not is_user_in_packed_relevant:
            ensure_user_in_dict(notification_objects, user)
            notification_objects[str(user["_id"])]['notifications'].append({
                "type": "any",
                "on": relevant_day_label
            })
    else:
        if user['notifications']['schema'][relevant_meal_type][relevant_day_date.day_of_week] and not user['meals'][relevant_day_date.day_of_week][6]:
            if not (is_user_in_meals_relevant if relevant_meal_type == 'meals' else is_user_in_packed_relevant):
                ensure_user_in_dict(notification_objects, user)
                notification_objects[str(user["_id"])]['notifications'].append({
                    "type": relevant_meal_type,
                    "on": relevant_day_label
                })

def is_user_in_day_meals(user, day_id_list):
    return str(user["_id"]) in day_id_list

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

    notification_objects = {}

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

    try:
        relevant_day_meals = [meal for meal in relevant_meals if meal['date'] == relevant_day_date.format('D/M/YYYY')][0]
    except IndexError:
        relevant_day_meals = []
    try:
        relevant_next_day_meals = [meal for meal in relevant_meals if meal['date'] == relevant_next_day_date.format('D/M/YYYY')][0]
    except IndexError:
        relevant_next_day_meals = None

    try:
        relevant_day_packed_meal_types = [packed for packed in relevant_day_meals['packedMeals']]
        print(relevant_day_packed_meal_types)
        relevant_day_packed_meals = [packed['_id'] for packed in relevant_day_packed_meal_types]
        print("Getting relevant_day_packed_meals:", relevant_day_packed_meals)
    except AttributeError as e:
        print("Could not get relevant_day_packed_meals:", e)
        relevant_day_packed_meals = []

    try:
        relevant_next_day_packed_meal_types = [packed['_id'] for packed in relevant_next_day_meals['packedMeals']]
        relevant_day_packed_meals = [packed._id for packed in relevant_next_day_packed_meal_types]
        print("Getting relevant_next_day_packed_meals:", relevant_next_day_packed_meals)
    except AttributeError as e:
        print("Could not get relevant_next_day_packed_meals:", e)
        relevant_next_day_packed_meals = []

    for user in users.find(user_query):

        # First check what meals the user is signed up for
        is_user_in_meals_relevant = is_user_in_week_meals(user, relevant_day_date)
        is_user_in_meals_relevant_next = is_user_in_week_meals(user, relevant_next_day_date)

        is_user_in_packed_relevant = is_user_in_day_meals(user, relevant_day_packed_meals)
        is_user_in_packed_relevant_next = is_user_in_week_packed_meals(user, relevant_next_day_date)


        # First handle alerts for the current day
        handle_user_day_notification(user, relevant_day_date, relevant_day_label, "meals", is_user_in_meals_relevant, is_user_in_packed_relevant, notification_objects)

        # handle the next day
        handle_user_day_notification(user, relevant_next_day_date, relevant_day_label_next, "packed_meals", is_user_in_meals_relevant_next, is_user_in_packed_relevant_next, notification_objects)

        try:
            # Add full report, both for the relevant day and the next day
            add_report = \
                user['notifications']['report']['full_report'][relevant_day_date.day_of_week] \
                or (user['notifications']['report']['report_on_notifications'][relevant_day_date.day_of_week] and len(notification_objects.get(str(user["_id"]), [])) > 0)
            
            if add_report:
                ensure_user_in_dict(notification_objects, user)

                # relevant_user_meals = user['meals'][relevant_day_date.day_of_week][:3] + 
                notification_objects[str(user["_id"])]['report'] = {
                    "first_on": relevant_day_label,
                    "next_on": relevant_day_label_next,
                    "first_meals": user['meals'][relevant_day_date.day_of_week] + [not any(user['meals'][relevant_day_date.day_of_week])],
                    "next_meals": user['meals'][relevant_next_day_date.day_of_week] + [not any(user['meals'][relevant_next_day_date.day_of_week])]
                }
        except KeyError:
            pass

    return notification_objects