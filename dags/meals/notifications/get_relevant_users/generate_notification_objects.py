import pendulum
import json


def generate_notification_objects_task(user_objects):
    """
    Get the users that should be notified about the meals.
    """

    # convert to json and print
    for user in user_objects:
        user_json = json.dumps({
            "email": user.email,
            "message": f"Hello {user.firstName}, you have a new meal notification!"
        })
        print(user_json)