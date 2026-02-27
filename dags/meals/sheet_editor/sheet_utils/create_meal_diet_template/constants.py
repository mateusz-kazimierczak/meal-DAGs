# Define meal categories for total diners calculation
MEAL_CATEGORIES = {
    "Breakfast": ["Breakfast"],
    "Dinner": ["Supper at table", "Late Supper (normal)", "Late Supper in container"],
    "Lunch": ["Pack Lunch P1", "Pack Lunch P2", "Pack Supper", "Lunch"]
}

# Mapping of short meal keys to their full display names (main table)
MEAL_TYPES_MAP = {
    "B": "Breakfast",
    "L": "Lunch",
    "S": "Supper at table",
    "LS": "Late Supper (normal)",
    "LSC": "Late Supper in container",
}

# Mapping of short packed meal keys to their full display names
PACKED_MEAL_TYPES = {
    "P1": "Pack Lunch P1",
    "P2": "Pack Lunch P2",
    "PS": "Pack Supper"
}

# Additional columns as key, add checkbox for the given meal types only
ADDITIONAL_COLS = {"At Snack": ["P1", "P2", "PS"]}
