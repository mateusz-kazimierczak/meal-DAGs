# Define meal categories for total diners calculation
MEAL_CATEGORIES = {
    "Dinner": ["Supper at table", "Late Supper (normal)", "Late Supper in container", "Tray (Supper)"],
    "Lunch": ["Pack Lunch P1", "Pack Lunch P2", "Pack Supper", "Lunch", "Tray (Lunch)"]
}

# Mapping of short meal keys to their full display names (main tables)
LUNCH_MEAL_TYPES_MAP = {
    "L": "Lunch",
    "TL": "Tray (Lunch)",
}

DINNER_MEAL_TYPES_MAP = {
    "S": "Supper at table",
    "LS": "Late Supper (normal)",
    "LSC": "Late Supper in container",
    "TS": "Tray (Supper)",
}

MEAL_TYPES_MAP = {
    **LUNCH_MEAL_TYPES_MAP,
    **DINNER_MEAL_TYPES_MAP,
}

# Mapping of short packed meal keys to their full display names
PACKED_MEAL_TYPES = {
    "PB": "Packed Breakfast",
    "P1": "Pack Lunch P1",
    "P2": "Pack Lunch P2",
    "PS": "Pack Supper"
}

# Additional columns as key, add checkbox for the given meal types only
ADDITIONAL_COLS = {}
