import string

from meals._common.mongo_utils.get_diets import get_all_diets, get_diet_user_counts
from .constants import MEAL_TYPES_MAP, PACKED_MEAL_TYPES, ADDITIONAL_COLS, MEAL_CATEGORIES


def _is_numeric_diet(diet, diet_user_counts):
    """Return True if a diet should use numeric values instead of checkboxes.
    
    A diet uses numbers when more than 1 active user is assigned to it.
    """
    return diet_user_counts.get(diet, 0) > 1


def prepare_main_table(data_dict, all_diets, diet_user_counts):
    """Build the header row and data rows for the main meals table.

    For diets with >1 user, cells contain the numeric count (or 0) instead of True/False.

    Args:
        data_dict: Today's meal data keyed by short meal type.
        all_diets: Sorted list of all diet names + additional column names.
        diet_user_counts: Dict mapping diet names to active user counts.

    Returns:
        tuple: (headers, table_data, grand_totals_row, diet_totals)
    """
    headers = ["Meal Type", "Total"] + all_diets

    table_data = []
    grand_total_meals = 0
    diet_totals = {diet: 0 for diet in all_diets}

    for meal_key_short, meal_name in MEAL_TYPES_MAP.items():
        if meal_key_short in data_dict:
            meal_details = data_dict[meal_key_short]
            number_of_meals = meal_details["number"]
            has_diets = meal_details["hasDiet"]
        else:
            number_of_meals = 0
            has_diets = {}

        grand_total_meals += number_of_meals

        row = [meal_name, number_of_meals]
        for diet in all_diets:
            if diet in ADDITIONAL_COLS:
                # Additional columns only get values for specific meal types
                if meal_key_short in ADDITIONAL_COLS[diet]:
                    if diet in has_diets and has_diets[diet] > 0:
                        row.append(True)
                        diet_totals[diet] += has_diets[diet]
                    else:
                        row.append(False)
                else:
                    row.append("")
            elif _is_numeric_diet(diet, diet_user_counts):
                # Numeric diet: show actual count instead of checkbox
                count = has_diets.get(diet, 0)
                row.append(count)
                diet_totals[diet] += count
            else:
                # Single-user diet: use checkbox (True/False)
                if diet in has_diets and has_diets[diet] > 0:
                    row.append(True)
                    diet_totals[diet] += has_diets[diet]
                else:
                    row.append(False)

        table_data.append(row)

    grand_totals_row = ["Grand totals", grand_total_meals] + [diet_totals[diet] for diet in all_diets]
    table_data.append(grand_totals_row)

    return headers, table_data, grand_totals_row, diet_totals


def prepare_packed_meals(tomorrow_data, all_diets, diet_user_counts):
    """Build header and data rows for the packed-meals-for-tomorrow section.

    Args:
        tomorrow_data: Tomorrow's meal data keyed by short meal type.
        all_diets: Sorted list of all diet names + additional column names.
        diet_user_counts: Dict mapping diet names to active user counts.

    Returns:
        tuple: (packed_headers, packed_all_rows, packed_grand_total)
    """
    packed_table_data = []
    packed_grand_total = 0
    packed_diet_totals = {diet: 0 for diet in all_diets}

    for meal_key, meal_name in PACKED_MEAL_TYPES.items():
        if meal_key in tomorrow_data:
            meal_details = tomorrow_data[meal_key]
            number_of_meals = meal_details.get("number", 0)
            has_diets = meal_details.get("hasDiet", {})
        else:
            number_of_meals = 0
            has_diets = {}

        packed_grand_total += number_of_meals

        row = [meal_name, number_of_meals]
        for diet in all_diets:
            if _is_numeric_diet(diet, diet_user_counts):
                count = has_diets.get(diet, 0)
                row.append(count)
                packed_diet_totals[diet] += count
            else:
                if diet in has_diets and has_diets[diet] > 0:
                    row.append(True)
                    packed_diet_totals[diet] += has_diets[diet]
                else:
                    row.append(False)

        packed_table_data.append(row)

    packed_totals_row = ["Grand totals", packed_grand_total] + [packed_diet_totals[diet] for diet in all_diets]
    packed_table_data.append(packed_totals_row)

    packed_headers = ["Packed Meals for Tomorrow", "Total"] + all_diets
    packed_all_rows = [packed_headers] + packed_table_data

    return packed_headers, packed_all_rows, packed_grand_total


def prepare_prediction(tomorrow_data):
    """Build the prediction-for-tomorrow section rows.

    Args:
        tomorrow_data: Tomorrow's meal data.

    Returns:
        list: List of rows for the prediction section.
    """
    prediction_rows = [
        ["Prediction for Tomorrow", ""],
        ["Breakfast", tomorrow_data.get("B", 0)],
        ["Breakfast with snack", ""],
        ["Lunch", tomorrow_data.get("L", 0)],
        ["Supper", tomorrow_data.get("S", 0)]
    ]
    return prediction_rows


def prepare_total_diners(meal_types_map, packed_meal_types, start_row, packed_start_row, prediction_start_row):
    """Build the total-diners section with SUM and AVERAGE formulas.

    Args:
        meal_types_map: Ordered dict of short key -> full meal name (main table).
        packed_meal_types: Ordered dict of short key -> full meal name (packed).
        start_row: 1-based start row of the main table (header row).
        packed_start_row: 1-based start row of the packed meals section (header row).
        prediction_start_row: 1-based start row of the prediction/total-diners section.

    Returns:
        list: Rows for the Total Diners section including formulas.
    """
    # Map meal names to their data row (1-based, skipping header)
    meal_name_to_row = {}
    for idx, (_, meal_name) in enumerate(meal_types_map.items()):
        meal_name_to_row[meal_name] = start_row + idx

    packed_meal_name_to_row = {}
    for idx, (_, meal_name) in enumerate(packed_meal_types.items()):
        packed_meal_name_to_row[meal_name] = packed_start_row + idx

    total_diners_rows = [["Total Diners", ""]]

    for category, meal_names in MEAL_CATEGORIES.items():
        cell_refs = []
        for meal_name in meal_names:
            if meal_name in meal_name_to_row:
                row_num = meal_name_to_row[meal_name] + 1  # +1 for 1-based indexing
                cell_refs.append(f"B{row_num}")
            elif meal_name in packed_meal_name_to_row:
                row_num = packed_meal_name_to_row[meal_name] + 1
                cell_refs.append(f"B{row_num}")

        formula = f"=SUM({','.join(cell_refs)})" if cell_refs else 0
        total_diners_rows.append([category, formula])

    # Average of the three category values (E column)
    breakfast_row = prediction_start_row + 1
    dinner_row = prediction_start_row + 2
    lunch_row = prediction_start_row + 3
    average_formula = f"=ROUND(AVERAGE(E{breakfast_row},E{dinner_row},E{lunch_row}),1)"
    total_diners_rows.append(["Average", average_formula])

    return total_diners_rows


def resolve_diets(data_dict):
    """Collect and return the sorted list of all diets from day data and the database.

    Also fetches per-diet user counts from the users collection.

    Args:
        data_dict: Today's meal data.

    Returns:
        tuple: (all_diets, diet_user_counts) where all_diets includes additional columns.
    """
    day_diets = set(
        diet
        for meal_key in data_dict
        for diet in data_dict[meal_key]["hasDiet"].keys()
    )
    db_diets = get_all_diets()
    diet_user_counts = get_diet_user_counts()

    all_diets = sorted(list(set(day_diets) | set(db_diets)))
    all_diets.extend(ADDITIONAL_COLS.keys())

    return all_diets, diet_user_counts
