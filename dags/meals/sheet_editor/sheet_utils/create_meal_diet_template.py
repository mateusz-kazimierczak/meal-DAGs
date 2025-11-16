from googleapiclient.discovery import build
import string

def create_meal_template(service, spreadsheet_id, sheet_name, start_col_index, input_data):
    """
    Creates a meal and diet summary table in a Google Sheet starting at a specific column index.

    Args:
        service: Authorized Google Sheets API service instance.
        spreadsheet_id: The ID of the target spreadsheet.
        start_col_index: The 0-based index of the column where the table should start (e.g., 0 for column A).
        input_data: A dictionary containing 'date' and 'dataDictionary'.
    """
    date_val = input_data.get("date", "N/A")
    data_dict = input_data.get("data", {})

    # 1. Define Headers and Diets
    meal_types_map = {
        "B": "Breakfast",
        "L": "Lunch",
        "S": "Supper at table",
        "P1": "Pack Lunch P1",
        "P2": "Pack Lunch P2",
        "PS": "Pack Supper",
        "LS": "Late Supper (normal)",
        "LSC": "Late Supper in container",
    }

    # Extract all unique diet types from the data dictionary to create columns
    # We will sort them alphabetically for consistent column order
    all_diets = sorted(list(set(diet for meal_key in data_dict for diet in data_dict[meal_key]["hasDiet"].keys())))

    headers = ["Meal Type", "Total"] + all_diets

    # 2. Prepare the data matrix for the table body
    table_data = []
    grand_total_meals = 0
    diet_totals = {diet: 0 for diet in all_diets}

    for meal_key_short, meal_details in data_dict.items():
        meal_name = meal_types_map.get(meal_key_short, meal_key_short)
        number_of_meals = meal_details["number"]
        grand_total_meals += number_of_meals
        has_diets = meal_details["hasDiet"]  # This is now a dict with counts

        # Build the row: [Meal Name, Total Count, Counts for each diet...]
        row = [meal_name, number_of_meals]
        for diet in all_diets:
            # Check if this meal type has this diet restriction and add the count
            if diet in has_diets:
                row.append(has_diets[diet])  # Add the actual count for this diet
                diet_totals[diet] += has_diets[diet]
            else:
                row.append("")  # Leave blank

        table_data.append(row)

    # 3. Prepare the Grand Totals Row
    grand_totals_row = ["Grand totals", grand_total_meals] + [diet_totals[diet] for diet in all_diets]
    table_data.append(grand_totals_row)

    # 4. Determine the A1 notation for the target range
    start_col_letter = string.ascii_uppercase[start_col_index % 26]
    end_col_letter = string.ascii_uppercase[(start_col_index + len(headers) - 1) % 26]
    start_row = 4 # Assuming we start around row 4 (like the image)

    # We need enough rows for all meal types + totals + headers (total rows + 1 header row)
    num_rows = len(table_data) + 1
    end_row = start_row + num_rows - 1

    # The entire range we will update in one batch
    target_range = f'{sheet_name}!{start_col_letter}{start_row}:{end_col_letter}{end_row}'

    # 5. Structure the data for the batchUpdate API call
    # The first row is headers, subsequent rows are the table body
    all_data_rows = [headers] + table_data
    
    batch_data = [
        {
            'range': target_range,
            'values': all_data_rows
        }
    ]

    # Optional: Add a range just above to put the date/title
    date_range = f'{sheet_name}!{start_col_letter}{start_row - 1}'
    batch_data.append({
        'range': date_range,
        'values': [[f"Data for Date: {date_val.format('D-M-YYYY')}"]]
    })

    # 6. Execute the batch update
    body = {
        'valueInputOption': 'USER_ENTERED',
        'data': batch_data
    }
    
    # Use the batchUpdate method, not values().batchUpdate
    result = service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()
    
    print(f"Template created and {result.get('totalUpdatedCells')} cells updated successfully.")
    return result

