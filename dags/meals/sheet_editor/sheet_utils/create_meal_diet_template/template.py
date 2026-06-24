import string

from .constants import LUNCH_MEAL_TYPES_MAP, DINNER_MEAL_TYPES_MAP, PACKED_MEAL_TYPES
from .data_preparation import (
    resolve_diets,
    prepare_main_table,
    prepare_packed_meals,
    prepare_prediction,
    prepare_total_diners,
)
from .formatting import (
    build_main_table_formatting,
    build_packed_meals_formatting,
    build_prediction_formatting,
    build_total_diners_formatting,
)
from .grouping import (
    build_collapse_existing_row_groups_requests,
    build_current_day_row_group_requests,
)
from .summary_stats import update_summary_statistics


def create_meal_template(service, spreadsheet_id, sheet_name, start_row_index, input_data):
    """
    Creates a meal and diet summary table in a Google Sheet starting at a specific row index.
    The table will always start at column A.

    Args:
        service: Authorized Google Sheets API service instance.
        spreadsheet_id: The ID of the target spreadsheet.
        sheet_name: The name of the sheet to update.
        start_row_index: The 1-based index of the row where the table should start (e.g., 4 for row 4).
        input_data: A dictionary containing 'date', 'today' and 'tomorrow'.
    """
    date_val = input_data.get("date", "N/A")
    data_dict = input_data.get("today", {})
    tomorrow_data = input_data.get("tomorrow", {})

    # ------------------------------------------------------------------
    # 1. Resolve diets and user counts
    # ------------------------------------------------------------------
    all_diets, diet_user_counts = resolve_diets(data_dict)

    # ------------------------------------------------------------------
    # 2. Prepare data for every section
    # ------------------------------------------------------------------
    start_col_index = 0  # Column A
    start_col_letter = 'A'
    num_columns = len(all_diets) + 2
    end_col_letter = string.ascii_uppercase[(start_col_index + num_columns - 1) % 26]
    start_row = start_row_index

    lunch_headers, lunch_table_data, _, _ = prepare_main_table(
        data_dict, all_diets, diet_user_counts, LUNCH_MEAL_TYPES_MAP, "Lunch"
    )
    lunch_end_row = start_row + len(lunch_table_data)

    # Lunch table range
    lunch_range = f'{sheet_name}!{start_col_letter}{start_row}:{end_col_letter}{lunch_end_row}'
    lunch_rows = [lunch_headers] + lunch_table_data

    batch_data = [
        {'range': lunch_range, 'values': lunch_rows}
    ]

    dinner_start_row = lunch_end_row + 2
    dinner_headers, dinner_table_data, _, _ = prepare_main_table(
        data_dict, all_diets, diet_user_counts, DINNER_MEAL_TYPES_MAP, "Dinner"
    )
    dinner_end_row = dinner_start_row + len(dinner_table_data)
    dinner_range = f'{sheet_name}!{start_col_letter}{dinner_start_row}:{end_col_letter}{dinner_end_row}'
    dinner_rows = [dinner_headers] + dinner_table_data
    batch_data.append({'range': dinner_range, 'values': dinner_rows})

    # Date / title row (one row above header)
    date_range = f'{sheet_name}!{start_col_letter}{start_row - 1}'
    batch_data.append({
        'range': date_range,
        'values': [[f"Meals for date: {date_val.format('D-M-YYYY')}"]]
    })

    # ------------------------------------------------------------------
    # 3. Packed meals section
    # ------------------------------------------------------------------
    packed_start_row = dinner_end_row + 2
    _, packed_all_rows, _ = prepare_packed_meals(tomorrow_data, all_diets, diet_user_counts)
    packed_end_row = packed_start_row + len(packed_all_rows) - 1
    packed_range = f'{sheet_name}!{start_col_letter}{packed_start_row}:{end_col_letter}{packed_end_row}'
    batch_data.append({'range': packed_range, 'values': packed_all_rows})

    # ------------------------------------------------------------------
    # 4. Prediction section
    # ------------------------------------------------------------------
    prediction_start_row = packed_end_row + 2
    prediction_rows = prepare_prediction(tomorrow_data)
    prediction_range = (
        f'{sheet_name}!{start_col_letter}{prediction_start_row}:'
        f'{chr(ord(start_col_letter) + 1)}{prediction_start_row + len(prediction_rows) - 1}'
    )
    batch_data.append({'range': prediction_range, 'values': prediction_rows})

    # ------------------------------------------------------------------
    # 5. Total Diners section (columns D–E, same start row as prediction)
    # ------------------------------------------------------------------
    total_diners_rows = prepare_total_diners(
        (
            (LUNCH_MEAL_TYPES_MAP, start_row),
            (DINNER_MEAL_TYPES_MAP, dinner_start_row),
        ),
        PACKED_MEAL_TYPES,
        packed_start_row,
        prediction_start_row,
    )
    total_diners_range = (
        f'{sheet_name}!D{prediction_start_row}:E{prediction_start_row + len(total_diners_rows) - 1}'
    )
    batch_data.append({'range': total_diners_range, 'values': total_diners_rows})

    # ------------------------------------------------------------------
    # 6. Execute the values batch update
    # ------------------------------------------------------------------
    result = service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'valueInputOption': 'USER_ENTERED', 'data': batch_data}
    ).execute()
    print(f"Template created and {result.get('totalUpdatedCells')} cells updated successfully.")

    # ------------------------------------------------------------------
    # 7. Update running summary statistics (B2 / B3)
    # ------------------------------------------------------------------
    daily_average_row = prediction_start_row + len(total_diners_rows) - 1
    update_summary_statistics(service, spreadsheet_id, sheet_name, daily_average_row)

    # ------------------------------------------------------------------
    # 8. Apply formatting
    # ------------------------------------------------------------------
    sheet_metadata = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields='sheets(properties(sheetId,title),rowGroups)'
    ).execute()
    sheet_id = None
    for sheet in sheet_metadata.get('sheets', []):
        if sheet.get('properties', {}).get('title') == sheet_name:
            sheet_id = sheet.get('properties', {}).get('sheetId')
            break

    if sheet_id is not None:
        start_col_idx = start_col_index
        end_col_idx = start_col_index + num_columns - 1

        # Re-derive section row boundaries (same logic as above)
        prediction_end_row = prediction_start_row + len(prediction_rows)
        total_diners_end_row = prediction_start_row + len(total_diners_rows)

        formatting_requests = []
        formatting_requests.extend(
            build_collapse_existing_row_groups_requests(sheet_metadata, sheet_id)
        )
        formatting_requests.extend(
            build_current_day_row_group_requests(sheet_id, start_row, total_diners_end_row)
        )
        formatting_requests.extend(
            build_main_table_formatting(
                sheet_id, all_diets, diet_user_counts,
                start_col_idx, end_col_idx, start_row, lunch_end_row, LUNCH_MEAL_TYPES_MAP
            )
        )
        formatting_requests.extend(
            build_main_table_formatting(
                sheet_id, all_diets, diet_user_counts,
                start_col_idx, end_col_idx, dinner_start_row, dinner_end_row, DINNER_MEAL_TYPES_MAP
            )
        )
        formatting_requests.extend(
            build_packed_meals_formatting(
                sheet_id, all_diets, diet_user_counts,
                start_col_idx, end_col_idx, packed_start_row, packed_end_row
            )
        )
        formatting_requests.extend(
            build_prediction_formatting(sheet_id, start_col_idx, prediction_start_row, prediction_end_row)
        )
        formatting_requests.extend(
            build_total_diners_formatting(sheet_id, prediction_start_row, total_diners_end_row)
        )

        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': formatting_requests}
        ).execute()
        print("Formatting applied successfully.")

    return result
