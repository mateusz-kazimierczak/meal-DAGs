from googleapiclient.discovery import build
import string

from meals._common.mongo_utils.get_diets import get_all_diets

# Define meal categories for total diners calculation
MEAL_CATEGORIES = {
    "Breakfast": ["Breakfast"],
    "Dinner": ["Supper at table", "Late Supper (normal)", "Late Supper in container"],
    "Lunch": ["Pack Lunch P1", "Pack Lunch P2", "Pack Supper", "Lunch"]
}

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

    # 1. Define Headers and Diets
    meal_types_map = {
        "B": "Breakfast",
        "L": "Lunch",
        "S": "Supper at table",
        "LS": "Late Supper (normal)",
        "LSC": "Late Supper in container",
    }

    # Extract all unique diet types from the data dictionary to create columns
    # We will sort them alphabetically for consistent column order
    day_diets = set(diet for meal_key in data_dict for diet in data_dict[meal_key]["hasDiet"].keys())
    # Add diets from the database to ensure all are shown

    db_diets = get_all_diets()

    # Additional column as key, add checkbox for the given meal types only
    additional_cols = {"At Snack": ["P1", "P2", "PS"]}

    all_diets = sorted(list(set(day_diets) | set(db_diets)))

    # Add the additional columns at the end
    all_diets.extend(additional_cols.keys())

    headers = ["Meal Type", "Total"] + all_diets

    # 2. Prepare the data matrix for the table body
    table_data = []
    grand_total_meals = 0
    diet_totals = {diet: 0 for diet in all_diets}

    # Iterate through all meal types in the defined order, not just those with data
    for meal_key_short, meal_name in meal_types_map.items():
        # Get meal details if they exist, otherwise use default empty values
        if meal_key_short in data_dict:
            meal_details = data_dict[meal_key_short]
            number_of_meals = meal_details["number"]
            has_diets = meal_details["hasDiet"]  # This is now a dict with counts
        else:
            # No data for this meal type, show as empty
            number_of_meals = 0
            has_diets = {}
        
        grand_total_meals += number_of_meals

        # Build the row: [Meal Name, Total Count, Checkboxes for each diet...]
        row = [meal_name, number_of_meals]
        for diet in all_diets:
            # Check if this diet is an additional column with specific meal type requirements
            if diet in additional_cols:
                # Only add checkbox if this meal type is in the allowed list for this column
                if meal_key_short in additional_cols[diet]:
                    # Check if this meal type has this diet restriction
                    if diet in has_diets and has_diets[diet] > 0:
                        row.append(True)
                        diet_totals[diet] += has_diets[diet]
                    else:
                        row.append(False)
                else:
                    # Leave blank for meal types not in the allowed list
                    row.append("")
            else:
                # Regular diet column - add checkbox for all meal types
                if diet in has_diets and has_diets[diet] > 0:
                    row.append(True)  # TRUE renders as a checked checkbox in Google Sheets
                    diet_totals[diet] += has_diets[diet]
                else:
                    row.append(False)  # FALSE renders as an unchecked checkbox

        table_data.append(row)

    # 3. Prepare the Grand Totals Row
    grand_totals_row = ["Grand totals", grand_total_meals] + [diet_totals[diet] for diet in all_diets]
    table_data.append(grand_totals_row)

    # 4. Determine the A1 notation for the target range
    # Always start at column A (index 0)
    start_col_index = 0  # Column A
    start_col_letter = 'A'
    end_col_letter = string.ascii_uppercase[(start_col_index + len(headers) - 1) % 26]
    start_row = start_row_index

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
        'values': [[f"Meals for date: {date_val.format('D-M-YYYY')}"]]
    })

    # Add "Packed Meals for Tomorrow" section
    tomorrow_data = input_data.get("tomorrow", {})
    packed_start_row = end_row + 2  # 2 rows below the main table
    
    # Define packed meal types
    packed_meal_types = {
        "P1": "Pack Lunch P1",
        "P2": "Pack Lunch P2",
        "PS": "Pack Supper"
    }
    
    # Prepare packed meals data with diets
    packed_table_data = []
    packed_grand_total = 0
    packed_diet_totals = {diet: 0 for diet in all_diets}
    
    for meal_key, meal_name in packed_meal_types.items():
        if meal_key in tomorrow_data:
            meal_details = tomorrow_data[meal_key]
            number_of_meals = meal_details.get("number", 0)
            has_diets = meal_details.get("hasDiet", {})
        else:
            number_of_meals = 0
            has_diets = {}
        
        packed_grand_total += number_of_meals
        
        # Build row with checkboxes for diets
        row = [meal_name, number_of_meals]
        for diet in all_diets:
            if diet in has_diets and has_diets[diet] > 0:
                row.append(True)
                packed_diet_totals[diet] += has_diets[diet]
            else:
                row.append(False)
        
        packed_table_data.append(row)
    
    # Add grand totals row for packed meals
    packed_totals_row = ["Grand totals", packed_grand_total] + [packed_diet_totals[diet] for diet in all_diets]
    packed_table_data.append(packed_totals_row)
    
    # Add headers and data for packed meals
    packed_headers = ["Packed Meals for Tomorrow", "Total"] + all_diets
    packed_all_rows = [packed_headers] + packed_table_data
    
    packed_end_row = packed_start_row + len(packed_all_rows) - 1
    packed_range = f'{sheet_name}!{start_col_letter}{packed_start_row}:{end_col_letter}{packed_end_row}'
    batch_data.append({
        'range': packed_range,
        'values': packed_all_rows
    })

    # Add "Prediction for Tomorrow" section (now below packed meals)
    prediction_start_row = packed_end_row + 2  # 2 rows below the packed meals table
    
    # Prepare tomorrow's prediction data - B, L, S are just counts, not dicts
    prediction_rows = [
        ["Prediction for Tomorrow", ""],  # Header row with empty second cell
        ["Breakfast", tomorrow_data.get("B", 0)],
        ["Breakfast with snack", ""],  # Leave blank as requested
        ["Lunch", tomorrow_data.get("L", 0)],
        ["Supper", tomorrow_data.get("S", 0)]
    ]
    
    prediction_range = f'{sheet_name}!{start_col_letter}{prediction_start_row}:{chr(ord(start_col_letter) + 1)}{prediction_start_row + len(prediction_rows) - 1}'
    batch_data.append({
        'range': prediction_range,
        'values': prediction_rows
    })

    # Add "Total Diners" section (to the left of prediction section, columns D and E)
    total_diners_start_row = prediction_start_row  # Same row as prediction section
    
    # Create a mapping of meal names to their row positions in the main table
    meal_name_to_row = {}
    for idx, (meal_key, meal_name) in enumerate(meal_types_map.items()):
        meal_name_to_row[meal_name] = start_row + idx  # +1 for header, but 0-indexed
    
    # Also map packed meal names to their row positions in the packed meals table
    packed_meal_name_to_row = {}
    for idx, (meal_key, meal_name) in enumerate(packed_meal_types.items()):
        packed_meal_name_to_row[meal_name] = packed_start_row + idx
    
    # Build formulas for each category
    total_diners_rows = [
        ["Total Diners", ""]  # Header row
    ]
    
    category_formulas = {}
    for category, meal_names in MEAL_CATEGORIES.items():
        # Build SUM formula by finding the appropriate cell references
        cell_refs = []
        for meal_name in meal_names:
            if meal_name in meal_name_to_row:
                # Reference from main table (column B is the Total column)
                row_num = meal_name_to_row[meal_name] + 1  # +1 for 1-based indexing
                cell_refs.append(f"B{row_num}")
            elif meal_name in packed_meal_name_to_row:
                # Reference from packed meals table (column B is the Total column)
                row_num = packed_meal_name_to_row[meal_name] + 1  # +1 for 1-based indexing
                cell_refs.append(f"B{row_num}")
        
        # Create the SUM formula
        if cell_refs:
            formula = f"=SUM({','.join(cell_refs)})"
        else:
            formula = 0
        
        total_diners_rows.append([category, formula])
    
    # Add Average row - calculate average of the three categories
    # The three category rows are at total_diners_start_row, +1, +2 (0-indexed in sheet, so +1 for 1-based)
    breakfast_row = total_diners_start_row + 1
    dinner_row = total_diners_start_row + 2
    lunch_row = total_diners_start_row + 3
    average_formula = f"=ROUND(AVERAGE(E{breakfast_row},E{dinner_row},E{lunch_row}),1)"
    total_diners_rows.append(["Average", average_formula])
    
    # Place Total Diners in columns D and E (starting at column index 3)
    total_diners_range = f'{sheet_name}!D{total_diners_start_row}:E{total_diners_start_row + len(total_diners_rows) - 1}'
    batch_data.append({
        'range': total_diners_range,
        'values': total_diners_rows
    })

    # 6. Execute the batch update for values
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
    
    # Update the summary statistics at the top of the sheet (A2:B3)
    # The Average row in Total Diners is at row: total_diners_start_row + 4 (header + 3 categories + average)
    daily_average_row = total_diners_start_row + 4
    
    # Read current values from B2 and B3 to determine if this is the first day or not
    current_stats_range = f'{sheet_name}!B2:B3'
    current_stats = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=current_stats_range
    ).execute()
    
    current_values = current_stats.get('values', [[0], [0]])
    current_avg = current_values[0][0] if current_values and len(current_values) > 0 else 0
    current_days = current_values[1][0] if current_values and len(current_values) > 1 else 0
    
    # Check if current values are formulas or numbers
    # If it's 0, this is the first day, so just reference the daily average
    # Otherwise, we need to build a formula that averages all daily averages
    
    if current_days == 0 or current_days == '0':
        # First day - just reference this day's average
        new_avg_formula = f"=E{daily_average_row}"
        new_days_formula = 1
    else:
        # Not the first day - need to find all Average rows and average them
        # We'll use a different approach: read the current formula and extend it
        # For simplicity, we'll use AVERAGE of a range that includes all F columns where daily averages are
        # Since we don't know all the row numbers, we'll use a simpler approach:
        # Store in C2 a formula that calculates running average
        
        # Get the current formula from B2
        current_formula_response = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!B2',
            valueRenderOption='FORMULA'
        ).execute()
        
        current_formula = current_formula_response.get('values', [['']])[0][0] if current_formula_response.get('values') else ''
        
        if current_formula and current_formula.startswith('='):
            # Extract the cell references from existing formula and add new one
            # Current formula is like =F10 or =AVERAGE(F10,F25,...)
            if 'AVERAGE' in current_formula:
                # Remove =AVERAGE( and ) to get the cell list
                cells = current_formula.replace('=AVERAGE(', '').replace(')', '')
                new_avg_formula = f"=AVERAGE({cells},E{daily_average_row})"
            else:
                # Single cell reference, convert to AVERAGE
                old_cell = current_formula.replace('=', '')
                new_avg_formula = f"=AVERAGE({old_cell},E{daily_average_row})"
        else:
            # Fallback: just reference the new daily average
            new_avg_formula = f"=E{daily_average_row}"
        
        # Increment days count - it could be a number or formula
        if isinstance(current_days, str) and current_days.startswith('='):
            # It's a formula, just add 1 to it
            new_days_formula = f"={current_days.replace('=', '')}+1"
        else:
            # It's a number, increment it
            try:
                new_days_formula = int(current_days) + 1
            except (ValueError, TypeError):
                new_days_formula = 1
    
    # Update the summary statistics
    summary_update_range = f'{sheet_name}!B2:B3'
    summary_body = {
        'values': [
            [new_avg_formula],
            [new_days_formula]
        ]
    }
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=summary_update_range,
        valueInputOption='USER_ENTERED',
        body=summary_body
    ).execute()
    
    print(f"Updated summary statistics: Average formula and days count.")
    
    # 7. Apply formatting to make the table look better
    # Get sheet ID (needed for formatting requests)
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for sheet in sheet_metadata.get('sheets', []):
        if sheet.get('properties', {}).get('title') == sheet_name:
            sheet_id = sheet.get('properties', {}).get('sheetId')
            break
    
    if sheet_id is not None:
        # Convert column letters to indices
        start_col_idx = start_col_index
        end_col_idx = start_col_index + len(headers) - 1
        
        # Calculate section rows
        packed_start_row = end_row + 2
        packed_end_row = packed_start_row + 4  # Title + 3 rows of data (P1, P2, PS) + grand total
        prediction_start_row = packed_end_row + 2
        prediction_end_row = prediction_start_row + 4  # Title + 4 rows of data
        total_diners_start_row = prediction_start_row  # Same row as prediction
        total_diners_end_row = total_diners_start_row + 4  # Title + 3 categories (Breakfast, Dinner, Lunch) + Average
        
        formatting_requests = []
        
        # Add checkbox data validation to diet columns dynamically
        # We need to handle regular diet columns and additional columns separately
        diet_col_offset = 2  # Skip "Meal Type" and "Total" columns
        
        for idx, diet in enumerate(all_diets):
            col_idx = start_col_idx + diet_col_offset + idx
            
            if diet in additional_cols:
                # For additional columns, only add checkboxes to specific meal type rows
                allowed_meal_keys = additional_cols[diet]
                
                # Find the row indices for the allowed meal types
                for row_offset, (meal_key, meal_name) in enumerate(meal_types_map.items()):
                    if meal_key in allowed_meal_keys:
                        # Add checkbox for this specific cell
                        formatting_requests.append({
                            'setDataValidation': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': start_row + row_offset,
                                    'endRowIndex': start_row + row_offset + 1,
                                    'startColumnIndex': col_idx,
                                    'endColumnIndex': col_idx + 1
                                },
                                'rule': {
                                    'condition': {
                                        'type': 'BOOLEAN'
                                    },
                                    'strict': True,
                                    'showCustomUi': True
                                }
                            }
                        })
            else:
                # For regular diet columns, add checkboxes to all meal type rows
                formatting_requests.append({
                    'setDataValidation': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': start_row,
                            'endRowIndex': end_row - 1,  # Exclude the totals row
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'rule': {
                            'condition': {
                                'type': 'BOOLEAN'
                            },
                            'strict': True,
                            'showCustomUi': True
                        }
                    }
                })
        
        formatting_requests.extend([
            # Bold header row
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': start_row - 1,
                        'endRowIndex': start_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True,
                                'fontSize': 11
                            },
                            'backgroundColor': {
                                'red': 0.85,
                                'green': 0.85,
                                'blue': 0.85
                            },
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,backgroundColor,horizontalAlignment,verticalAlignment)'
                }
            },
            # Bold and highlight grand totals row
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': end_row - 1,
                        'endRowIndex': end_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True
                            },
                            'backgroundColor': {
                                'red': 0.95,
                                'green': 0.95,
                                'blue': 0.85
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,backgroundColor)'
                }
            },
            # Bold date/title row
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': start_row - 2,
                        'endRowIndex': start_row - 1,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True,
                                'fontSize': 12
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat)'
                }
            },
            # Add borders around the entire table
            {
                'updateBorders': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': start_row - 1,
                        'endRowIndex': end_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'top': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'bottom': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'left': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'right': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'innerHorizontal': {
                        'style': 'SOLID',
                        'width': 1,
                        'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}
                    },
                    'innerVertical': {
                        'style': 'SOLID',
                        'width': 1,
                        'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}
                    }
                }
            },
            # Center align all cells in the table
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': start_row,
                        'endRowIndex': end_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                }
            },
            # Auto-resize columns to fit content
            {
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': start_col_idx,
                        'endIndex': end_col_idx + 1
                    }
                }
            },
            # Add padding to columns for better spacing
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': start_col_idx,
                        'endIndex': end_col_idx + 1
                    },
                    'properties': {
                        'pixelSize': 150  # Set minimum column width
                    },
                    'fields': 'pixelSize'
                }
            }
        ])
        
        # Add checkbox data validation to packed meals diet columns dynamically
        # Handle regular diet columns and additional columns separately for packed meals
        for idx, diet in enumerate(all_diets):
            col_idx = start_col_idx + 2 + idx  # Skip "Meal Type" and "Total" columns
            
            if diet in additional_cols:
                # For additional columns in packed meals, check if any packed meal types match
                # Packed meals are always P1, P2, PS
                allowed_meal_keys = additional_cols[diet]
                packed_meal_keys = ["P1", "P2", "PS"]
                
                # Find which rows should have checkboxes
                for row_offset, meal_key in enumerate(packed_meal_keys):
                    if meal_key in allowed_meal_keys:
                        formatting_requests.append({
                            'setDataValidation': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': packed_start_row + row_offset,
                                    'endRowIndex': packed_start_row + row_offset + 1,
                                    'startColumnIndex': col_idx,
                                    'endColumnIndex': col_idx + 1
                                },
                                'rule': {
                                    'condition': {
                                        'type': 'BOOLEAN'
                                    },
                                    'strict': True,
                                    'showCustomUi': True
                                }
                            }
                        })
            else:
                # For regular diet columns, add checkboxes to all packed meal rows
                formatting_requests.append({
                    'setDataValidation': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': packed_start_row,
                            'endRowIndex': packed_end_row - 1,  # Exclude the grand totals row
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'rule': {
                            'condition': {
                                'type': 'BOOLEAN'
                            },
                            'strict': True,
                            'showCustomUi': True
                        }
                    }
                })
        
        formatting_requests.extend([
            # Format "Packed Meals for Tomorrow" header
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': packed_start_row - 1,
                        'endRowIndex': packed_start_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True,
                                'fontSize': 11
                            },
                            'backgroundColor': {
                                'red': 0.85,
                                'green': 0.85,
                                'blue': 0.85
                            },
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,backgroundColor,horizontalAlignment,verticalAlignment)'
                }
            },
            # Make the first column of packed meals section wider
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': start_col_idx,
                        'endIndex': start_col_idx + 1
                    },
                    'properties': {
                        'pixelSize': 220  # Wider for "Packed Meals for Tomorrow"
                    },
                    'fields': 'pixelSize'
                }
            },
            # Bold and highlight packed meals grand totals row
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': packed_end_row - 1,
                        'endRowIndex': packed_end_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True
                            },
                            'backgroundColor': {
                                'red': 0.95,
                                'green': 0.95,
                                'blue': 0.85
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,backgroundColor)'
                }
            },
            # Add border around packed meals section
            {
                'updateBorders': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': packed_start_row - 1,
                        'endRowIndex': packed_end_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'top': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'bottom': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'left': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'right': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'innerHorizontal': {
                        'style': 'SOLID',
                        'width': 1,
                        'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}
                    },
                    'innerVertical': {
                        'style': 'SOLID',
                        'width': 1,
                        'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}
                    }
                }
            },
            # Center align packed meals data cells
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': packed_start_row,
                        'endRowIndex': packed_end_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': end_col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                }
            },
            # Format "Prediction for Tomorrow" title
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': prediction_start_row - 1,
                        'endRowIndex': prediction_start_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': start_col_idx + 2
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True,
                                'fontSize': 11
                            },
                            'backgroundColor': {
                                'red': 0.85,
                                'green': 0.85,
                                'blue': 0.85
                            },
                            'horizontalAlignment': 'LEFT'
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)'
                }
            },
            # Add border around prediction section
            {
                'updateBorders': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': prediction_start_row - 1,
                        'endRowIndex': prediction_end_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': start_col_idx + 2
                    },
                    'top': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'bottom': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'left': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'right': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'innerHorizontal': {
                        'style': 'SOLID',
                        'width': 1,
                        'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}
                    },
                    'innerVertical': {
                        'style': 'SOLID',
                        'width': 1,
                        'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}
                    }
                }
            },
            # Center align prediction data cells
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': prediction_start_row,
                        'endRowIndex': prediction_end_row,
                        'startColumnIndex': start_col_idx,
                        'endColumnIndex': start_col_idx + 2
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                }
            },
            # Format "Total Diners" title
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': total_diners_start_row - 1,
                        'endRowIndex': total_diners_start_row,
                        'startColumnIndex': 3,  # Column D
                        'endColumnIndex': 5  # Column E
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True,
                                'fontSize': 11
                            },
                            'backgroundColor': {
                                'red': 0.85,
                                'green': 0.85,
                                'blue': 0.85
                            },
                            'horizontalAlignment': 'LEFT'
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)'
                }
            },
            # Add border around total diners section
            {
                'updateBorders': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': total_diners_start_row - 1,
                        'endRowIndex': total_diners_end_row,
                        'startColumnIndex': 3,  # Column D
                        'endColumnIndex': 5  # Column E
                    },
                    'top': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'bottom': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'left': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'right': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': {'red': 0, 'green': 0, 'blue': 0}
                    },
                    'innerHorizontal': {
                        'style': 'SOLID',
                        'width': 1,
                        'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}
                    },
                    'innerVertical': {
                        'style': 'SOLID',
                        'width': 1,
                        'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}
                    }
                }
            },
            # Center align total diners data cells
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': total_diners_start_row,
                        'endRowIndex': total_diners_end_row,
                        'startColumnIndex': 3,  # Column D
                        'endColumnIndex': 5  # Column E
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'horizontalAlignment': 'CENTER',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                }
            },
            # Bold and highlight Average row (like grand totals)
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': total_diners_end_row - 1,  # Last row is Average
                        'endRowIndex': total_diners_end_row,
                        'startColumnIndex': 3,  # Column D
                        'endColumnIndex': 5  # Column E
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True
                            },
                            'backgroundColor': {
                                'red': 0.95,
                                'green': 0.95,
                                'blue': 0.85
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,backgroundColor)'
                }
            }
        ])
        
        # Apply formatting
        format_body = {
            'requests': formatting_requests
        }
        
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=format_body
        ).execute()
        
        print("Formatting applied successfully.")
    
    return result

