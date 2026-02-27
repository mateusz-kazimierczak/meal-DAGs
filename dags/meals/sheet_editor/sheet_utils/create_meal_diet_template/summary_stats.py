def update_summary_statistics(service, spreadsheet_id, sheet_name, daily_average_row):
    """Update the running-average formula (B2) and days-count (B3) at the top of the sheet.

    Each day the DAG runs, B2 accumulates another AVERAGE reference and B3 increments by 1.

    Args:
        service: Authorized Google Sheets API service instance.
        spreadsheet_id: The ID of the target spreadsheet.
        sheet_name: The name of the sheet.
        daily_average_row: The 1-based row number where this day's Average value lives
                           (in the Total Diners section, column E).
    """
    # Read current B2 (average formula) and B3 (days count)
    current_stats = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f'{sheet_name}!B2:B3'
    ).execute()

    current_values = current_stats.get('values', [[0], [0]])
    current_avg = current_values[0][0] if current_values and len(current_values) > 0 else 0
    current_days = current_values[1][0] if current_values and len(current_values) > 1 else 0

    if current_days == 0 or current_days == '0':
        # First day — just reference this day's average
        new_avg_formula = f"=E{daily_average_row}"
        new_days_formula = 1
    else:
        # Extend the existing formula
        current_formula_response = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!B2',
            valueRenderOption='FORMULA'
        ).execute()

        current_formula = (
            current_formula_response.get('values', [['']])[0][0]
            if current_formula_response.get('values')
            else ''
        )

        if current_formula and current_formula.startswith('='):
            if 'AVERAGE' in current_formula:
                cells = current_formula.replace('=AVERAGE(', '').replace(')', '')
                new_avg_formula = f"=AVERAGE({cells},E{daily_average_row})"
            else:
                old_cell = current_formula.replace('=', '')
                new_avg_formula = f"=AVERAGE({old_cell},E{daily_average_row})"
        else:
            new_avg_formula = f"=E{daily_average_row}"

        # Increment days count
        if isinstance(current_days, str) and current_days.startswith('='):
            new_days_formula = f"={current_days.replace('=', '')}+1"
        else:
            try:
                new_days_formula = int(current_days) + 1
            except (ValueError, TypeError):
                new_days_formula = 1

    # Write back
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f'{sheet_name}!B2:B3',
        valueInputOption='USER_ENTERED',
        body={'values': [[new_avg_formula], [new_days_formula]]}
    ).execute()

    print("Updated summary statistics: Average formula and days count.")
