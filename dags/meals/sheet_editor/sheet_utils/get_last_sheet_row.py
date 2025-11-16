from googleapiclient.errors import HttpError
# Authentication setup is assumed to be complete

def get_last_row_number_in_column(service, spreadsheet_id, sheet_name, column='B'):
    """
    Gets the row number of the last populated row *only* in the specified column.
    """
    # Define the range specifically for the specified column
    range_name = f"'{sheet_name}'!{column}:{column}"
    
    try:
        # Request data only for the specified column
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])

        if not values:
            print(f"No data found in Column {column} of '{sheet_name}'.")
            return 0

        # The length of the 'values' list is the total number of rows with data in the specified column
        last_row_number = len(values)
        return last_row_number

    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

# Example usage (assuming 'service' and 'SPREADSHEET_ID' are defined and authenticated)
# SPREADSHEET_ID = 'your_spreadsheet_id'
# SHEET_NAME = 'Sheet1' # The name of the sheet you want to check

# last_row_b = get_last_row_number_in_column_B(service, SPREADSHEET_ID, SHEET_NAME)
# if last_row_b is not None:
#     print(f"The last populated row number in Column B is: {last_row_b}")

