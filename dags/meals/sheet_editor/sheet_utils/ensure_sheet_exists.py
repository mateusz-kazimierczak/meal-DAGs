def ensure_sheet_exists(service, spreadsheet_id, sheet_name):
    """
    Ensures that a sheet with the given name exists in the specified spreadsheet.
    If the sheet does not exist, it creates a new one.
    """
    # 1. Retrieve the metadata to get existing sheet titles
    spreadsheet_metadata = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields='sheets(properties(title))' # Request only the sheet titles
    ).execute()

    existing_sheet_titles = [
        sheet['properties']['title']
        for sheet in spreadsheet_metadata.get('sheets', [])
    ]

    if sheet_name not in existing_sheet_titles:
        # Create new sheet
        add_sheet_request = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet_name,
                            "index": 1 # leaves space for a possible summary sheet at index 0
                        }
                    }
                }
            ]
        }
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=add_sheet_request
        ).execute()
        print(f"Created new sheet: {sheet_name}")
