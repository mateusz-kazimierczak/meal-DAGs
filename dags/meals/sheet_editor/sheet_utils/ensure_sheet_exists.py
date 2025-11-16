def ensure_sheet_exists(service, spreadsheet_id, sheet_name):
    """
    Ensures that a sheet with the given name exists in the specified spreadsheet.
    If the sheet does not exist, it creates a new one and adds a header.
    Returns True if the sheet was newly created, False if it already existed.
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
        
        # Add header to the newly created sheet
        header_range = f'{sheet_name}!B1'
        header_body = {
            'values': [[sheet_name]]
        }
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=header_range,
            valueInputOption='USER_ENTERED',
            body=header_body
        ).execute()
        
        # Format the header
        # Get the sheet ID for formatting
        spreadsheet_metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        sheet_id = None
        for sheet in spreadsheet_metadata.get('sheets', []):
            if sheet.get('properties', {}).get('title') == sheet_name:
                sheet_id = sheet.get('properties', {}).get('sheetId')
                break
        
        if sheet_id is not None:
            format_request = {
                'requests': [
                    {
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 0,
                                'endRowIndex': 1,
                                'startColumnIndex': 1,  # Column B
                                'endColumnIndex': 2
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'textFormat': {
                                        'bold': True,
                                        'fontSize': 14
                                    },
                                    'horizontalAlignment': 'LEFT'
                                }
                            },
                            'fields': 'userEnteredFormat(textFormat,horizontalAlignment)'
                        }
                    }
                ]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=format_request
            ).execute()
        
        print(f"Added header to sheet: {sheet_name}")
        return True
    
    return False
