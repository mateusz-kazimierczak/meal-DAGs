def build_collapse_existing_row_groups_requests(sheet_metadata, sheet_id):
    """Return requests to collapse all existing row groups on the target sheet."""
    requests = []

    for sheet in sheet_metadata.get('sheets', []):
        properties = sheet.get('properties', {})
        if properties.get('sheetId') != sheet_id:
            continue

        for row_group in sheet.get('rowGroups', []):
            dimension_range = row_group.get('range', {})
            if dimension_range.get('dimension') != 'ROWS':
                continue

            requests.append({
                'updateDimensionGroup': {
                    'dimensionGroup': {
                        'range': dimension_range,
                        'depth': row_group.get('depth', 1),
                        'collapsed': True
                    },
                    'fields': 'collapsed'
                }
            })

        break

    return requests


def build_current_day_row_group_requests(sheet_id, start_row, end_row):
    """Return requests to make the current day details an expanded row group.

    The date/title row immediately above start_row is intentionally left outside
    the group so collapsed historical days still show their date label.
    """
    return [
        {
            'addDimensionGroup': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'startIndex': start_row - 1,
                    'endIndex': end_row
                }
            }
        },
        {
            'updateDimensionGroup': {
                'dimensionGroup': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'startIndex': start_row - 1,
                        'endIndex': end_row
                    },
                    'depth': 1,
                    'collapsed': False
                },
                'fields': 'collapsed'
            }
        }
    ]
