from .constants import ADDITIONAL_COLS, MEAL_TYPES_MAP
from .data_preparation import _is_numeric_diet


def _border_block(sheet_id, start_row, end_row, start_col, end_col):
    """Return an updateBorders request dict with standard solid borders."""
    return {
        'updateBorders': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row,
                'endRowIndex': end_row,
                'startColumnIndex': start_col,
                'endColumnIndex': end_col
            },
            'top':    {'style': 'SOLID', 'width': 2, 'color': {'red': 0, 'green': 0, 'blue': 0}},
            'bottom': {'style': 'SOLID', 'width': 2, 'color': {'red': 0, 'green': 0, 'blue': 0}},
            'left':   {'style': 'SOLID', 'width': 2, 'color': {'red': 0, 'green': 0, 'blue': 0}},
            'right':  {'style': 'SOLID', 'width': 2, 'color': {'red': 0, 'green': 0, 'blue': 0}},
            'innerHorizontal': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}},
            'innerVertical':   {'style': 'SOLID', 'width': 1, 'color': {'red': 0.7, 'green': 0.7, 'blue': 0.7}}
        }
    }


def _center_align_block(sheet_id, start_row, end_row, start_col, end_col):
    """Return a repeatCell request that centre-aligns a range."""
    return {
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row,
                'endRowIndex': end_row,
                'startColumnIndex': start_col,
                'endColumnIndex': end_col
            },
            'cell': {
                'userEnteredFormat': {
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE'
                }
            },
            'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
        }
    }


def _header_format_block(sheet_id, start_row, end_row, start_col, end_col, alignment='CENTER'):
    """Return a repeatCell request that formats a header row (bold, grey bg)."""
    return {
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row,
                'endRowIndex': end_row,
                'startColumnIndex': start_col,
                'endColumnIndex': end_col
            },
            'cell': {
                'userEnteredFormat': {
                    'textFormat': {'bold': True, 'fontSize': 11},
                    'backgroundColor': {'red': 0.85, 'green': 0.85, 'blue': 0.85},
                    'horizontalAlignment': alignment,
                    'verticalAlignment': 'MIDDLE'
                }
            },
            'fields': 'userEnteredFormat(textFormat,backgroundColor,horizontalAlignment,verticalAlignment)'
        }
    }


def _totals_row_format(sheet_id, start_row, end_row, start_col, end_col):
    """Return a repeatCell request for a grand-totals / average row."""
    return {
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row,
                'endRowIndex': end_row,
                'startColumnIndex': start_col,
                'endColumnIndex': end_col
            },
            'cell': {
                'userEnteredFormat': {
                    'textFormat': {'bold': True},
                    'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.85}
                }
            },
            'fields': 'userEnteredFormat(textFormat,backgroundColor)'
        }
    }


def _checkbox_request(sheet_id, start_row, end_row, col_idx):
    """Return a setDataValidation request for a BOOLEAN checkbox column."""
    return {
        'setDataValidation': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row,
                'endRowIndex': end_row,
                'startColumnIndex': col_idx,
                'endColumnIndex': col_idx + 1
            },
            'rule': {
                'condition': {'type': 'BOOLEAN'},
                'strict': True,
                'showCustomUi': True
            }
        }
    }


# ---------------------------------------------------------------------------
# Public builders
# ---------------------------------------------------------------------------

def build_main_table_formatting(
    sheet_id, all_diets, diet_user_counts,
    start_col_idx, end_col_idx, start_row, end_row, num_headers
):
    """Return formatting requests for the main meals table.

    Skips checkbox validation for diets that have >1 user (numeric columns).
    """
    requests = []
    diet_col_offset = 2  # Skip "Meal Type" and "Total"

    for idx, diet in enumerate(all_diets):
        col_idx = start_col_idx + diet_col_offset + idx

        if diet in ADDITIONAL_COLS:
            # Checkboxes only on specific meal-type rows
            allowed_meal_keys = ADDITIONAL_COLS[diet]
            for row_offset, (meal_key, _) in enumerate(MEAL_TYPES_MAP.items()):
                if meal_key in allowed_meal_keys:
                    requests.append(_checkbox_request(
                        sheet_id,
                        start_row + row_offset,
                        start_row + row_offset + 1,
                        col_idx
                    ))
        elif _is_numeric_diet(diet, diet_user_counts):
            # Numeric diet — no checkbox validation needed
            pass
        else:
            # Single-user diet — checkbox for all meal rows (exclude totals)
            requests.append(_checkbox_request(
                sheet_id,
                start_row,
                end_row - 1,
                col_idx
            ))

    requests.extend([
        # Bold header row
        _header_format_block(sheet_id, start_row - 1, start_row, start_col_idx, end_col_idx + 1),
        # Grand totals row
        _totals_row_format(sheet_id, end_row - 1, end_row, start_col_idx, end_col_idx + 1),
        # Bold date / title row
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
                        'textFormat': {'bold': True, 'fontSize': 12}
                    }
                },
                'fields': 'userEnteredFormat(textFormat)'
            }
        },
        # Borders
        _border_block(sheet_id, start_row - 1, end_row, start_col_idx, end_col_idx + 1),
        # Centre-align data cells
        _center_align_block(sheet_id, start_row, end_row, start_col_idx, end_col_idx + 1),
        # Auto-resize columns
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
        # Set minimum column width
        {
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': start_col_idx,
                    'endIndex': end_col_idx + 1
                },
                'properties': {'pixelSize': 150},
                'fields': 'pixelSize'
            }
        }
    ])

    return requests


def build_packed_meals_formatting(
    sheet_id, all_diets, diet_user_counts,
    start_col_idx, end_col_idx, packed_start_row, packed_end_row
):
    """Return formatting requests for the packed-meals section.

    Skips checkbox validation for numeric diets.
    """
    requests = []

    for idx, diet in enumerate(all_diets):
        col_idx = start_col_idx + 2 + idx

        if diet in ADDITIONAL_COLS:
            allowed_meal_keys = ADDITIONAL_COLS[diet]
            packed_meal_keys = ["P1", "P2", "PS"]
            for row_offset, meal_key in enumerate(packed_meal_keys):
                if meal_key in allowed_meal_keys:
                    requests.append(_checkbox_request(
                        sheet_id,
                        packed_start_row + row_offset,
                        packed_start_row + row_offset + 1,
                        col_idx
                    ))
        elif _is_numeric_diet(diet, diet_user_counts):
            pass
        else:
            requests.append(_checkbox_request(
                sheet_id,
                packed_start_row,
                packed_end_row - 1,
                col_idx
            ))

    requests.extend([
        # Header
        _header_format_block(sheet_id, packed_start_row - 1, packed_start_row, start_col_idx, end_col_idx + 1),
        # Wider first column
        {
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': start_col_idx,
                    'endIndex': start_col_idx + 1
                },
                'properties': {'pixelSize': 220},
                'fields': 'pixelSize'
            }
        },
        # Grand totals row
        _totals_row_format(sheet_id, packed_end_row - 1, packed_end_row, start_col_idx, end_col_idx + 1),
        # Borders
        _border_block(sheet_id, packed_start_row - 1, packed_end_row, start_col_idx, end_col_idx + 1),
        # Centre-align
        _center_align_block(sheet_id, packed_start_row, packed_end_row, start_col_idx, end_col_idx + 1),
    ])

    return requests


def build_prediction_formatting(sheet_id, start_col_idx, prediction_start_row, prediction_end_row):
    """Return formatting requests for the prediction section."""
    return [
        _header_format_block(sheet_id, prediction_start_row - 1, prediction_start_row, start_col_idx, start_col_idx + 2, alignment='LEFT'),
        _border_block(sheet_id, prediction_start_row - 1, prediction_end_row, start_col_idx, start_col_idx + 2),
        _center_align_block(sheet_id, prediction_start_row, prediction_end_row, start_col_idx, start_col_idx + 2),
    ]


def build_total_diners_formatting(sheet_id, total_diners_start_row, total_diners_end_row):
    """Return formatting requests for the total-diners section (columns D–E)."""
    return [
        _header_format_block(sheet_id, total_diners_start_row - 1, total_diners_start_row, 3, 5, alignment='LEFT'),
        _border_block(sheet_id, total_diners_start_row - 1, total_diners_end_row, 3, 5),
        _center_align_block(sheet_id, total_diners_start_row, total_diners_end_row, 3, 5),
        _totals_row_format(sheet_id, total_diners_end_row - 1, total_diners_end_row, 3, 5),
    ]
