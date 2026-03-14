from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def get_sheets_service(credentials_dict: dict):
    """Authenticates and builds the Sheets API service.

    Args:
        credentials_dict: Service account credentials as a Python dict,
                          loaded from the 'GCP_AUTH' key in the env config Variable.
    """
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)

    try:
        return build('sheets', 'v4', credentials=creds)
    except HttpError as err:
        print(err)
        return None
