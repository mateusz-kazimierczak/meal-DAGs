from google.oauth2.service_account import Credentials 
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os

def get_sheets_service(file_path="/home/mateusz/secrets/sheets_sa.json"):
    """Authenticates and builds the Sheets API service."""

    SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    
    creds = None
    # Load your credentials here. This is a placeholder for a robust auth flow.
    # For a full auth implementation, refer to the Google Sheets API Python Quickstart guide.
    if os.path.exists(file_path):
        creds = Credentials.from_authorized_user_file(file_path, SCOPES)
    if not creds or not creds.valid:
        # Code to refresh or prompt for login would go here
        pass
    
    try:
        service = build('sheets', 'v4', credentials=creds) # Build the service using discovery
        return service
    except HttpError as err:
        print(err)
        return None
