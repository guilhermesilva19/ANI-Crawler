"""Google Sheets service for logging crawler alerts."""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import json

from ..config import SCOPES, TOP_PARENT_ID

__all__ = ['SheetsService']


class SheetsService:
    """Service for logging crawler alerts to Google Sheets."""
    
    def __init__(self):
        """Initialize Google Sheets service."""
        self.sheets_service = None
        self.drive_service = None
        self.spreadsheet_id = None
        self.spreadsheet_url = None
        self._setup_services()
        self._setup_spreadsheet()

    def _setup_services(self) -> None:
        """Setup Google Sheets and Drive API services."""
        try:
            # Get credentials from environment
            private_key = os.getenv("PRIVATE_KEY")
            if not private_key:
                raise ValueError("PRIVATE_KEY environment variable not set")
            
            # Replace escaped newlines in private key
            private_key = private_key.replace('\\n', '\n')
            
            # Create credentials
            credentials_info = {
                "type": "service_account",
                "project_id": os.getenv("PROJECT_ID"),
                "private_key_id": os.getenv("PRIVATE_KEY_ID"),
                "private_key": private_key,
                "client_email": os.getenv("CLIENT_EMAIL"),
                "client_id": os.getenv("CLIENT_ID"),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{os.getenv('CLIENT_EMAIL')}"
            }
            
            credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
            
            # Build services
            self.sheets_service = build('sheets', 'v4', credentials=credentials)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            
            print("Google Sheets service initialized successfully")
            
        except Exception as e:
            print(f"Error setting up Google Sheets service: {e}")
            raise

    def _setup_spreadsheet(self) -> None:
        """Setup or find the crawler alerts spreadsheet."""
        try:
            # First, try to find existing spreadsheet in the folder
            spreadsheet_name = "ANI-Crawler-Alerts"
            existing_sheet = self._find_spreadsheet_in_folder(spreadsheet_name)
            
            if existing_sheet:
                self.spreadsheet_id = existing_sheet['id']
                self.spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
                print(f"Found existing alerts spreadsheet: {self.spreadsheet_url}")
            else:
                # Create new spreadsheet
                self._create_new_spreadsheet(spreadsheet_name)
                
        except Exception as e:
            print(f"Error setting up spreadsheet: {e}")
            raise

    def _find_spreadsheet_in_folder(self, name: str) -> Optional[Dict]:
        """Find spreadsheet by name in the TOP_PARENT_ID folder."""
        try:
            query = f"name='{name}' and parents in '{TOP_PARENT_ID}' and mimeType='application/vnd.google-apps.spreadsheet'"
            results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            return files[0] if files else None
        except Exception as e:
            print(f"Error finding spreadsheet: {e}")
            return None

    def _create_new_spreadsheet(self, name: str) -> None:
        """Create new spreadsheet in the Drive folder."""
        try:
            # Create spreadsheet
            spreadsheet_body = {
                'properties': {
                    'title': name
                }
            }
            
            spreadsheet = self.sheets_service.spreadsheets().create(body=spreadsheet_body).execute()
            self.spreadsheet_id = spreadsheet['spreadsheetId']
            self.spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
            
            # Move to crawler folder
            self.drive_service.files().update(
                fileId=self.spreadsheet_id,
                addParents=TOP_PARENT_ID,
                removeParents='root',
                fields='id, parents'
            ).execute()
            
            print(f"Created new alerts spreadsheet: {self.spreadsheet_url}")
            print(f"Spreadsheet moved to crawler folder: {TOP_PARENT_ID}")
            
        except Exception as e:
            print(f"Error creating spreadsheet: {e}")
            raise

    def get_or_create_monthly_tab(self, date: datetime = None) -> str:
        """Get or create a monthly tab for the given date."""
        if date is None:
            date = datetime.now()
        
        # Format: YYYY-MM
        tab_name = date.strftime("%Y-%m")
        
        try:
            # Check if tab exists
            spreadsheet = self.sheets_service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            existing_sheets = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
            
            if tab_name in existing_sheets:
                return tab_name
            
            # Create new tab
            self._create_monthly_tab(tab_name)
            return tab_name
            
        except Exception as e:
            print(f"Error getting/creating monthly tab: {e}")
            raise

    def _create_monthly_tab(self, tab_name: str) -> None:
        """Create a new monthly tab with headers."""
        try:
            # Add new sheet
            requests = [
                {
                    'addSheet': {
                        'properties': {
                            'title': tab_name
                        }
                    }
                }
            ]
            
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={'requests': requests}
            ).execute()
            
            # Add headers
            headers = [
                ['Timestamp', 'Alert Type', 'Page URL', 'Status Code', 'Description', 
                 'Screenshot Link', 'HTML Link', 'Last Success']
            ]
            
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{tab_name}!A1:H1",
                valueInputOption='RAW',
                body={'values': headers}
            ).execute()
            
            # Format headers (bold)
            format_requests = [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': self._get_sheet_id(tab_name),
                            'startRowIndex': 0,
                            'endRowIndex': 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {'bold': True}
                            }
                        },
                        'fields': 'userEnteredFormat.textFormat.bold'
                    }
                }
            ]
            
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={'requests': format_requests}
            ).execute()
            
            print(f"Created monthly tab: {tab_name}")
            
        except Exception as e:
            print(f"Error creating monthly tab {tab_name}: {e}")
            raise

    def _get_sheet_id(self, tab_name: str) -> int:
        """Get the sheet ID for a given tab name."""
        try:
            spreadsheet = self.sheets_service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == tab_name:
                    return sheet['properties']['sheetId']
            raise ValueError(f"Sheet {tab_name} not found")
        except Exception as e:
            print(f"Error getting sheet ID for {tab_name}: {e}")
            raise

    def log_alert(self, alert_type: str, page_url: str, status_code: int, 
                  description: str, screenshot_link: str = "", html_link: str = "", 
                  last_success: datetime = None) -> None:
        """Log an alert to the appropriate monthly tab."""
        try:
            # Get current month tab
            tab_name = self.get_or_create_monthly_tab()
            
            # Format timestamp in AEST
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Format last success if provided
            last_success_str = ""
            if last_success:
                last_success_str = last_success.strftime("%Y-%m-%d %H:%M:%S")
            
            # Prepare row data
            row_data = [
                timestamp,
                alert_type,
                page_url,
                str(status_code),
                description,
                screenshot_link,
                html_link,
                last_success_str
            ]
            
            # Append to sheet
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id,
                range=f"{tab_name}!A:H",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': [row_data]}
            ).execute()
            
            print(f"Logged {alert_type} alert to sheet: {page_url}")
            
        except Exception as e:
            print(f"Error logging alert to sheets: {e}")
            # Don't raise - we want crawler to continue even if sheets logging fails

    def log_new_page_alert(self, page_url: str, screenshot_link: str, html_link: str) -> None:
        """Log a new page discovery alert."""
        self.log_alert(
            alert_type="New Page",
            page_url=page_url,
            status_code=200,
            description="New page discovered",
            screenshot_link=screenshot_link,
            html_link=html_link
        )

    def log_changed_page_alert(self, page_url: str, changes_description: str, 
                              screenshot_link: str, html_link: str) -> None:
        """Log a page change alert."""
        self.log_alert(
            alert_type="Changed Page",
            page_url=page_url,
            status_code=200,
            description=changes_description,
            screenshot_link=screenshot_link,
            html_link=html_link
        )

    def log_deleted_page_alert(self, page_url: str, status_code: int, 
                              last_success: datetime = None) -> None:
        """Log a deleted page alert."""
        description = f"Page returned {status_code} - previously accessible"
        self.log_alert(
            alert_type="Deleted Page",
            page_url=page_url,
            status_code=status_code,
            description=description,
            last_success=last_success
        )

    def get_spreadsheet_url(self) -> str:
        """Get the URL of the alerts spreadsheet."""
        return self.spreadsheet_url 