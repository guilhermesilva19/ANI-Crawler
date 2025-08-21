"""Google Drive service for file storage and management."""

import os
import mimetypes
from datetime import datetime, timedelta
from typing import Optional, Tuple, Any, List, Dict

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from src.config import (
    SCOPES, 
    GOOGLE_DRIVE_CREDENTIALS_FILE, 
    GOOGLE_DRIVE_TOKEN_FILE, 
    GOOGLE_DRIVE_ROOT_FOLDER_ID
)

__all__ = ['DriveService']


class DriveService:
    """Google Drive service for file storage and management."""
    
    def __init__(self, root_folder_id: Optional[str] = None):
        """Initialize Google Drive service."""
        self.root_folder_id = root_folder_id or GOOGLE_DRIVE_ROOT_FOLDER_ID
        if not self.root_folder_id:
            raise ValueError("GOOGLE_DRIVE_ROOT_FOLDER_ID must be set in environment variables")
        
        self.service = self._authenticate()
        print(f"✅ Google Drive service initialized with root folder: {self.root_folder_id}")

    def get_credentials_with_refresh_token(self):
        """Get credentials using the refresh token."""
        creds = None
        if os.path.exists(GOOGLE_DRIVE_TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(GOOGLE_DRIVE_TOKEN_FILE, SCOPES)
            
            if creds and creds.expired and creds.refresh_token:
                print("🔄 Refreshing the access token using the refresh token...")
                creds.refresh(Request())
                
                with open(GOOGLE_DRIVE_TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                print("✅ Access token refreshed and saved!")
            
            elif creds and creds.valid:
                print("🔐 Using valid token")
                return creds

        if not creds or not creds.valid:
            print("❌ No valid credentials found.")
            return None
        return creds  
    def _authenticate(self):
        """Authenticate with Google Drive API using service account or OAuth 2.0."""
        # First, try service account authentication (from .env file)
        try:
            service_account_info = {
                "type": os.getenv('TYPE'),
                "project_id": os.getenv('PROJECT_ID'),
                "private_key_id": os.getenv('PRIVATE_KEY_ID'),
                "private_key": os.getenv('PRIVATE_KEY'),
                "client_email": os.getenv('CLIENT_EMAIL'),
                "client_id": os.getenv('CLIENT_ID'),
                "auth_uri": os.getenv('AUTH_URI'),
                "token_uri": os.getenv('TOKEN_URI'),
                "auth_provider_x509_cert_url": os.getenv('AUTH_PROVIDER_x509_CERT_URL'),
                "client_secret": os.getenv("CLIENT_SECRET")
            }
            creds = self.get_credentials_with_refresh_token()
            if creds and creds.valid:
                print("🔐 Using existing refresh_token")
                return build('drive', 'v3', credentials=creds)
            
            # Check if all required service account fields are present
            if all(service_account_info.values()):
                print("🔐 Using service account authentication")
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info, scopes=SCOPES
                )
                return build('drive', 'v3', credentials=credentials)
            else:
                print("⚠️ Service account credentials incomplete, trying OAuth 2.0...")
        except Exception as e:
            print(f"⚠️  Service account authentication failed: {e}")
            print("🔄 Falling back to OAuth 2.0...")

        # Check if token file exists and is valid
        creds = None
        if os.path.exists(GOOGLE_DRIVE_TOKEN_FILE):
            try:
                creds = Credentials.from_authorized_user_file(GOOGLE_DRIVE_TOKEN_FILE, SCOPES)
                
                if creds and creds.valid:
                    print("🔐 Using existing valid token")
                    return build('drive', 'v3', credentials=creds)
                elif creds and creds.expired and creds.refresh_token:
                    # If expired, try refreshing the token
                    creds.refresh(Request())
                    # Save the refreshed token to the file
                    with open(GOOGLE_DRIVE_TOKEN_FILE, 'w') as token:
                        token.write(creds.to_json())
                    print("✅ Refreshed and saved token")
                    return build('drive', 'v3', credentials=creds)
            except Exception as e:
                print(f"⚠️ Error loading or refreshing token: {e}")
                creds = None

        # If no valid credentials, get new ones
        if not creds:
            print("🔄 No valid token found, authenticating via OAuth 2.0...")
            flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_DRIVE_CREDENTIALS_FILE, SCOPES)
            flow.access_type = 'offline'
            flow.include_granted_scopes = 'true'
            creds = flow.run_local_server(port=8002)

            # Save the credentials to a file for future use
            with open(GOOGLE_DRIVE_TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
            print("✅ OAuth credentials saved to token file")
        refresh_token = creds.refresh_token
        print(f"Refresh Token: {refresh_token}")
        return build('drive', 'v3', credentials=creds)


    def upload_file(self, file_path: str, folder_id: str) -> Optional[str]:
        """Upload a file to Google Drive and return the file ID."""
        try:
            # Validate source file exists and non-empty
            if not os.path.isfile(file_path):
                print(f"❌ File not found: {file_path}")
                return None
            
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                print(f"⚠️  Skipping empty file: {file_path}")
                return None
            
            # Get file metadata
            file_name = os.path.basename(file_path)
            mime_type, _ = mimetypes.guess_type(file_path)
            if not mime_type:
                mime_type = 'application/octet-stream'
            
            # Add timestamp to filename to avoid conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name_without_ext, ext = os.path.splitext(file_name)
            safe_filename = f"{name_without_ext}_{timestamp}{ext}"
            
            # Prepare file metadata
            file_metadata = {
                'name': safe_filename,
                'parents': [folder_id]
            }
            
            # Create media upload
            media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
            
            # Upload file
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size,webViewLink'
            ).execute()
            
            file_id = file.get('id')
            file_size_uploaded = int(file.get('size', 0))
            
            print(f"✅ File uploaded to Google Drive: {safe_filename}")
            print(f"   📁 File ID: {file_id}")
            print(f"   📏 Size: {file_size_uploaded} bytes")
            print(f"   🔗 View: {file.get('webViewLink')}")
            
            return file_id
            
        except HttpError as e:
            print(f"❌ Google Drive API error: {e}")
            return None
        except Exception as e:
            print(f"❌ Error uploading file: {e}")
            return None

    def find_file(self, file_name: str, folder_id: str) -> Optional[str]:
        """Find a file in a specific folder and return its file ID."""
        try:
            # Search for files with similar names (ignoring timestamp)
            name_without_ext, ext = os.path.splitext(file_name)
            query = f"'{folder_id}' in parents and name contains '{name_without_ext}' and name ends with '{ext}'"
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id,name,size,modifiedTime)',
                orderBy='modifiedTime desc'
            ).execute()
            
            files = results.get('files', [])
            if files:
                # Return the most recently modified file
                return files[0]['id']
            
            return None
            
        except HttpError as e:
            return None
        except Exception as e:
            return None

    def get_or_create_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> Tuple[Optional[str], str]:
        """Get or create a folder in Google Drive."""
        try:
            parent_id = parent_folder_id or self.root_folder_id
            
            # Check if folder already exists
            query = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id,name)'
            ).execute()
            
            existing_folders = results.get('files', [])
            if existing_folders:
                folder_id = existing_folders[0]['id']
                return folder_id, 'already_exist'
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            
            folder = self.service.files().create(
                body=folder_metadata,
                fields='id,name,webViewLink'
            ).execute()
            
            folder_id = folder.get('id')
            print(f"✅ Created new folder: {folder_name}")
            print(f"   📁 Folder ID: {folder_id}")
            print(f"   🔗 View: {folder.get('webViewLink')}")
            
            return folder_id, 'new'
            
        except HttpError as e:
            return None, 'error'
        except Exception as e:
            print(f"❌ Error creating folder: {e}")
            return None, 'error'

    def rename_file(self, file_id: str, new_name: str) -> Optional[str]:
        """Rename a file in Google Drive."""
        try:
            file_metadata = {'name': new_name}
            
            file = self.service.files().update(
                fileId=file_id,
                body=file_metadata,
                fields='id,name'
            ).execute()
            
            print(f"✅ File renamed: {new_name}")
            return file.get('id')
            
        except HttpError as e:
            print(f"❌ Google Drive API error renaming file: {e}")
            return None
        except Exception as e:
            print(f"❌ Error renaming file: {e}")
            return None

    def delete_file(self, file_id: str) -> bool:
        """Delete a file from Google Drive."""
        try:
            self.service.files().delete(fileId=file_id).execute()
            print(f"✅ File deleted from Google Drive: {file_id}")
            return True
            
        except HttpError as e:
            print(f"❌ Google Drive API error deleting file: {e}")
            return False
        except Exception as e:
            print(f"❌ Error deleting file: {e}")
            return False

    def delete_folder(self, folder_id: str) -> bool:
        """Delete a folder from Google Drive."""
        try:
            self.service.files().delete(fileId=folder_id).execute()
            print(f"✅ Folder deleted from Google Drive: {folder_id}")
            return True
            
        except HttpError as e:
            print(f"❌ Google Drive API error deleting folder: {e}")
            return False
        except Exception as e:
            print(f"❌ Error deleting folder: {e}")
            return False

    def upload_file_with_verification(self, file_path: str, folder_id: str) -> Optional[str]:
        """Upload file with verification that it was uploaded correctly."""
        try:
            # Upload the file
            file_id = self.upload_file(file_path, folder_id)
            if not file_id:
                return None
            
            # Verify the file was uploaded correctly
            file_info = self.get_file_info(file_id)
            if not file_info:
                print(f"❌ Verification failed: could not get file info for {file_id}")
                return None
            
            original_size = os.path.getsize(file_path)
            uploaded_size = file_info['size']
            
            if original_size != uploaded_size:
                print(f"❌ Verification failed: size mismatch. Original: {original_size}, Uploaded: {uploaded_size}")
                # Delete the corrupted upload
                self.delete_file(file_id)
                return None
            
            print(f"✅ File verification passed: {file_info['name']} ({uploaded_size} bytes)")
            return file_id
            
        except Exception as e:
            print(f"❌ Error in upload_file_with_verification: {e}")
            return None

    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a file."""
        try:
            file = self.service.files().get(
                fileId=file_id,
                fields='id,name,size,mimeType,modifiedTime,createdTime,webViewLink'
            ).execute()
            
            return {
                'id': file['id'],
                'name': file['name'],
                'size': int(file.get('size', 0)),
                'mime_type': file.get('mimeType'),
                'modified_time': file.get('modifiedTime'),
                'created_time': file.get('createdTime'),
                'web_view_link': file.get('webViewLink')
            }
            
        except HttpError as e:
            print(f"❌ Google Drive API error getting file info: {e}")
            return None
        except Exception as e:
            print(f"❌ Error getting file info: {e}")
            return None

    def get_folder_url(self, folder_id: str) -> str:
        """Get the web view URL for a folder."""
        return f"https://drive.google.com/drive/folders/{folder_id}"

    def get_file_url(self, file_id: str) -> str:
        """Get the web view URL for a file."""
        return f"https://drive.google.com/file/d/{file_id}/view"

    def list_files(self, folder_id: str) -> List[Dict[str, Any]]:
        """List all files in a folder."""
        try:
            results = self.service.files().list(
                q=f"'{folder_id}' in parents",
                spaces='drive',
                fields='files(id,name,size,mimeType,modifiedTime,webViewLink)',
                orderBy='modifiedTime desc'
            ).execute()
            
            files = results.get('files', [])
            return [
                {
                    'id': file['id'],
                    'name': file['name'],
                    'size': int(file.get('size', 0)),
                    'mime_type': file.get('mimeType'),
                    'modified_time': file.get('modifiedTime'),
                    'web_view_link': file.get('webViewLink')
                }
                for file in files
            ]
            
        except HttpError as e:
            print(f"❌ Google Drive API error listing files: {e}")
            return []
        except Exception as e:
            print(f"❌ Error listing files: {e}")
            return []

    def download_file(self, file_id: str, destination_path: str) -> bool:
        """Download a file from Google Drive to local storage."""
        try:
            # Get file metadata
            file = self.service.files().get(fileId=file_id).execute()
            file_name = file.get('name', 'unknown_file')
            
            # Download file content
            request = self.service.files().get_media(fileId=file_id)
            file_content = request.execute()
            
            # Write to local file
            with open(destination_path, 'wb') as f:
                f.write(file_content)
            
            print(f"✅ File downloaded: {destination_path}")
            return True
            
        except HttpError as e:
            print(f"❌ Google Drive API error downloading file: {e}")
            return False
        except Exception as e:
            print(f"❌ Error downloading file: {e}")
            return False

    def cleanup_old_files(self, days: int = 30) -> int:
        """Clean up files older than specified days."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            cutoff_date_str = cutoff_date.isoformat() + 'Z'
            
            # Find old files in root folder and subfolders
            query = f"'{self.root_folder_id}' in parents and modifiedTime < '{cutoff_date_str}'"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id,name,modifiedTime)'
            ).execute()
            
            old_files = results.get('files', [])
            deleted_count = 0
            
            for file in old_files:
                try:
                    self.service.files().delete(fileId=file['id']).execute()
                    deleted_count += 1
                    print(f"🗑️  Cleaned up old file: {file['name']}")
                except Exception as e:
                    print(f"⚠️  Error deleting old file {file['name']}: {e}")
            
            print(f"🧹 Cleanup completed: {deleted_count} files deleted")
            return deleted_count
            
        except HttpError as e:
            print(f"❌ Google Drive API error during cleanup: {e}")
            return 0
        except Exception as e:
            print(f"❌ Error during cleanup: {e}")
            return 0

    def debug_upload_issue(self, file_path: str):
        """Debug upload issues by checking file properties."""
        try:
            print(f"🔍 Debugging file: {file_path}")
            
            if not os.path.exists(file_path):
                print(f"❌ File does not exist: {file_path}")
                return
            
            file_size = os.path.getsize(file_path)
            print(f"📏 File size: {file_size} bytes")
            
            if file_size == 0:
                print(f"⚠️  File is empty: {file_path}")
                return
            
            mime_type, _ = mimetypes.guess_type(file_path)
            print(f"📄 MIME type: {mime_type}")
            
            # Check if file is readable
            try:
                with open(file_path, 'rb') as f:
                    first_bytes = f.read(100)
                    print(f"📖 First 100 bytes: {first_bytes[:50]}...")
            except Exception as e:
                print(f"❌ Error reading file: {e}")
                
        except Exception as e:
            print(f"❌ Error in debug_upload_issue: {e}") 