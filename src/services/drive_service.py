from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload # type: ignore

import io
import os
import json
# Environment variables are loaded in main.py and configes.py
# No need to duplicate load_dotenv() here
import mimetypes
from datetime import datetime, timedelta
from typing import Optional, Tuple, Any, List
from src.config import SCOPES

__all__ = ['DriveService']

class DriveService:
    def __init__(self):
        self.service = self._initialize_service()

    def _initialize_service(self):
        """Initialize and return the Google Drive service."""
        try:
            # Get service account info from environment variables
            service_account_info = {
                "type": os.getenv("TYPE"),
                "project_id": os.getenv("PROJECT_ID"),
                "private_key_id": os.getenv("PRIVATE_KEY_ID"),
                "private_key": os.getenv("PRIVATE_KEY"),
                "client_email": os.getenv("CLIENT_EMAIL"), 
                "client_id": os.getenv("CLIENT_ID"),
                "auth_uri": os.getenv("AUTH_URI"),
                "token_uri": os.getenv("TOKEN_URI"),
                "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_x509_CERT_URL"),
                "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),  
            }

            credentials = service_account.Credentials.from_service_account_info(
                service_account_info, scopes=SCOPES
            )

            service =  build('drive', 'v3', credentials=credentials)


            service.files().list(pageSize=1).execute()


           

            print("\nGoogle Drive service initialized successfully")
            return service
        except Exception as e:
            print(f"\nError initializing Drive service: {e}")
            return None
        

        
    def upload_file(self, file_path: str, folder_id: str) -> Optional[str]:
        """Upload (or update) a file to Google Drive and return its ID.

        Fixes:
        - Avoid reading files as UTF-8 (binary like PNG causes decode errors)
        - Detect empty files via size, not content
        - Update existing file with same name to avoid duplicates/quota growth
        - Handle Drive quota errors explicitly
        - Enhanced validation to prevent blank file uploads
        """
        try:
            # Enhanced file validation
            if not os.path.exists(file_path):
                print(f"❌ Skipped upload, file not found: {file_path}")
                return None
                
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                print(f"❌ Skipped uploading empty file: {file_path}")
                return None
                
            # Additional validation: check if file has meaningful content
            if file_path.endswith('.html'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read(1000)  # Read first 1000 chars
                        if len(content.strip()) < 100:  # Too short to be meaningful
                            print(f"❌ Skipped uploading file with insufficient content: {file_path}")
                            return None
                        
                        # Additional HTML validation
                        if "<html" not in content.lower() and "<!doctype" not in content.lower():
                            print(f"❌ Skipped uploading non-HTML file: {file_path}")
                            return None
                        
                        # Check for common error page indicators
                        error_indicators = ["error", "not found", "404", "500", "access denied", "forbidden"]
                        if any(indicator in content.lower() for indicator in error_indicators):
                            print(f"⚠️  File appears to be an error page: {file_path}")
                            # Still upload but log the warning
                        
                except Exception as e:
                    print(f"❌ Error reading file content: {file_path} - {e}")
                    return None
            
            print(f"📤 Uploading file: {file_path} ({file_size} bytes)")
            
            # Guess MIME type, default to binary stream
            mt = mimetypes.guess_type(file_path)
            mime_type = mt[0] or 'application/octet-stream'

            file_name = os.path.basename(file_path)
            media = MediaFileUpload(file_path, mimetype=mime_type, resumable=False)

            # If a file with the same name exists in the folder, update it to avoid duplicates
            existing_id = self.find_file(file_name, folder_id)
            if existing_id:
                print(f"🔄 Updating existing file: {file_name}")
                updated = self.service.files().update(
                    fileId=existing_id,
                    media_body=media,
                    fields='id'
                ).execute()
                print(f"✅ File updated successfully: {file_name}")
                return updated.get('id')

            # Otherwise create a new file
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            uploaded = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"✅ File uploaded successfully: {file_name}")
            return uploaded.get('id')
        except HttpError as he:
            # Surface quota issues clearly
            if 'storageQuotaExceeded' in str(he):
                print(f"\n❌ Error uploading file: Drive storage quota exceeded. Consider cleanup or using update strategy.")
            else:
                print(f"\n❌ Error uploading file: {he}")
            return None
        except Exception as e:
            print(f"\n❌ Error uploading file: {e}")
            return None



    def find_file(self, file_name: str, folder_id: str) -> Optional[str]:
        """Find a file in a specific folder and return its ID."""
        try:
            query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
            result = self.service.files().list(q=query, fields="files(id, name)").execute()
            files = result.get('files', [])
            return files[0]['id'] if files else None
        except Exception as e:
            print(f"\nError finding file: {e}")
            return None

    def download_file(self, file_id: str, destination_path: str) -> bool:
        """Download a file from Google Drive."""
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.FileIO(destination_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            return True
        except Exception as e:
            print(f"\nError downloading file: {e}")
            return False

    def get_or_create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> Tuple[Optional[str], str]:
        """Get or create a folder in Google Drive."""
        try:
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            if parent_id:
                query += f" and '{parent_id}' in parents"

            response = self.service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
            files = response.get('files', [])

            if files:
                folder_id = files[0]['id']
                return folder_id, 'already_exist'
            else:
                metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                }
                if parent_id:
                    metadata['parents'] = [parent_id]

                folder = self.service.files().create(body=metadata, fields='id').execute()
                folder_id = folder.get('id')
                return folder_id, 'new'

        except Exception as e:
            print(f"\nError in get_or_create_folder: {e}")
            return None, 'error'

    def rename_file(self, file_id: str, new_name: str) -> Optional[Any]:
        """Rename a file in Google Drive."""
        try:
            file_metadata = {'name': new_name}
            return self.service.files().update(
                fileId=file_id,
                body=file_metadata,
                fields='id, name'
            ).execute()
        except Exception as e:
            print(f"\nError renaming file: {e}")
            return None

    def delete_file(self, file_id: str) -> bool:
        """Delete a file from Google Drive."""
        try:
            self.service.files().delete(fileId=file_id).execute()
            return True
        except Exception as e:
            print(f"\nError deleting file: {e}")
            return False

    def delete_folder(self, folder_id: str) -> bool:
        """Delete a folder from Google Drive."""
        try:
            # Check if folder has any contents first
            response = self.service.files().list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="files(id)"
            ).execute()
            
            # If folder has contents, delete them first
            for file in response.get('files', []):
                self.delete_file(file['id'])
            
            # Now delete the empty folder
            self.service.files().delete(fileId=folder_id).execute()
            return True
        except Exception as e:
            print(f"\nError deleting folder: {e}")
            return False
    

    def delete_files_older_than(self, folder_id: str, file_name: str, days: int = 90) -> None:
        """Delete files in a folder that are older than specified days."""
        try:
            # Calculate the cutoff date
            cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
            
            # Query for files matching the name and older than cutoff date
            query = f"'{folder_id}' in parents and name = '{file_name}' and trashed = false and createdTime < '{cutoff_date}'"
            response = self.service.files().list(
                q=query,
                fields="files(id, name, createdTime)",
            ).execute()

            files = response.get('files', [])
            
            # Delete old files
            for file in files:
                self.delete_file(file['id'])

        except Exception as e:
            print(f"Error deleting old files: {e}")

    def delete_older_duplicates(self, folder_id: str, file_name: str) -> None:
        """Delete files older than 3 months in a folder."""
        self.delete_files_older_than(folder_id, file_name) 
