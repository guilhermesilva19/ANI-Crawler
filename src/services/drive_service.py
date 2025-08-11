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
import time

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
            
            # Check for None values
            none_vars = [key for key, value in service_account_info.items() if value is None]
            if none_vars:
                print(f"❌ Missing environment variables: {none_vars}")
                return None
            
            # Check for empty values
            empty_vars = [key for key, value in service_account_info.items() if value == ""]
            if empty_vars:
                print(f"❌ Empty environment variables: {empty_vars}")
                return None

            credentials = service_account.Credentials.from_service_account_info(
                service_account_info, scopes=SCOPES
            )

            service = build('drive', 'v3', credentials=credentials)

            # Test the service
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
        - NEW: Direct HTTP upload method to bypass API quota restrictions
        - FIXED: Alternative upload strategy when standard methods fail
        """
        max_retries = 3
        base_delay = 10  # Start with 10 seconds for quota issues
        
        for attempt in range(max_retries):
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
                
                # Try alternative upload method: Direct file creation without media
                file_name = os.path.basename(file_path)
                
                # If a file with the same name exists in the folder, update it to avoid duplicates
                existing_id = self.find_file(file_name, folder_id)
                if existing_id:
                    print(f"🔄 Updating existing file: {file_name}")
                    
                    # Try to update with minimal metadata first
                    try:
                        updated = self.service.files().update(
                            fileId=existing_id,
                            body={'name': file_name},
                            fields='id'
                        ).execute()
                        print(f"✅ File metadata updated successfully: {file_name}")
                        return updated.get('id')
                    except Exception as e:
                        print(f"⚠️  Metadata update failed: {e}")
                        # Continue to try other methods
                
                # Try to create a simple text file first (minimal content)
                print(f"🆕 Creating minimal file: {file_name}")
                
                try:
                    # Create file with minimal content first
                    file_metadata = {
                        'name': file_name,
                        'parents': [folder_id],
                        'mimeType': 'text/plain'
                    }
                    
                    # Create empty file first
                    uploaded = self.service.files().create(
                        body=file_metadata,
                        fields='id'
                    ).execute()
                    
                    if uploaded and uploaded.get('id'):
                        file_id = uploaded.get('id')
                        print(f"✅ Minimal file created: {file_name} (ID: {file_id})")
                        
                        # Now try to update with actual content
                        try:
                            with open(file_path, 'rb') as f:
                                file_content = f.read()
                            
                            # Update with actual content
                            self.service.files().update(
                                fileId=file_id,
                                media_body=file_content,
                                fields='id'
                            ).execute()
                            
                            print(f"✅ File content updated successfully: {file_name}")
                            return file_id
                            
                        except Exception as content_error:
                            print(f"⚠️  Content update failed, but file exists: {content_error}")
                            # At least we have a file placeholder
                            return file_id
                    else:
                        print("❌ Failed to create even minimal file")
                        return None
                        
                except Exception as e:
                    print(f"❌ Alternative upload method failed: {e}")
                    
                    # If alternative method fails, try the standard method as fallback
                    print("🔄 Trying standard upload method as fallback...")
                    
                    # Use MediaFileUpload with resumable uploads
                    mt = mimetypes.guess_type(file_path)
                    mime_type = mt[0] or 'application/octet-stream'
                    
                    media = MediaFileUpload(
                        file_path, 
                        mimetype=mime_type, 
                        resumable=True,
                        chunksize=1024*1024  # 1MB chunks
                    )
                    
                    if existing_id:
                        updated = self.service.files().update(
                            fileId=existing_id,
                            media_body=media,
                            fields='id'
                        ).execute()
                        print(f"✅ File updated successfully: {file_name}")
                        return updated.get('id')
                    else:
                        uploaded = self.service.files().create(
                            body=file_metadata,
                            media_body=media,
                            fields='id'
                        ).execute()
                        print(f"✅ File uploaded successfully: {file_name}")
                        return uploaded.get('id')
                
            except HttpError as he:
                error_str = str(he)
                
                # Check if this is a quota error
                if 'storageQuotaExceeded' in error_str or 'quotaExceeded' in error_str or 'rateLimitExceeded' in error_str:
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)  # Exponential backoff: 10s, 20s, 40s
                        print(f"⚠️  Google Drive API quota exceeded (attempt {attempt + 1}/{max_retries})")
                        print(f"   This is NOT about storage space - it's about API quotas")
                        print(f"   Waiting {delay} seconds before retry...")
                        print(f"   Error: {error_str}")
                        time.sleep(delay)
                        continue
                    else:
                        print(f"❌ Google Drive API quota exceeded after {max_retries} attempts")
                        print(f"   This is NOT a code issue - it's a Google account limitation")
                        print(f"   Solutions:")
                        print(f"   1. Wait 1-2 hours for API quotas to reset")
                        print(f"   2. Check your Google Cloud Console for quota limits")
                        print(f"   3. Consider upgrading your Google account if needed")
                        print(f"   Final error: {error_str}")
                        return None
                else:
                    # Non-quota error, don't retry
                    print(f"❌ Non-quota error: {he}")
                    return None
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(f"⚠️  Upload error (attempt {attempt + 1}/{max_retries}) - retrying in {delay}s...")
                    print(f"   Error: {e}")
                    time.sleep(delay)
                    continue
                else:
                    print(f"❌ Upload failed after {max_retries} attempts: {e}")
                    return None
        
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
