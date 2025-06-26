from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload # type: ignore

import io
import os
import json
from dotenv import load_dotenv
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
            load_dotenv()
            
            print(os.getenv("PRIVATE_KEY"))
            # Get service account info from environment variables
            service_account_info = {
                "type": os.getenv("TYPE"),
                "project_id": os.getenv("PROJECT_ID"),
                "private_key_id": os.getenv("PRIVATE_KEY_ID"),
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCEypwFBVXqcLHs\nB9GOvMeVIvouQ2xuA3Gt4i4uXMZntdKv/qYJcM3vK1rcnL6Z72B9+LOPnepIOMrG\netYO3Bw500dSyNWCfWe6SIEhHpaX4muBNoyip+JAQozNCQ86/mZ+iOItFDn+jM8R\nrXm/AsQzao5EUbWo7GdhBDhvYiqwmXoSkYC6tO0vfwQSi3V56JTXAl8vscvPGd6w\nnVK3K1ZDpPHWWHJQnPgrnrGmA5Y9yj8sdyuRyC1zixC1vdXnIN3fxt0X6KyrTdEV\nwQooclzhN2XE12kVweMfmu03S+vp9mVBaBHOBI/CmoLaDl2QA8cdnsFdgl0DOIiZ\nFts3qhiTAgMBAAECggEAAcAcxv4qg0Qhoq+TTH/iW6EFGwrQr5fEOsGo0GI9shlJ\nLs1Uoy3SMSBpR4MAPWduTLE0elffdXAP0DnHTGI/ZwDveqcvruz4EV8Qs+Vc09S5\nJdz+62THv6DnWgtc0+8nKuzWZzfrvPzEyZvEgPpiDrfaKnFifr8OOWh37y2KmLhW\nvh90p3vdVvxKxYqdrsLkwlF6pVrem7UxJFMTFmEMcH9g/fy7UpMKB7Eci/02taYB\nMTu0SoAPAm3AGaAIpqcBEX9VO5D8PX3q5OsaXO/dI8711uRXkzHNF7w1sfGrJnWK\nZ3C4FAuXiELwiQZbhTjC2ugshs2+CMconPDJ0KUD+QKBgQC7n4aFmiz7isQpgX84\necWKUFodc+sOvUtzbT12ypnpFevzmqiT7VK6x/KX9yGh54bXHp8YOIZbOW4oNpbl\n9BkMwDvcPrnzLWAeT4jGOqaisiRyNtE0oQ/XwkCv1TRlLLGkixyb0b2xZFefPzBe\nxnMBuwD0jr2id1ejLhmWJ4yY+wKBgQC1L4J33tvMaNUAuVo5sDk68WJc7TMeGEvA\nitCb/Vv+GafWrt0V+kgicZZlwEJhfJqxgNJCpEX3ie04Vdz68oKrXseQeuKBo+48\nYSwAtZLtdRE37rFYepHldAVkL8GQAZroMGBJk3k4yt/x/M572FQHtqWsKKAIHqc0\nCN+1ilAbSQKBgBL7zhmgguKkpLpqXtvw/jrDbIrEWskv//1a1Tg6wZ3i2O/Z+yqU\nfaVma7ZSi/WcvnjvoTtLJNUAF5Q8CcKUkyBGJwhe1GO1eJ84fudHseqOSb/AqiRA\nE6FKB0+DK/z8q7CxXqCpdR8IvxDezN8aBzLqn+sesjNJDfX1ySL3N3OXAoGACGhA\n9JD5v/1pTNRM9HpR3YNP3iH/ijj0YuXOEj7qYtsL57FLfvaTHE4K/YOzUNS4VN42\nY3M97+ONh1ysmVRhbdRDDFg3ZG6X4mNb6FiEjdItz1ubDRD67hu5GFIC+EyqxHVi\nMmfOay02wQLLDgIWvBjkjR2Bd378oTR8NO9LEkkCgYBScgeSfWi6seS6s67ryiQ7\n/Cn9Z6REs+s17M/Gm9WPv7ueTThcN+IFUrLYrXlW7vauEgob6xHQu3dsGKOtyO32\nNEhxf+PkyRJeN9Uq2Zxsb7V8JzVeY0UPYyItH54PI2Ffa5yul2Ty0GAMQqOCOtb+\nG1er+4j9PKtRks5F0v+abA==\n-----END PRIVATE KEY-----\n",
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
        """Upload a file to Google Drive and return its ID."""
        try:
            mt = mimetypes.guess_type(file_path)
            mime_type = mt[0]

            file_metadata = {
                'name': os.path.basename(file_path),
                'parents': [folder_id]
            }

            media = MediaFileUpload(file_path, mimetype=mime_type)
            uploaded = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            return uploaded.get('id')
        except Exception as e:
            print(f"\nError uploading file: {e}")
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