import os
from dotenv import load_dotenv

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# Load .env
load_dotenv()

# CONFIG
OAUTH_FILE = os.getenv("OAUTH_CLIENT_FILE")
TOKEN_FILE = os.getenv("OAUTH_TOKEN_FILE")
FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
FILE_PATH = os.getenv("LOCAL_FILE_PATH")

SCOPES = ['https://www.googleapis.com/auth/drive.file']



# AUTH HANDLER (AUTO REFRESH)
def get_credentials():
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        print("Refreshing token...")
        creds.refresh(Request())

    elif not creds or not creds.valid:
        print("Login required...")
        flow = InstalledAppFlow.from_client_secrets_file(OAUTH_FILE, SCOPES)
        creds = flow.run_local_server(port=0)

    with open(TOKEN_FILE, "w") as token:
        token.write(creds.to_json())

    return creds


# FIND FILE (IDEMPOTENT)
def find_file(service, filename, folder_id):
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"

    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)'
    ).execute()

    files = results.get('files', [])
    return files[0] if files else None


# MAIN UPLOAD FUNCTION
def upload_to_drive():
    print("Starting upload process...")
    print(f"Source file: {FILE_PATH}")

    if not os.path.exists(OAUTH_FILE):
        raise FileNotFoundError("oauth-client.json tidak ditemukan")

    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError("file tidak ditemukan")

    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    filename = os.path.basename(FILE_PATH)

    media = MediaFileUpload(FILE_PATH, resumable=True)

    existing_file = find_file(service, filename, FOLDER_ID)

    if existing_file:
        print("File sudah ada → UPDATE (overwrite)")

        file = service.files().update(
            fileId=existing_file['id'],
            media_body=media
        ).execute()

    else:
        print("File belum ada → CREATE")

        file_metadata = {
            "name": filename,
            "parents": [FOLDER_ID]
        }

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, name"
        ).execute()

    print("upload sukses!")
    print(f"File name : {filename}")
    print(f"File ID   : {file.get('id')}")
    print(f"Link      : https://drive.google.com/file/d/{file.get('id')}/view")


# ENTRYPOINT
if __name__ == "__main__":
    upload_to_drive()