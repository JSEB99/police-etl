import os
import csv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def authenticate_drive(scope):
    """Authenticate and return the Google Drive service."""
    creds = None
    CREDENTIALS_PATH = "credentials.json"
    TOKEN_PATH = "token_upload.json"

    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, scope)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, scope)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())

    try:
        service = build("drive", "v3", credentials=creds)
        return service
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None


def upload_file(service, file_path, folder_id):
    """Upload a file to a specific folder in Google Drive."""
    name_file = os.path.basename(file_path)
    file_metadata = {
        'name': name_file,
        'parents': [folder_id]
    }
    media = MediaFileUpload(
        file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' if file_path.endswith('.xlsx') else 'application/vnd.ms-excel')

    try:
        file = service.files().create(body=file_metadata,
                                      media_body=media, fields='id').execute()
        print(f'File ID: {file.get("id")}, File Name: {name_file}')
    except HttpError as error:
        print(f"An error occurred: {error}")


def read_folder_ids_from_csv(csv_file_path):
    """Read folder IDs from a CSV file and return a dictionary mapping folder names to IDs."""
    folder_ids = {}
    with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)  # Skip header
        for row in csv_reader:
            folder_name, folder_id = row
            folder_ids[folder_name] = folder_id
    return folder_ids


def upload_files_to_drive(local_directory, folder_ids, service):
    """Upload all xlsx and xls files from local directories to the respective folders in Google Drive."""
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            if file.endswith('.xlsx') or file.endswith('.xls'):
                local_folder_name = os.path.basename(root)
                if local_folder_name in folder_ids and local_folder_name == "Violencia_Intrafamiliar":
                    folder_id = folder_ids[local_folder_name]
                    file_path = os.path.join(root, file)
                    upload_file(service, file_path, folder_id)


def main():
    # Ruta de la carpeta local
    local_directory = 'Data'

    # Ruta del archivo CSV que contiene los nombres de las carpetas y los IDs
    csv_file_path = 'parents_id.csv'

    # Leer los IDs de las carpetas desde el archivo CSV
    folder_ids = read_folder_ids_from_csv(csv_file_path)

    # Autenticar y obtener el servicio de Google Drive
    service = authenticate_drive(SCOPES)

    if service:
        # Subir todos los archivos xlsx y xls en los directorios locales a las carpetas correspondientes en Google Drive
        upload_files_to_drive(local_directory, folder_ids, service)


if __name__ == "__main__":
    main()
