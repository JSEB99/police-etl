import os
import io
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
READ_SCOPE = ["https://www.googleapis.com/auth/drive.metadata.readonly"]


def authenticate_drive(scope, token_name):
    """Authenticate and return the Google Drive service."""
    creds = None
    CREDENTIALS_PATH = "credentials.json"
    TOKEN_PATH = f"{token_name}.json"

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


def list_files_in_folder(service, folder_id):
    """List all files in a specific Google Drive folder."""
    results = service.files().list(
        q=f"'{folder_id}' in parents",
        pageSize=1000,
        fields="nextPageToken, files(id, name)"
    ).execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
        return []
    else:
        return items


def download_items_in_folder(items, service):

    for item in items:
        try:
            print(f'Descargando {item["name"]} ...')
            file = io.BytesIO()
            request = service.files().get_media(fileId=item['id'])

            downloader = MediaIoBaseDownload(file, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()
                print('Download progress {0}'.format(status.progress()*100))

            with open(os.path.join('./Silver', item['name']), 'wb') as download_file:
                download_file.write(file.read())
                download_file.close()
        except HttpError as error:
            print(
                f"An error occurred while downloading {item['name']} ({item['id']}): {error}")


def main():

    # ID de la carpeta de Google Drive de la que deseas descargar los archivos
    folder_id = '1o4uGVdzJwM5VmobCji6V59IzsfLdw5Ah'
    # Autenticar y obtener el servicio de Google Drive
    service = authenticate_drive(READ_SCOPE, 'token_read')

    if service:

        items = list_files_in_folder(service, folder_id)
        service_download = authenticate_drive(SCOPES, 'token_download')

        if service_download:
            download_items_in_folder(items, service_download)


if __name__ == "__main__":
    main()
