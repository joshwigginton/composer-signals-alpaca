# Just something I created to test the service account authentication with Google Drive API.
# Needs the service account file to be present in the same directory. You can get it from the Google Cloud Console.

from google.oauth2 import service_account
from googleapiclient.discovery import build

def list_drive_files(service_account_file):
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=['https://www.googleapis.com/auth/drive.metadata.readonly'])
    service = build('drive', 'v3', credentials=credentials)
    # Call the Drive v3 API
    results = service.files().list(
        pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))

if __name__ == '__main__':
    service_account_file = 'alpaca-trading-python-123aBcD456.json' # Update with actual path
    list_drive_files(service_account_file)
