
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import pandas as pd
import dlt
import logging

@dlt.resource(name="file-tracking", columns={"modified_time": {"data_type": "timestamp", "dedup_sort": "desc"}}, primary_key="id",
    write_disposition="merge")
def get_fileIds(sheet_name: str, creds: service_account.Credentials, logger: logging.Logger):
  """Search file in drive location

  Load pre-authorized user credentials from the environment.
  TODO(developer) - See https://developers.google.com/identity
  for guides on implementing OAuth2 for the application.
  """
  drive = build('drive', 'v3', credentials=creds)
  try:
    # First, get the folder ID by querying by mimeType and name
    folderId = drive.files().list(q = f"mimeType = 'application/vnd.google-apps.folder' and name = '{sheet_name}'", pageSize=10, fields="nextPageToken, files(id, name)").execute()
    # this gives us a list of all folders with that name
    folderIdResult = folderId.get('files', [])
    # We just get the id of the 1st item in the list
    id = folderIdResult[0].get('id')
    # Now, using the folder ID gotten above, we get all the files from
    # that particular folder
    results = drive.files().list(q = "'" + id + "' in parents", pageSize=10, fields="nextPageToken, files(id, name, modifiedTime)").execute()
    items = results.get('files', [])
    # Now we can loop through each file in that folder, and do whatever (in this case, download them and open them as images in OpenCV)
    data = []
    for f in range(0, len(items)):
        record = {'id' : items[f].get('id'),
        'name': items[f].get('name'),
        'modified_time': items[f].get('modifiedTime')
        }
        data.append(record)
    yield data
  except HttpError as error:
    print(f"An error occurred: {error}")
    logger.error(error)


@dlt.resource(name="load-google-sheet",write_disposition='replace')
def read_gsheet(spreadsheet_id: str, named_range: str, creds: service_account.Credentials, logger: logging.Logger):
  """
  Creates the batch_update the user has access to.
  Load pre-authorized user credentials from the environment.
  TODO(developer) - See https://developers.google.com/identity
  for guides on implementing OAuth2 for the application.
  """
  # pylint: disable=maybe-no-member
  try:
    service = build("sheets", "v4", credentials=creds)
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=named_range)
        .execute()
    )
    rows = result.get("values", [])
    df = pd.DataFrame(rows[1:], columns=rows[0]).to_dict(orient='records')
    yield df
  except HttpError as error:
    print(f"An error occurred: {error}")
    logger.error(error)