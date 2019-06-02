import pandas as pd
import os
from oauth2client.service_account import ServiceAccountCredentials
from utils import strip_list
from googleapiclient.discovery import build

def _sheets_client():
    scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        scope
    )

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    sheet = service.spreadsheets()

    return sheet

def _get_column(sheets_client, rng):
    vals = (sheets_client
            .values()
            .get(spreadsheetId=os.getenv('GUT_SHEET_ID'),
                 range=rng)
            .execute())['values']

    return [x.strip() for y in vals for x in y]


def get_keywords(sheets_client):
    vals = _get_column(sheets_client, 'follows!G2:G')
    return [v.replace('#', '') for v in vals]

def get_additional_terms(sheets_client):
    return _get_column(sheets_client, 'Parties_Candidates_Names_Acronyms!B2:E')

def get_user_groups(sheets_client):
    res = zip(_get_column(sheets_client, 'follows!D2:D'),
              _get_column(sheets_client, 'follows!B2:B'))

    d = {}
    for k,v in res:
        try:
            d[k].append(v)
        except KeyError:
            d[k] = [v]
    return d
