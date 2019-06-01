import pandas as pd
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import strip_list

SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME', 'Agrius_search_criteria')
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')



def _sheets_client():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_APPLICATION_CREDENTIALS,
        scope
    )

    return gspread.authorize(creds)

def get_keywords():
    client = _sheets_client()
    sheet = client.open(SPREADSHEET_NAME).worksheet('follows')

    return [s.replace('#', '').strip() for s in sheet.col_values(7)[1:]]


def get_additional_terms():
    client = _sheets_client()
    sheet = client.open(SPREADSHEET_NAME).worksheet(
        'Parties_Candidates_Names_Acronyms'
    )
    return [y.strip() for x in [2,3,4,5]
            for y in sheet.col_values(x)[1:]
            if y.strip()]


def get_user_groups():
    client = _sheets_client()
    sheet = client.open(SPREADSHEET_NAME).worksheet('follows')

    user_groups = pd.DataFrame({
        'screen_name': strip_list(sheet.col_values(2)[1:]),
        'user_group': strip_list(sheet.col_values(4)[1:])
    })\
                    .groupby('user_group')['screen_name']\
                    .apply(list).to_dict()

    return user_groups
