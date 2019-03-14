import numpy as np
import os
import pytest
import gcsfs
import gspread
import datetime
from oauth2client.service_account import ServiceAccountCredentials
from chunkwise_processing import *


# get environment variables
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/usr/share/keys/key.json')
GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID', 'toixotoixo')
BELLY_LOCATION = os.getenv('BELLY_LOCATION', 'agrius-tweethouse')
OUTPUT_LOCATION = os.getenv('OUTPUT_LOCATION', 'agrius-outputs')
SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME', 'Agrius_search_criteria')


# read in user groups from spreadsheet
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scope)
client = gspread.authorize(creds)
sheet = client.open(SPREADSHEET_NAME).sheet1
user_groups = pd.DataFrame({
    'screen_name': strip_list(sheet.col_values(2)[1:]),
    'user_group': strip_list(sheet.col_values(3)[1:])
})\
    .groupby('user_group')['screen_name']\
    .apply(list).to_dict()


# access GCS
fs = gcsfs.GCSFileSystem(project=GOOGLE_PROJECT_ID, token=GOOGLE_APPLICATION_CREDENTIALS, access='read_write')
fps = fs.ls(BELLY_LOCATION)


# shoten for testing
#user_groups['media_outlet'] = user_groups['media_outlet'][:20]
#user_groups['party'] = user_groups['party'][:20]
#fps = fps[:50]

# let the fun begin ...
tz = 'Asia/Kolkata'
chunksize = 100 # number of files from bucket to be processed in one chunk
now = datetime.datetime.now()
timestamp = '-'.join(str(x) for x in getattrs(now, ['year', 'month', 'day', 'hour', 'minute']))
output_pt = 'gs://' + OUTPUT_LOCATION + '/' + timestamp + '/'
chunkwise_processing(user_groups, fps, tz, output_pt, chunksize=chunksize, fs=fs)
