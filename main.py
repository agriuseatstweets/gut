import numpy as np
import os
import pytest
import gcsfs
import gspread
import datetime
from oauth2client.service_account import ServiceAccountCredentials
from chunkwise_processing import *


# some testing
pytest.main()

# get user from google spreadsheet
spreadsheet_token_fp = 'key_inputsheet.json'
spreadsheet_name = 'Agrius_search_criteria'


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(spreadsheet_token_fp, scope)
client = gspread.authorize(creds)
sheet = client.open(spreadsheet_name).sheet1
all_user = {
    'media_outlets': strip_list(np.unique(sheet.col_values(1)[1:])),
    'parties': strip_list(np.unique(sheet.col_values(2)[1:]))
}

# access tweet bucket
project = 'toixotoixo'
bucket_token_fp = 'key.json'
bucket = 'agrius-tweethouse-test'
output_dir = 'descriptives'
tz = 'Asia/Kolkata'
chunksize = 100 # number of files from bucket to be processed in one chunk


if output_dir[-1] != '!': output_dir += '/'
now = datetime.datetime.now()
output_dir += '/' + str(now.year) + str(now.month) + str(now.day) + '_' + str(now.hour) + str(now.minute)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# let the fun begin ...
fs = gcsfs.GCSFileSystem(project=project, token=bucket_token_fp, access='read_only')
fps = fs.ls(bucket)
chunkwise_processing(all_user, fps[:5], tz, output_dir, chunksize=chunksize, fs=fs)



#fps = list(get_abs_fps('sample_data', '.txt'))
#chunkwise_processing(all_user, fps, tz, output_dir, chunksize=chunksize)