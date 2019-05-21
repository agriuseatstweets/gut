import os
import pytest
import gcsfs
import gspread
import datetime as dt
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import strip_list
from load_tweets import *
from get_descriptives import *
from os.path import join
import logging

logging.basicConfig(level = logging.INFO)

# get environment variables
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/usr/share/keys/key.json')
OUTPUT_LOCATION = os.getenv('GUT_LOCATION', 'agrius-outputs')
SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME', 'Agrius_search_criteria')
GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID', 'toixotoixo')

# read in user groups from spreadsheet
def get_user_groups():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scope)
    client = gspread.authorize(creds)
    sheet = client.open(SPREADSHEET_NAME).sheet1

    user_groups = pd.DataFrame({
        'screen_name': strip_list(sheet.col_values(2)[1:]),
        'user_group': strip_list(sheet.col_values(4)[1:])
    })\
                    .groupby('user_group')['screen_name']\
                    .apply(list).to_dict()

    return user_groups

metrics = {
    'engagement-counts': get_engagement_by_day,
    'network': count_edges,
}

def all_users(user_groups):
    return [x for y in list(user_groups.values()) for x in y]

def make_filename(metric, group):
    timestamp = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    outfi = 'gs://' + join(OUTPUT_LOCATION, metric, group, timestamp)
    return outfi

def write_df(fs, fi, df):
    with fs.open(fi, 'w') as f:
        df.to_csv(f, index=False)

def process(metric, group):
    fs = gcsfs.GCSFileSystem(project=GOOGLE_PROJECT_ID, token=GOOGLE_APPLICATION_CREDENTIALS, access='read_write')

    tz = 'Asia/Kolkata'

    user_groups = get_user_groups()

    if metric == 'network':
        users = user_groups[group]
    else:
        users = all_users(user_groups)

    if metric == 'follower-counts':
        df = follower_count(user_groups)
    else:
        db = RedisDB(os.getenv('REDIS_HOST'), int(os.getenv('REDIS_PORT')))
        tweets = db.get_tweets(tz)
        fn = metrics[metric]
        df = fn(users, tweets)

    if metric == 'engagement-counts':
        write_df(fs, make_filename('engagement-counts-by-day', group), df)

        write_df(fs, make_filename('engagement-counts', group),
                 df.groupby(['user.screen_name'], as_index=False).sum())
    else:
        write_df(fs, make_filename(metric, group), df)


from clize import run
if __name__ == '__main__':
    run(process)
