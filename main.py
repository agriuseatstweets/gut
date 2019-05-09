import os
import pytest
import gcsfs
import gspread
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
GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID', 'toixotoixo')
BELLY_LOCATION = os.getenv('BELLY_LOCATION', 'agrius-tweethouse')
OUTPUT_LOCATION = os.getenv('GUT_LOCATION', 'agrius-outputs')
SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME', 'Agrius_search_criteria')

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
    'engagement-counts-by-day': get_engagement_by_day,
    'network': count_edges,
}

def all_users(user_groups):
    return [x for y in list(user_groups.values()) for x in y]

def make_filename(metric, group):
    timestamp = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    outfi = 'gs://' + join(OUTPUT_LOCATION, metric, group, timestamp)
    return outfi

def process(metric, group, limit=None):
    fs = gcsfs.GCSFileSystem(project=GOOGLE_PROJECT_ID, token=GOOGLE_APPLICATION_CREDENTIALS, access='read_write')
    fps = sorted(fs.ls(BELLY_LOCATION))

    if limit:
        fps = fps[:int(limit)]

    tz = 'Asia/Kolkata'

    user_groups = get_user_groups()
    if metric == 'network':
        users = user_groups[group]
    else:
        users = all_users(user_groups)

    logging.info(f'Processing {len(fps)} files and {len(users)} users with metric: {metric}')

    if metric == 'follower-counts':
        df = follower_count(user_groups)

    elif metric == 'engagement-counts':
        path = sorted(fs.ls(join(OUTPUT_LOCATION, 'engagement-counts-by-day')))[-1]
        df = get_engagement(path, fs)

    else:
        tweets = load_tweets(fps, tz, fs)
        fn = metrics[metric]
        df = fn(users, tweets)

    # join groups?

    with fs.open(make_filename(metric, group), 'w') as f:
        df.to_csv(f, index=False)

from clize import run

if __name__ == '__main__':
    run(process)
