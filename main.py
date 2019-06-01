import os
import gcsfs
from datetime import datetime
from utils import strip_list
from descriptives.get_descriptives import *
from os.path import join
from sheets.sheets import get_user_groups, _sheets_client
from db import RedisDB
import logging

logging.basicConfig(level = logging.INFO)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
OUTPUT_LOCATION = os.getenv('GUT_LOCATION')
GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID')
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))

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
    fs = gcsfs.GCSFileSystem(project=GOOGLE_PROJECT_ID,
                             token=GOOGLE_APPLICATION_CREDENTIALS,
                             access='read_write')

    tz = 'Asia/Kolkata'

    user_groups = get_user_groups(_sheets_client())

    if metric == 'network':
        users = user_groups[group]
    else:
        users = all_users(user_groups)

    if metric == 'follower-counts':
        df = follower_count(user_groups)
    else:
        db = RedisDB(REDIS_HOST, REDIS_PORT)
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
