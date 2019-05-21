import pandas as pd
from utils import safe_get
from datetime import datetime, date
import pytz
from diskcache import Cache
import logging
from concurrent.futures import ThreadPoolExecutor
from toolz import curry
from db import RedisDB
from itertools import takewhile, islice, count
import redis
import json
import orjson
import os
import gcsfs
from tqdm import tqdm
from joblib import Parallel, delayed
from math import ceil

logging.basicConfig(level = logging.INFO)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/usr/share/keys/key.json')
GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID', 'toixotoixo')
BELLY_LOCATION = os.getenv('BELLY_LOCATION', 'agrius-tweethouse')
CHUNK_SIZE = int(os.getenv('GUT_CHUNK_SIZE', '200'))

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def load_tweets_from_fp(fs, fp):
    opener = fs.open
    with opener(fp, 'r') as f:
        tweets = [json.loads(line) for line in f.readlines()]
    return tweets

def load_tweets_from_fps(fs, fps):
    N = len(fps)
    logging.info(f'Loading {N} files.')
    for i,fp in enumerate(fps):
        logging.info(f'Loading file: {i*5}/{N}')
        for t in load_tweets_from_fp(fs, fp):
            yield t

def get_tweet_attrs(t, attrs):
    data = {}
    for a in attrs:
        keys = a.split('.')
        if ',' in ''.join(keys):
            if ',' in keys[-1]:
                _, key_emb = keys[-1].split(',')
                vs = safe_get(t, *keys[:-1])
                data[a] = list({x[key_emb] for x in vs}) if vs else [{}]
            else:
                raise NotImplementedError('Only implemented for comma on final nesting level.')
        else:
            data[a] = safe_get(t, *keys)
    return data


def tweet_attrs():
    '''t tweet attributes'''
    attrs = ['created_at', 'id_str',
             'user.screen_name', 'user.followers_count', 'user.friends_count',
             'in_reply_to_status_id_str', 'in_reply_to_screen_name',
             'retweeted_status.id_str', 'retweeted_status.user.screen_name',
             'quoted_status.id_str',
             'entities.user_mentions.,screen_name']
    return attrs


def set_timezone(tweet, tz):
    d = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
    tweet['created_at'] = d.astimezone(pytz.timezone(tz))
    return tweet

def filter_tweets(tweets):
    return (t for t in tweets if t.get('created_at'))

def filter_tweet_time_range(tweets, tz):
    start_time = datetime(2019, 4, 12, tzinfo=pytz.timezone(tz))
    end_time = datetime.strptime(os.getenv('GUT_END_DATE'), '%Y-%m-%d')
    end_time = end_time.replace(tzinfo = pytz.timezone(tz))
    return (t for t in tweets if t['created_at'] >= start_time and t['created_at'] <= end_time)

def _is_in_range(fp):
    date = fp.split('/')[1].split('T')[0]
    dt = datetime.strptime(date, "%Y-%m-%d")
    return dt >= datetime(2019, 4, 10) and dt <= datetime(2019, 5, 24)

def filter_start_time(fps):
    return [fp for fp in fps if _is_in_range(fp)]

def load_tweets(fps, fs, tz):
    '''
    wrapper for tweet loading

    :param list tweet_fps: list of tweet filepaths
    :param str tz: timezone from datetime library
    :param gcsfs.GCSFileSystem fs: access to google cloud storeage

    :returns pd.DataFrame df: dataframe with extracted attributes
    '''

    db = RedisDB(os.getenv('REDIS_HOST'), int(os.getenv('REDIS_PORT')))

    tweets = load_tweets_from_fps(fs, fps)
    tweets = (get_tweet_attrs(t, tweet_attrs()) for t in tweets)
    tweets = filter_tweets(tweets)
    tweets = (set_timezone(t, tz) for t in tweets)
    tweets = filter_tweet_time_range(tweets, tz)
    tweets = list(tweets)
    db.load(tweets)




def process(limit=None):
    fs = gcsfs.GCSFileSystem(project=GOOGLE_PROJECT_ID, token=GOOGLE_APPLICATION_CREDENTIALS, access='read_write')
    fps = sorted(fs.ls(BELLY_LOCATION))

    if limit:
        fps = fps[-int(limit):]

    logging.info(f'Processing {len(fps)} files')

    tz = 'Asia/Kolkata'
    fps = filter_start_time(fps)

    Parallel(n_jobs=-1)(delayed(load_tweets)(list(f), fs, tz)
                        for f in tqdm(chunk(CHUNK_SIZE, fps), total=ceil(len(fps)/CHUNK_SIZE)))

    logging.info(f'Loaded all tweets')


from clize import run
if __name__ == '__main__':
    run(process)
