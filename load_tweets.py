import pandas as pd
from utils import safe_get
from datetime import datetime, date, timedelta
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
from dateutil import parser
from collect_entities import collect_entities, collect_text
from filter_tweets import only_tweets_of_interest


logging.basicConfig(level = logging.INFO)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/usr/share/keys/key.json')
GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID', 'toixotoixo')
BELLY_LOCATION = os.getenv('BELLY_LOCATION', 'agrius-tweethouse')
CHUNK_SIZE = int(os.getenv('GUT_CHUNK_SIZE', '200'))

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def load_tweets_from_fp(fs, fp):
    with fs.open(fp, 'r') as f:
        tweets = [json.loads(line) for line in f.readlines()]
    return tweets

def load_tweets_from_fps(fs, fps):
    N = len(fps)
    logging.info(f'Loading {N} files.')
    for i,fp in enumerate(fps):
        logging.info(f'Loading file: {i}/{N}')
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
             'collected_text',
             'user.screen_name', 'user.followers_count', 'user.friends_count',
             'in_reply_to_status_id_str', 'in_reply_to_screen_name',
             'retweeted_status.id_str', 'retweeted_status.user.screen_name',
             'quoted_status.id_str',
             'quoted_status.user.screen_name',
             'entities.user_mentions.,screen_name']
    return attrs


def set_timezone(tweet, tz):
    d = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
    tweet['created_at'] = d.astimezone(pytz.timezone(tz))
    return tweet

def filter_tweets(tweets):
    return (t for t in tweets if t.get('created_at'))

def _get_time(key, tz):
    time = datetime.strptime(os.getenv(key), '%Y-%m-%d')
    return pytz.timezone(tz).localize(time)

def get_end_time(tz):
    return _get_time('GUT_END_DATE', tz)

def get_start_time(tz):
    return _get_time('GUT_START_DATE', tz)

def filter_tweet_time_range(tweets, tz):
    end_time, start_time = get_end_time(tz), get_start_time(tz)
    return (t for t in tweets
            if t['created_at'] >= start_time and t['created_at'] <= end_time)


def _is_in_range(tz, fp):
    date = fp.split('/')[1]
    dt = parser.parse(date)
    dt = dt.astimezone(pytz.timezone(tz))

    st = get_start_time(tz)
    et = get_end_time(tz) + timedelta(hours = 12)

    return dt >= st and dt <= et


def filter_start_time(fps, tz):
    return [fp for fp in fps if _is_in_range(tz, fp)]


def get_relevant_terms():
    pass

def get_users():
    pass


def get_tweets(fps, fs, tz):
    tweets = load_tweets_from_fps(fs, fps)

    tweets = filter_tweets(tweets)

    # collect entities and text
    tweets = (collect_text(t) for t in tweets)
    tweets = (collect_entities(t) for t in tweets)

    tweets = (get_tweet_attrs(t, tweet_attrs()) for t in tweets)
    tweets = (set_timezone(t, tz) for t in tweets)
    tweets = filter_tweet_time_range(tweets, tz)

    # filter non election tweets
    tweets = only_tweets_of_interest(tweets)

    return tweets


def load_tweets(fps, fs, tz):
    db = RedisDB(os.getenv('REDIS_HOST'), int(os.getenv('REDIS_PORT')))
    tweets = get_tweets(fps, fs, tz)



    db.load(tweets)


def process(limit=None):
    fs = gcsfs.GCSFileSystem(project=GOOGLE_PROJECT_ID, token=GOOGLE_APPLICATION_CREDENTIALS, access='read_write')
    fps = sorted(fs.ls(BELLY_LOCATION))

    tz = 'Asia/Kolkata'
    fps = filter_start_time(fps, tz)

    if limit:
        fps = fps[-int(limit):]

    logging.info(f'Processing {len(fps)} files')

    Parallel(n_jobs=-1)(delayed(load_tweets)(list(f), fs, tz)
                        for f in tqdm(chunk(CHUNK_SIZE, fps), total=ceil(len(fps)/CHUNK_SIZE)))

    logging.info(f'Loaded all tweets')


from clize import run
if __name__ == '__main__':
    run(process)
