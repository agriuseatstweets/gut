from utils import safe_get
from datetime import datetime, date, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor
from toolz import curry
from db import RedisDB
import json
import os
from dateutil import parser
from preprocessing.collect_entities import collect_entities, collect_text
from preprocessing.filter_tweets import only_tweets_of_interest
from sheets.sheets import _sheets_client
import logging

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
            if t['created_at'] >= start_time and t['created_at'] < end_time)

def _is_in_range(tz, fp):
    date = fp.split('/')[1]
    dt = parser.parse(date)
    dt = dt.astimezone(pytz.timezone(tz))

    st = get_start_time(tz)
    et = get_end_time(tz) + timedelta(hours = 3)

    return dt >= st and dt <= et


def filter_start_time(fps, tz):
    return [fp for fp in fps if _is_in_range(tz, fp)]


def counter(pre, it):
    i = 0
    for x in it:
        i += 1
        yield x
    logging.info(f'{pre}: {i}')

def get_tweets(fps, fs, tz):
    tweets = load_tweets_from_fps(fs, fps)

    tweets = filter_tweets(tweets)

    # collect entities and text
    tweets = (collect_text(t) for t in tweets)
    tweets = (collect_entities(t) for t in tweets)

    tweets = (get_tweet_attrs(t, tweet_attrs()) for t in tweets)
    tweets = (set_timezone(t, tz) for t in tweets)
    tweets = filter_tweet_time_range(tweets, tz)
    tweets = counter('PRE FILTERING', tweets)

    # filter non election tweets
    sheets = _sheets_client()
    tweets = only_tweets_of_interest(tweets, sheets)

    tweets = counter('POST FILTERING', tweets)
    return tweets
