import json
import pandas as pd
from utils import safe_get
from datetime import datetime, date
import pytz
from diskcache import Cache
import logging
from concurrent.futures import ThreadPoolExecutor
from toolz import curry
from itertools import takewhile, islice, count

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

@curry
def load_tweets_from_fp(fs, fp):
    opener = fs.open
    with opener(fp, 'r') as f:
        tweets = [json.loads(line) for line in f.readlines()]
    return tweets

def parallel_load_batch(fps, fs=None):
    with ThreadPoolExecutor(len(fps)) as pool:
        res = pool.map(load_tweets_from_fp(fs), fps)
    return [y for x in res for y in x]

def load_tweets_from_fps(fps, fs=None):
    N = len(fps)
    logging.info(f'Loading {N} files.')
    fps = chunk(5, fps)
    for i,fp in enumerate(fps):
        logging.info(f'Loading file: {i*5}/{N}')
        for t in parallel_load_batch(fp, fs):
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

def load_tweet_attrs(tweet_fps, attrs, fs=None):
    tweets = load_tweets_from_fps(tweet_fps, fs)
    return (get_tweet_attrs(t, attrs) for t in tweets)

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
    end_time = datetime(2019, 5, 23, tzinfo=pytz.timezone(tz))
    return (t for t in tweets if t['created_at'] >= start_time and t['created_at'] <= end_time)

def filter_repeats(tweets):
    # move to disk if this blows up
    seen_ids = set()

    for tweet in tweets:
        if tweet['id_str'] not in seen_ids:
            seen_ids.add(tweet['id_str'])
            yield tweet


def _is_in_range(fp):
    date = fp.split('/')[1].split('T')[0]
    dt = datetime.strptime(date, "%Y-%m-%d")
    return dt >= datetime(2019, 4, 10) and dt <= datetime(2019, 5, 23)

def filter_start_time(fps):
    return [fp for fp in fps if _is_in_range(fp)]

# TODO: is this necessary? Some way to filter out non-useful tweets?
def filter_nonsense(hashtags, users, tweets):
    pass

def load_tweets(tweet_fps, tz, fs=None):
    '''
    wrapper for tweet loading

    :param list tweet_fps: list of tweet filepaths
    :param str tz: timezone from datetime library
    :param gcsfs.GCSFileSystem fs: access to google cloud storeage

    :returns pd.DataFrame df: dataframe with extracted attributes
    '''

    tweet_fps = filter_start_time(tweet_fps)
    tweets = load_tweets_from_fps(tweet_fps, fs)
    tweets = (get_tweet_attrs(t, tweet_attrs()) for t in tweets)
    tweets = filter_tweets(tweets)
    tweets = (set_timezone(t, tz) for t in tweets)
    tweets = filter_tweet_time_range(tweets, tz)
    tweets = filter_repeats(tweets)
    return tweets
