import json
import pandas as pd
from tqdm import tqdm
from utils import safe_get
from datetime import datetime, date
import pytz

def load_tweets_from_fp(fp, fs=None):
    opener = fs.open if fs else open
    with opener(fp, 'r') as f:
        tweets = [json.loads(line) for line in f.readlines()]

    return tweets


def load_tweets_from_fps(fps, fs=None):
    return (t for fp in fps for t in load_tweets_from_fp(fp, fs))

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
    """
    loads all files with extension ext in directory dir_p
    and extracts attributes into pd.Dataframe

    :param list tweet_fps: list of tweet filepaths
    :param list attrs: list of str attributes to be extracted from tweet object
                       e.g. 'id_str' or 'user.screen_name' for nested values
                       or 'user.entities.,screen_name' if last nesting layer is list-like
    :param str ext: extension of files, e.g. '.txt'
    :param gcsfs.GCSFileSystem fs: access to google cloud storeage
    :returns pd.DataFrame df: dataframe with extracted attributes
    """
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
    '''preprocessing for tweet_dataframe'''
    d = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    tweet['created_at'] = d.astimezone(pytz.timezone(tz))
    return tweet

def filter_tweets(tweets):
    """ Removes limit tweets and includes only real tweets"""
    return (t for t in tweets if t.get('created_at'))


def filter_repeats(tweets):
    # move to disk if this blows up
    seen_ids = set()

    for tweet in tweets:
        if tweet['id_str'] not in seen_ids:
            seen_ids.add(tweet['id_str'])
            yield tweet


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

    tweets = load_tweets_from_fps(tweet_fps, fs)
    tweets = (get_tweet_attrs(t, tweet_attrs()) for t in tweets)
    tweets = filter_tweets(tweets)
    tweets = filter_repeats(tweets)
    return (set_timezone(t, tz) for t in tweets)
