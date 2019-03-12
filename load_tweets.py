import json
import pandas as pd
from tqdm import tqdm
from utils import *


def load_tweets_from_fp(fp, fs=None):
    if fs:
        with fs.open(fp, 'r') as f:
            tweets = (json.loads(line) for line in f.readlines())
    else:
        with open(fp, 'r') as f:
            tweets = (json.loads(line) for line in f.readlines())
    return tweets


def load_tweets_from_fps(fps, fs=None):
    return (load_tweets_from_fp(fp, fs) for fp in fps)


def load_tweets2df(tweet_fps, attrs, fs=None):
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
    tweets_files = load_tweets_from_fps(tweet_fps, fs)
    data = {a: [] for a in attrs}
    for f in tqdm(tweets_files, total=len(tweet_fps)):
        for t in f:
            for a in attrs:
                keys = a.split('.')
                if ',' in ''.join(keys):
                    if ',' in keys[-1]:
                        _, key_emb = keys[-1].split(',')
                        vs = safe_get(t, *keys[:-1])
                        data[a] += [{x[key_emb] for x in vs}] if vs else [{}]
                    else:
                        raise NotImplementedError('Only implemented for comma on final nesting level.')
                else:
                    data[a] += [safe_get(t, *keys)]
    df = pd.DataFrame(data)
    return df


def tweet_attrs():
    '''t tweet attributes'''
    attrs = ['created_at', 'id_str',
             'user.screen_name', 'user.followers_count', 'user.friends_count',
             'in_reply_to_status_id_str', 'in_reply_to_screen_name',
             'retweeted_status.id_str', 'retweeted_status.user.screen_name',
             'quoted_status.id_str',
             'entities.user_mentions.,screen_name']
    return attrs


def preproc_tweet_df(tweet_df, tz):
    '''preprocessing for tweet_dataframe'''
    tweet_df['created_at'] = pd.to_datetime(tweet_df['created_at'], utc=True).dt.tz_convert(tz)
    tweet_df['created_at_D'] = tweet_df['created_at'].dt.floor('D')
    tweet_df = tweet_df.drop(tweet_df[tweet_df['id_str'].isnull()].index)
    return tweet_df


def load_tweets(tweet_fps, tz, fs=None):
    '''
    wrapper for tweet loading

    :param list tweet_fps: list of tweet filepaths
    :param str tz: timezone from datetime library
    :param str ext: extension of files, e.g. '.txt'
    :param tweet_attrs: list of str attributes to be extracted from tweet object
                       e.g. 'id_str' or 'user.screen_name' for nested values
                       or 'user.entities.,screen_name' if last nesting layer is list-like
    :param gcsfs.GCSFileSystem fs: access to google cloud storeage
    :returns pd.DataFrame df: dataframe with extracted attributes
    '''
    print('... Loading tweets ...')
    tweet_df = load_tweets2df(tweet_fps, tweet_attrs(), fs)
    tweet_df = preproc_tweet_df(tweet_df, tz)
    print('... Loading tweet completed.')
    return tweet_df
