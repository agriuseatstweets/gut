import json
import pandas as pd
from utils import *


def load_tweets(fp):
    with open(fp, 'r') as f:
        tweets = (json.loads(line) for line in f.readlines())
    return tweets


def load_tweets_from_dir(dir_p, ext=None):
    fps = get_abs_fps(dir_p, ext=ext)
    tweet_files = (load_tweets(fp) for fp in fps)
    return tweet_files


#def load_tweets2df(dir_p, attrs, ext=None):
#    """
#    loads all files with extension ext in directory dir_p
#    and extracts attributes into pd.Dataframe
#
#    :parameters:
#    dir_p: str
#        directory with tweets
#    attrs: list of str
#        attributes to be extracted from tweet object,
#        e.g. 'id_str' or 'user.screen_name' for nested values
#
#    :returns:
#    pd.Dataframe with the extracted attributes
#    """
#    tweets_files = load_tweets_from_dir(dir_p, ext)
#    data = {a: [] for a in attrs}
#    for f in tweets_files:
#        for t in f:
#            for a in attrs:
#                keys = a.split('.')
#            data[a] += [safe_get(t, *keys)]
#    df = pd.DataFrame(data)
#    return df


#df = pd.read_json("test_data/tweets.txt", orient = 'records', lines = True)
#df1 = load_tweets2df("test_data", ["id_str"], ext=".txt")
