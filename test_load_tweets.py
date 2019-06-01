import pytest
import pandas as pd
from load_tweets import *
from utils import *


@pytest.fixture
def tweet_fp():
    return 'test_data/tweets.txt'


@pytest.fixture
def test_dir():
    return 'test_data/'

@pytest.fixture
def ext():
    return '.txt'


@pytest.fixture
def tweet_fps(test_dir, ext):
    return list(get_abs_fps(test_dir, ext))


@pytest.fixture
def target_tweet_df(test_dir):
    return pd.read_pickle(test_dir + 'df1.pickle')

@pytest.fixture
def attrs():
    attrs = (
        'id_str',
        'bla',
        'user.screen_name',
        'retweeted',
        'entities.user_mentions.,screen_name'
    )
    return attrs


@pytest.fixture(autouse=True)
def with_envs(monkeypatch):
    def patch_os(attr):
        d = {'GUT_START_DATE': '2017-8-21',
             'GUT_END_DATE': '2017-8-22'}
        return d[attr]

    monkeypatch.setattr(os, 'getenv',  patch_os)

import time

class FS():
    def open(self, *args):
        return open(*args)

def test_get_tweet_attrs(target_tweet_df, tweet_fps, attrs):
    tweets = list(load_tweets_from_fps(FS(), tweet_fps))
    targets = target_tweet_df.to_dict(orient='records')
    for t,targ in zip(tweets, targets):
        a = get_tweet_attrs(t, attrs)
        assert(a['id_str'] == targ['id_str'])

def test_set_timezone(tweet_fps):
    tweets = list(load_tweets_from_fps(FS(), tweet_fps))
    tweets = [get_tweet_attrs(t, ['created_at']) for t in tweets]
    t = tweets[0].copy()
    out = set_timezone(tweets[0], 'Asia/Kolkata')
    assert(out['created_at'].strftime('%a %b %d %H:%M') == 'Mon Aug 21 03:30')

def test_load_tweets_filters_from_dates(tweet_fps):
    tweets = get_tweets(tweet_fps, FS(), 'Asia/Kolkata')
    assert(len(list(tweets)) == 1)
