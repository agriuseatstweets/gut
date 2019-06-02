import pytest
import pandas as pd
from .loading import *
from utils import *

@pytest.fixture
def tweet_fps():
    return list(get_abs_fps('test_data', '.txt'))

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

class FS():
    def open(self, *args):
        return open(*args)

def test_get_tweet_attrs(tweet_fps, attrs):
    tweets = list(load_tweets_from_fps(FS(), tweet_fps))
    targets = [{'id_str': '899390762688950273',
                'bla': None,
                'user.screen_name': 'tweecix',
                'retweeted': False,
                'entities.user_mentions.,screen_name': {'heutejournal'}},
               {'id_str': '899390778606321664',
                'bla': None,
                'user.screen_name': 'BGedanke',
                'retweeted': False,
                'entities.user_mentions.,screen_name': {'CSU'}}]

    for t,targ in zip(tweets, targets):
        a = get_tweet_attrs(t, attrs)
        assert(a['id_str'] == targ['id_str'])

def test_set_timezone(tweet_fps):
    tweets = list(load_tweets_from_fps(FS(), tweet_fps))
    tweets = [get_tweet_attrs(t, ['created_at']) for t in tweets]
    t = tweets[0].copy()
    out = set_timezone(tweets[0], 'Asia/Kolkata')
    assert(out['created_at'].strftime('%a %b %d %H:%M') == 'Mon Aug 21 03:30')

def test_filter_tweet_time_range(tweet_fps):
    tz = 'Asia/Kolkata'
    tweets = load_tweets_from_fps(FS(), tweet_fps)
    tweets = (set_timezone(t, tz) for t in tweets)
    tweets = list(filter_tweet_time_range(tweets, tz))
    assert(len(tweets) == 1)
