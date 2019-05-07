import pytest
import pandas as pd
from load_tweets import *
from utils import *


@pytest.fixture
def target_tweets():
    return load_pickle('test_data/tweets.pickle')


@pytest.fixture
def tweet_fp():
    return 'test_data/tweets.txt'

def test_load_tweets_from_fp(target_tweets, tweet_fp):
    loaded_tweets = load_tweets_from_fp(tweet_fp)
    for t1, t2 in zip(loaded_tweets, target_tweets):
        assert t1 == t2


@pytest.fixture
def test_dir():
    return 'test_data/'

@pytest.fixture
def ext():
    return '.txt'


@pytest.fixture
def tweet_fps(test_dir, ext):
    return list(get_abs_fps(test_dir, ext))


def test_load_tweets_from_fps(target_tweets, tweet_fps):
    tweet_files = load_tweets_from_fps(tweet_fps)
    for t in tweet_files:
        assert(type(t) == dict)
        assert(type(t['created_at']) == str)


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

def test_get_tweet_attrs(target_tweet_df, tweet_fps, attrs):
    tweets = list(load_tweets_from_fps(tweet_fps))
    targets = target_tweet_df.to_dict(orient='records')
    for t,targ in zip(tweets, targets):
        a = get_tweet_attrs(t, attrs)
        assert(a['id_str'] == targ['id_str'])

import time

def test_set_timezone(tweet_fps):
    tweets = list(load_tweets_from_fps(tweet_fps))
    tweets = [get_tweet_attrs(t, ['created_at']) for t in tweets]
    t = tweets[0].copy()
    out = set_timezone(tweets[0], 'Asia/Kolkata')
    assert(out['created_at'].strftime('%a %b %d %H:%M') == 'Mon Aug 21 01:30')


def test_load_tweets_attrs_notimplemented_exeption(tweet_fps):
    with pytest.raises(NotImplementedError):
        list(load_tweet_attrs(tweet_fps, ['entities,.user_mentions.bla']))

def test_filter_repeats():
    tweets = [{'id_str': 'foo'}, {'id_str': 'bar'}, {'id_str': 'baz'}, {'id_str': 'foo'}]
    tweets = filter_repeats(tweets)
    tweets = list(tweets)
    assert(len(tweets) == 3)
    assert(tweets == [{'id_str': 'foo'}, {'id_str': 'bar'}, {'id_str': 'baz'}])
