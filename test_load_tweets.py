import pytest
import pandas as pmd
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
    for f in tweet_files:
        for t1, t2 in zip(f, target_tweets):
            assert t1 == t2


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


def test_load_tweets2df(target_tweet_df, tweet_fps, attrs):
    loaded_tweet_df = load_tweets2df(tweet_fps, attrs)
    assert loaded_tweet_df.equals(target_tweet_df)




def test_load_tweets2df_notimplemented_exeption(tweet_fps):
    with pytest.raises(NotImplementedError):
        df = load_tweets2df(tweet_fps, ['entities,.user_mentions.bla'])
