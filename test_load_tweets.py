import pytest
import pandas as pd
from load_tweets import *
from utils import *

@pytest.fixture
def test_fp():
    return 'test_data/tweets.txt'

@pytest.fixture
def target_fp():
    return 'test_data/target_tweets.pickle'


def test_load_tweets(test_fp, target_fp):
    tweets = load_tweets(test_fp)
    target_tweets = load_pickle(target_fp)
    for t, tar_t in zip(tweets, target_tweets):
        assert t == t

@pytest.fixture
def test_dir_p():
    return 'test_data'

@pytest.fixture
def ext():
    return '.txt'

def test_load_tweets_from_dir(test_dir_p, ext, target_fp):
    tweets_files = load_tweets_from_dir(test_dir_p, ext)
    target_tweets = load_pickle(target_fp)
    for f in tweets_files:
        for t, tar_t in zip(f, target_tweets):
            assert t == t


@pytest.fixture
def attrs():
    attrs = (
        "id_str",
        "bla",
        "user.screen_name",
        "retweeted",
        "entities.user_mentions.,screen_name"
    )
    return attrs


@pytest.fixture
def target_df():
    return pd.read_pickle('test_data/target_tweets_df.pickle')


def test_load_tweets2df(test_dir_p, attrs, ext, target_df):
    df = load_tweets2df(test_dir_p, attrs, ext)
    assert df.equals(target_df)


def test_load_tweets2df_notimplemented_exeption(test_dir_p, ext):
    with pytest.raises(NotImplementedError):
        df = load_tweets2df(test_dir_p, ['entities,.user_mentions.bla'], ext)



