import pytest
import pandas as pmd
from load_tweets import *
from utils import *


@pytest.fixture
def tweets_txt_p():
    return 'test_data/tweets.txt'


@pytest.fixture
def tweets():
    return load_pickle('test_data/tweets.pickle')


def test_load_tweets(tweets_txt_p, tweets):
    tweets_ = load_tweets_from_fp(tweets_txt_p)
    for t, tar_t in zip(tweets, tweets_):
        assert t == t


@pytest.fixture
def test_dir_p():
    return 'test_data'


@pytest.fixture
def ext():
    return '.txt'


def test_load_tweets_from_dir(test_dir_p, ext, tweets):
    tweets_files = load_tweets_from_dir(test_dir_p, ext)
    for f in tweets_files:
        for t, tar_t in zip(f, tweets):
            assert t == t


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


@pytest.fixture
def target_df():
    return pd.read_pickle('test_data/df1.pickle')


def test_load_tweets2df(test_dir_p, attrs, ext, target_df):
    df = load_tweets2df(test_dir_p, attrs, ext)
    assert df.equals(target_df)


def test_load_tweets2df_notimplemented_exeption(test_dir_p, ext):
    with pytest.raises(NotImplementedError):
        df = load_tweets2df(test_dir_p, ['entities,.user_mentions.bla'], ext)
