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


@pytest.mark.parametrize(
    'attrs,tar_df_p', [
        [
            ('id_str', 'user.screen_name', 'retweeted', 'bla'),
            'test_data/target_tweets_df.pickle'
        ]
    ]
)
def test_load_tweets2df(test_dir_p, attrs, tar_df_p, ext):
    df = load_tweets2df(test_dir_p, attrs, ext)
    df_tar = pd.read_pickle(tar_df_p)
    assert df.equals(df_tar)
