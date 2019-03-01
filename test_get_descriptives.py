import pytest
import numpy as np
import pandas as pd
from utils import *
from get_descriptives import *


def test_pd_tz_convert():
    test_df = pd.DataFrame({'dt': ['2015-02-24 10:00:00']})
    test_df['dt'] = pd.to_datetime(test_df['dt'], utc=True)
    test_df['dt_converted'] = test_df['dt'].dt.tz_convert('Asia/Kolkata')
    assert pd.Timestamp('2015-02-24 15:30:00+0530', tz='Asia/Kolkata') == test_df['dt_converted'][0]


@pytest.fixture
def tweet_df():
    return pd.read_pickle('test_data/df2.pickle')


#def test_tweet_df_dtypes(tweet_df):
#    assert tweet_df['created_at'].dtype == 'datetime64[ns, Asia/Kolkata]'
#    assert all(isinstance(x, str) for x in tweet_df['id_str'])
#    assert all(isinstance(x, str) for x in tweet_df['user.screen_name'])
#    assert tweet_df['user.followers_count'].dtype == 'float64'
#    assert tweet_df['user.friends_count'].dtype == 'float64'
#    assert all(x is None or isinstance(x, str) for x in tweet_df['in_reply_to_status_id_str'])
#    assert all(x is None or isinstance(x, str) for x in tweet_df['in_reply_to_screen_name'])
#    assert all(x is None or isinstance(x, str) for x in tweet_df['quoted_status_id_str'])
#    assert tweet_df['quoted_status.retweet_count'].dtype == 'float64'
#    assert all(x is None or isinstance(x, str) for x in tweet_df['retweeted_status.id_str'])
#    assert tweet_df['retweeted_status.retweet_count'].dtype == 'float64'
#    assert all(x == {} or is_setofstr(x) for x in tweet_df['entities.user_mentions.,screen_name'])



@pytest.fixture
def follower_count():
    return pd.read_pickle('test_data/df2_follower_count.pickle')


def test_get_follower_counts(tweet_df, follower_count):
    np.random.seed(40)
    user = list(np.random.choice(np.unique(tweet_df['user.screen_name']), 10, replace=False))
    assert get_follower_counts(tweet_df, user).equals(follower_count)


@pytest.fixture
def original_tweet_count():
    return pd.read_pickle('test_data/df2_original_tweet_count')


def test_get_original_tweet_count(tweet_df, original_tweet_count):
    user = np.unique(tweet_df['user.screen_name'])
    assert get_original_tweet_count(tweet_df, user).equals(original_tweet_count)


@pytest.fixture
def reply_count():
    return pd.read_pickle('test_data/df2_reply_count')


def test_get_reply_count(tweet_df, reply_count):
    user = np.unique(tweet_df['user.screen_name'])
    assert get_reply_count(tweet_df, user).equals(reply_count)


@pytest.fixture
def retweet_counts():
    return pd.read_pickle('test_data/df2_retweet_counts')


def test_get_retweet_counts(tweet_df, retweet_counts):
    user = np.unique(tweet_df['user.screen_name'])
    assert get_retweet_counts(tweet_df, user).equals(retweet_counts)


@pytest.fixture
def mention_count():
    return pd.read_pickle('test_data/df2_mention_count')


def test_get_retweet_counts(tweet_df, mention_count):
    user = np.unique(tweet_df['user.screen_name'])[:200]
    assert get_mention_count(tweet_df, user).equals(mention_count)