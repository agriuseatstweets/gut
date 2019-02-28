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
def test_df():
    return pd.read_pickle('test_data/df2.pickle')


def test_df_dtypes(test_df):
    assert test_df['created_at'].dtype == 'datetime64[ns, Asia/Kolkata]'
    assert all(isinstance(x, str) for x in test_df['id_str'])
    assert all(isinstance(x, str) for x in test_df['user.screen_name'])
    assert test_df['user.followers_count'].dtype == 'float64'
    assert test_df['user.friends_count'].dtype == 'float64'
    assert all(x is None or isinstance(x, str) for x in test_df['in_reply_to_status_id_str'])
    assert all(x is None or isinstance(x, str) for x in test_df['in_reply_to_screen_name'])
    assert all(x is None or isinstance(x, str) for x in test_df['quoted_status_id_str'])
    assert test_df['quoted_status.retweet_count'].dtype == 'float64'
    assert all(x is None or isinstance(x, str) for x in test_df['retweeted_status.id_str'])
    assert test_df['retweeted_status.retweet_count'].dtype == 'float64'
    assert all(x == {} or is_setofstr(x) for x in test_df['entities.user_mentions.,screen_name'])


@pytest.fixture
def target_follower_count():
    return pd.read_pickle('test_data/df2_follower_count.pickle')


def test_get_follower_count(test_df, target_follower_count):
    np.random.seed(40)
    user = list(np.random.choice(np.unique(test_df['user.screen_name']), 10, replace=False))
    follower_count = get_follower_count(test_df, user)
    assert get_follower_count(test_df, user).equals(target_follower_count)


