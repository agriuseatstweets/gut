import pytest
import pandas as pd
from get_descriptives import *
from datetime import datetime

def test_pd_tz_convert():
    test_df = pd.DataFrame({'dt': ['2015-02-24 10:00:00']})
    test_df['dt'] = pd.to_datetime(test_df['dt'], utc=True)
    test_df['dt_converted'] = test_df['dt'].dt.tz_convert('Asia/Kolkata')
    assert pd.Timestamp('2015-02-24 15:30:00+0530', tz='Asia/Kolkata') == test_df['dt_converted'][0]


@pytest.fixture
def tweet_df():
    return pd.read_pickle('test_data/df2.pickle')


# def test_tweet_df_dtypes(tweet_df):
#     assert tweet_df['created_at'].dtype == 'datetime64[ns, Asia/Kolkata]'
#     assert tweet_df['created_at_D'].dtype == 'datetime64[ns, Asia/Kolkata]'
#     assert all(isinstance(x, str) for x in tweet_df['id_str'])
#     assert all(isinstance(x, str) for x in tweet_df['user.screen_name'])
#     assert tweet_df['user.followers_count'].dtype == 'float64'
#     assert tweet_df['user.friends_count'].dtype == 'float64'
#     assert all(x is None or isinstance(x, str) for x in tweet_df['in_reply_to_status_id_str'])
#     assert all(x is None or isinstance(x, str) for x in tweet_df['in_reply_to_screen_name'])
#     assert all(x is None or isinstance(x, str) for x in tweet_df['retweeted_status.id_str'])
#     assert all(x is None or isinstance(x, str) for x in tweet_df['retweeted_status.user.screen_name'])
#     assert all(x is None or isinstance(x, str) for x in tweet_df['quoted_status.id_str'])
#     assert all(x == {} or is_setofstr(x) for x in tweet_df['entities.user_mentions.,screen_name'])


def test_reply_count_gathers_by_day():
    tweets = [
        {'created_at': datetime(2019,5,1), 'in_reply_to_screen_name': 'foo'},
        {'created_at': datetime(2019,5,1), 'in_reply_to_screen_name': 'bar'},
        {'created_at': datetime(2019,4,1), 'in_reply_to_screen_name': 'foo'},
        {'created_at': datetime(2019,4,1), 'in_reply_to_screen_name': 'foo'},
        {'created_at': datetime(2019,4,1), 'in_reply_to_screen_name': 'baz'},
        {'created_at': datetime(2019,4,1)},
        {'created_at': datetime(2019,3,1), 'in_reply_to_screen_name': 'bar'}
    ]

    counts = all_counts(['foo', 'bar'], tweets)
    counts = counts[2]
    ans = {'Wed May 01': {'foo': 1, 'bar': 1}, 'Mon Apr 01': {'foo': 2}, 'Fri Mar 01': {'bar': 1}}
    assert(counts == ans)


def test_retweet_count_gathers_by_day():
    tweets = [
        {'created_at': datetime(2019,5,1), 'retweeted_status.user.screen_name': 'foo'},
        {'created_at': datetime(2019,5,1), 'retweeted_status.user.screen_name': 'bar'},
        {'created_at': datetime(2019,4,1), 'retweeted_status.user.screen_name': 'foo'},
        {'created_at': datetime(2019,4,1), 'retweeted_status.user.screen_name': 'foo'},
        {'created_at': datetime(2019,4,1), 'retweeted_status.user.screen_name': 'baz'},
        {'created_at': datetime(2019,4,1)},
        {'created_at': datetime(2019,3,1), 'retweeted_status.user.screen_name': 'bar'}
    ]

    counts = all_counts(['foo', 'bar'], tweets)
    counts = counts[1]
    ans = {'Wed May 01': {'foo': 1, 'bar': 1}, 'Mon Apr 01': {'foo': 2}, 'Fri Mar 01': {'bar': 1}}
    assert(counts == ans)


def test_mention_count_gathers_by_day():
    tweets = [
        {'created_at': datetime(2019,5,1), 'entities.user_mentions.,screen_name': ['foo', 'foo']},
        {'created_at': datetime(2019,5,1), 'entities.user_mentions.,screen_name': ['bar', 'foo', 'qux']},
        {'created_at': datetime(2019,4,1), 'entities.user_mentions.,screen_name': ['foo']},
        {'created_at': datetime(2019,4,1), 'entities.user_mentions.,screen_name': ['foo', 'qux']},
        {'created_at': datetime(2019,4,1), 'entities.user_mentions.,screen_name': ['baz']},
        {'created_at': datetime(2019,4,1), 'entities.user_mentions.,screen_name': []},
        {'created_at': datetime(2019,3,1), 'entities.user_mentions.,screen_name': ['bar']}
    ]

    counts = all_counts(['foo', 'bar', 'qux'], tweets)[3]
    ans = {'Wed May 01': {'foo': 3, 'bar': 1, 'qux': 1}, 'Mon Apr 01': {'foo': 2, 'qux': 1 }, 'Fri Mar 01': {'bar': 1}}
    assert(counts == ans)



def test_original_count_gathers_by_day():
    tweets = [
        {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo', 'in_reply_to_status_id_str': 'bar'},
        {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo', 'in_reply_to_status_id_str': 'bar'},
        {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo'},
        {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo', 'retweeted_status.id_str': 'bar'},
        {'created_at': datetime(2019,5,1), 'user.screen_name': 'baz'},
        {'created_at': datetime(2019,5,1), 'user.screen_name': 'bar'},
        {'created_at': datetime(2019,5,2), 'user.screen_name': 'bar'},
    ]

    counts = all_counts(['foo', 'bar'], tweets)[0]
    ans = {'Wed May 01': {'foo': 1, 'bar': 1}, 'Thu May 02': {'bar': 1} }
    assert(counts == ans)


def test_follower_count():
    tweets = [
        {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo', 'user.followers_count': 2, 'user.friends_count': 3},
        {'created_at': datetime(2019,5,1), 'user.screen_name': 'bar', 'user.followers_count': 1, 'user.friends_count': 1},
        {'created_at': datetime(2019,5,2), 'user.screen_name': 'foo', 'user.followers_count': 5, 'user.friends_count': 5},
    ]

    counts = follower_count(['foo', 'bar'], tweets)
    ans = { 'foo': {'user.followers_count': 5, 'user.friends_count': 5},
            'bar': {'user.followers_count': 1, 'user.friends_count': 1} }
    assert(counts == ans)

def test_lookup_user():
    assert(lookup_user(0, {}, 'foo') == (1, {'foo': 0}, 0))
    assert(lookup_user(1, {'foo': 0}, 'foo') == (1, {'foo': 0}, 0))
    assert(lookup_user(1, {'foo': 0}, 'bar') == (2, {'foo': 0, 'bar': 1}, 1))
    assert(lookup_user(2, {'foo': 0, 'bar': 1}, 'bar') == (2, {'foo': 0, 'bar': 1}, 1))


def test_get_mentions():
    tweet = {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo', 'in_reply_to_screen_name': 'baz', 'entities.user_mentions.,screen_name': ['bar']}

    assert(get_mentions(['bar', 'baz'], tweet) == [(0, ['baz']), (2, ['bar'])])

def test_build_dis():
    tweets = [
        {'user.screen_name': 'a', 'created_at': datetime(2019,5,1), 'entities.user_mentions.,screen_name': ['foo', 'foo']},
        {'user.screen_name': 'a', 'created_at': datetime(2019,5,1), 'entities.user_mentions.,screen_name': ['bar', 'foo', 'qux']},
        {'user.screen_name': 'a', 'created_at': datetime(2019,4,1), 'entities.user_mentions.,screen_name': ['foo']},
        {'user.screen_name': 'a', 'created_at': datetime(2019,4,1), 'entities.user_mentions.,screen_name': ['foo', 'qux']},
        {'user.screen_name': 'b', 'created_at': datetime(2019,4,1), 'entities.user_mentions.,screen_name': ['baz']},
        {'user.screen_name': 'a', 'created_at': datetime(2019,4,1), 'entities.user_mentions.,screen_name': []},
        {'user.screen_name': 'a', 'created_at': datetime(2019,3,1), 'entities.user_mentions.,screen_name': ['bar'], 'in_reply_to_screen_name': 'qux'}
    ]

    orgs = ['foo', 'bar', 'baz', 'qux']
    lookup = {k:i for i,k in enumerate(orgs)}
    dis = reduce(build_dis(orgs, lookup), tweets, [{}, {}, {}])
    assert(dis == [{'a': {3}}, {}, {'a': {0,1,3}, 'b': {2}}])
    assert(np.all(sum_edges_di(4, dis) == np.array([[[0,0,0,0],
                                                     [0,0,0,0],
                                                     [0,0,0,0],
                                                     [0,0,0,0]],
                                                    [[0,0,0,0],
                                                     [0,0,0,0],
                                                     [0,0,0,0],
                                                     [0,0,0,0]],
                                                    [[0,1,0,1],
                                                     [0,0,0,1],
                                                     [0,0,0,0],
                                                     [0,0,0,0]]])))



# def test_flatten_counts_to_df():
#     # Todo test this
#     nested = {'Wed May 01': {'foo': 3, 'bar': 1, 'qux': 1}, 'Mon Apr 01': {'foo': 2, 'qux': 1 }, 'Fri Mar 01': {'bar': 1}}
#     df = flatten_counts_to_df(nested, 'date', 'user')
#     print(df)
#     nested = {'foo': {'user.followers_count': 5, 'user.friends_count': 5}, 'bar': {'user.followers_count': 1, 'user.friends_count': 1} }
#     df = flatten_counts_to_df(nested, 'user', 'measure')
#     print(df)
#     assert(False)


# def test_get_edge_weights():
#     tweet_df = pd.DataFrame(
#         {'user.screen_name': ['a', 'a', 'b', 'b', 'c', 'c', 'd', 'a', 'b'],
#          'in_reply_to_screen_name': [1, 2, 1, 2, 1, 1, 2, 1, 3],
#          'retweeted_status.user.screen_name': [1, 2, 1, 2, 1, 1, 2, 1, 3],
#          'entities.user_mentions.,screen_name': [{1, 2, 3}, {1}, {3, 4}, {1}, {2}, {1}, {2}, {1}, {1}]}
#     )

#     tweets = [
#         {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo', 'in_reply_to_status_id_str': 'bar'},
#         {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo', 'in_reply_to_status_id_str': 'bar'},
#         {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo'},
#         {'created_at': datetime(2019,5,1), 'user.screen_name': 'foo', 'retweeted_status.id_str': 'bar'},
#         {'created_at': datetime(2019,5,1), 'user.screen_name': 'baz'},
#         {'created_at': datetime(2019,5,1), 'user.screen_name': 'bar'},
#     ]

#     edge_weights = pd.DataFrame(
#         {'user1': ['foo', 'foo', 'bar'],
#          'user2': ['bar', 'baz', 'baz'],
#          'weight replies': [2, 1, 0],
#          'weight retweets': [2, 1, 0],
#          'weight mentions': [2, 2, 1]}
#     )

#     assert get_edge_weights(['foo', 'bar', 'baz'], tweets).equals(edge_weights)
