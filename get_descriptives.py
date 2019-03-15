import pandas as pd
import numpy as np
from tqdm import tqdm
from utils import *
from functools import reduce
from toolz import curry

def flatten_counts_to_df(di, l1, l2):
    return pd.DataFrame([{l1: v1, l2: v2, 'count': count}
                         for v1, values in di.items()
                         for v2, count in values.items()])

def retweet_count(users, tweets):
    picker = lambda t: t.get('retweeted_status.user.screen_name')
    return _count_per_user(users, picker, tweets)

def reply_count(users, tweets):
    picker = lambda t: t.get('in_reply_to_screen_name')
    return _count_per_user(users, picker, tweets)

def mention_count(users, tweets):
    picker = lambda t: t.get('entities.user_mentions.,screen_name')
    return _count_per_user(users, picker, tweets)

def _count_per_user(users, picker, tweets):
    reducer = _reduce_picker(users, picker)
    return reduce(_per_day(reducer), tweets, {})

@curry
def _reduce_picker(users, picker, acc, t):
    val = picker(t)
    if not val:
        return acc
    return _update_count(users, acc, val)

@curry
def _per_day(fn, acc, t):
    """ fn is a reducer """
    key = t.get('created_at').date().strftime('%a %b %d')
    acc[key] = fn(acc.get(key, {}), t)
    return acc

@curry
def _update_count(users, counts, vals):
    if type(vals) is not list:
        vals = [vals]

    vals = [v for v in vals if v in users]

    for val in vals:
        try:
            counts[val] += 1
        except KeyError:
            counts[val] = 1

    return counts

def original_count(users, tweets):
    tweets = (t for t in tweets if t.get('user.screen_name') in users)
    names = (t.get('user.screen_name') for t in tweets if _is_original(t))
    return reduce(_update_count(users), names, {})

def _is_original(t):
    keys = ['in_reply_to_status_id_str',
            'retweeted_status.id_str',
            'quoted_status.id_str']

    return not [t.get(k) for k in keys if t.get(k)]

def follower_count(users, tweets):
    tweets = (t for t in tweets if t.get('user.screen_name') in users)
    keys = ['user.followers_count', 'user.friends_count']
    return reduce(_followers(keys), tweets, {})

@curry
def _followers(keys, acc, t):
    for k in keys:
        user = t['user.screen_name']
        try:
            acc[user][k] = max(t[k], acc[user][k])
        except:
            acc[user] = {k:0 for k in keys}
            acc[user][k] = t[k]
    return acc



def get_edge_weights(tweet_df, user):
    '''
    for all pairwise combinations of users, this function counts how many users
    interact with both users of the respective combination. interaction is divided into
    reply to tweets by user, retweet tweety by user or mention user

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame res: dataframe with edge counts
    '''
    user_combs = pairwise_combs(user)
    res = []
    print('...... Getting edge weights ...')
    for u1, u2 in tqdm(user_combs):
        w_reply = tweet_df\
            [tweet_df['in_reply_to_screen_name'].isin([u1, u2])]\
            .groupby('user.screen_name')\
            ['in_reply_to_screen_name']\
            .nunique()\
            .ge(2)\
            .sum()
        w_retweet = tweet_df\
            [tweet_df['retweeted_status.user.screen_name'].isin([u1, u2])]\
            .groupby('user.screen_name')\
            ['in_reply_to_screen_name']\
            .nunique()\
            .ge(2)\
            .sum()
        # w_mention
        idxs = outerjoin_lists(
            get_idxs_true(isin_listofsets(u1, tweet_df['entities.user_mentions.,screen_name'])),
            get_idxs_true(isin_listofsets(u2, tweet_df['entities.user_mentions.,screen_name'])),
        )
        df_fil = tweet_df.iloc[idxs]
        df_fil = df_fil \
            .groupby('user.screen_name') \
            .apply(lambda x: set.union(*x['entities.user_mentions.,screen_name']))
        w_mention = np.array(list(isin_listofsets(u1, df_fil, return_int=True))) \
            .dot(list(isin_listofsets(u2, df_fil, return_int=True)))
        res += [(u1, u2, w_reply, w_retweet, w_mention)]
    res = pd.DataFrame(res)
    res.columns = ['user1', 'user2', 'weight replies', 'weight retweets', 'weight mentions']
    return res


def get_counts(tweet_df, user):
    '''
    wrapper for get_original_tweet_count, get_retweet_count, get_reply_count
    and get mention count; returns list of outputs from these functions

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns list: list of dataframes with count output
    '''
    counts = []
    counts += [get_original_tweet_count(tweet_df, user)]
    counts += [get_retweet_count(tweet_df, user)]
    counts += [get_reply_count(tweet_df, user)]
    counts += [get_mention_count(tweet_df, user)]
    return counts


def assert_concat(counts, counts_concat):
    '''
    tests whether the concatenated counts_concat contains the same values as the
    list of dfs counts by comparing 10 random values for each df in counts
    '''
    assert sum(len(df) for df in counts) > 0
    for df in counts:
        if len(df) == 0:
            continue
        idxs = np.random.choice(df.index, 10)
        for idx in idxs:
            assert df.loc[idx] == counts_concat.loc[idx, df.columns[0]]


def safe_concat(counts):
    '''
    concatenates the list of dfs counts along axis=1 and asserts
    concat has worked by calling assert_concat

    :param list counts: list of dfs with output from count functions
    :returns pd.DataFrame: concatenated dataframe
    '''
    print(len(counts))
    counts_concat = pd.concat(counts, axis=1)
    counts_concat.columns = [x.columns[0] for x in counts]
    assert_concat(counts, counts_concat)
    return counts_concat


def make_ts(tweet_df, user, counts_concat):
    '''
    converts the counts_concat into a timeseries by adding days with no observed counts

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param pd.DataFrame counts_concat: dataframe with counts as columns
    :returns pd.DataFrame: dataframe with timestamps
    '''
    dt_range = pd.date_range(tweet_df['created_at_D'].min(), tweet_df['created_at_D'].max(), freq='D')
    multiindex = pd.MultiIndex.from_product((user, dt_range), names=['user.screen_name', 'created_at_D'])
    counts_concat = counts_concat.reindex(multiindex)
    counts_concat.sort_values('created_at_D', inplace=True)
    counts_concat.fillna(0, inplace=True)
    return counts_concat


def get_processed_counts(tweet_df, user):
    '''
    wrapper to get counts of  counts of original tweets, retweets, replies and mentions
    and process the output (concatenation, add days with no observed counts)

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.Dataframe: dataframe with the counts as columns
    '''
    counts_concat = get_counts(tweet_df, user)
    counts_concat = safe_concat(counts_concat)
    counts_concat = make_ts(tweet_df, user, counts_concat)
    return counts_concat


def agg_over_days(counts_concat):
    counts_concat_agg = counts_concat\
        .groupby('user.screen_name') \
        .sum()
    return counts_concat_agg


def get_descriptives(tweet_df, all_user):
    '''
    gets all types of counts from tweet dfs for users in media_outlets, parties
    and candidates


    :param pd.DataFrame tweet_df: dataframe with tweets
    :param dict all_user: keys are names (e.g. candidates) values are list of screen_names
    :returns
    '''

    r = {user_n: {} for user_n in all_user.keys()}

    for i, (user_n, user) in enumerate(all_user.items()):

        print('... Getting descriptives user group', str(i+1) + '/' + str(len(all_user)))

        # follower counts
        r[user_n]['follower_counts'] = get_follower_counts(tweet_df, user)

        # counts of original tweets, retweets, replies and mentions
        r[user_n]['engagement_counts_by_day'] = get_processed_counts(tweet_df, user)
        r[user_n]['engagement_counts'] = agg_over_days(r[user_n]['engagement_counts_by_day'])

        # edges weights
        r[user_n]['edges_weights'] = get_edge_weights(tweet_df, user)

    print('... Getting descriptives completed.')

    return r
