import pandas as pd
import numpy as np
from tqdm import tqdm
from utils import *
from functools import reduce
from toolz import curry
from itertools import combinations
from copy import deepcopy
from itertools import combinations

import zlib

def flatten_counts_to_df(di, l1, l2):
    return pd.DataFrame([{l1: v1, l2: v2, 'count': count}
                         for v1, values in di.items()
                         for v2, count in values.items()])


def all_counts(users, tweets):
    pickers = [lambda t: t.get('user.screen_name') if _is_original(t) else None,
               lambda t: t.get('retweeted_status.user.screen_name'),
               lambda t: t.get('in_reply_to_screen_name'),
               lambda t: t.get('entities.user_mentions.,screen_name')]

    reducers = [_per_day(_reduce_picker(users, picker)) for picker in pickers]
    inits = [{} for _ in pickers]
    return multireduce(tweets, reducers, inits)


def multireduce(iterable, funcs, inits):
    def reducer(accs, t):
        return [func(acc, t) for acc,func in zip(accs, funcs)]

    return reduce(reducer, iterable, inits)

def _is_original(t):
    keys = ['in_reply_to_status_id_str',
            'retweeted_status.id_str',
            'quoted_status.id_str']

    return not [t.get(k) for k in keys if t.get(k)]

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
    return reduce(_freq, vals, counts)

@curry
def _freq(counts, val):
    try:
        counts[val] += 1
    except KeyError:
        counts[val] = 1
    return counts

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


def make_edge_df(edges):
    [{'users': k, 'weight': w} for k,w in edges.items()]
    pass

def get_edge_weights(users, tweets):


    pass

def get_edge_weights(users, tweets):
    edges = (_make_pairs(_get_all_names_in_tweet(users, t)) for t in tweets)
    keys = ('{}:{}'.format(a,b) for a,b in edges)
    return reduce(_freq, keys, {})

def _make_pairs(names):
    return combinations(sorted(names), 2)

def _get_all_edges_in_tweet(users, tweet):

    a = tweet.get('user.screen_name')
    # handle missing screen_name???

    # replies, retweets, mentions
    keys = ['in_reply_to_screen_name',
            'retweeted_status.user.screen_name',
            'entities.user_mentions.,screen_name']

    names = [tweet.get(k) for k in keys]
    names = [n if type(n) is list else [n] for n in names if n]
    names = [[n for n in na if n] for na in names if n in users]

    # return user1, [(key, names) for key in keys]
    return a, list(zip(keys, names))

@curry
def acc_edges(users, acc, t):
    a, edges = _get_all_edges_in_tweet(users, t)
    '{}:{}'

# first pass through data to collect all users who mentioned at least (2) of our
# orgs ?


def lookup_user_ht(id_range, user):
    return zlib.adler32(user.encode('utf8')) % id_range

def lookup_user(count, lookup, user):
    idx = lookup.get(user)
    if idx is None:
        lookup[user] = count
        idx = count
        count += 1
    return count, lookup, idx


def get_mentions(orgs, t):
    keys = ['in_reply_to_screen_name',
            'retweeted_status.user.screen_name',
            'entities.user_mentions.,screen_name']

    names = [t.get(k) for k in keys]
    names = [n if type(n) is list else [n] for n in names]
    names = [[n for n in k if n in orgs] for k in names]
    mentions = list(zip([0,1,2], names))
    return [(m,li) for m,li in mentions if li]


@curry
def build_array(orgs, org_lookup, acc, t):
    user = t.get('user.screen_name')

    # handle no screen_name?
    if user is None:
        return acc

    count, lookup, arr = acc
    count, lookup, idx = lookup_user(count, lookup, user)
    mentions = get_mentions(orgs, t)

    for m,org in mentions:
        org_id = org_lookup[org]
        arr[idx, m, org_id] += 1

    return count, lookup, arr


@curry
def build_dis(orgs, org_lookup, dis, t):
    user = t.get('user.screen_name')

    # handle no screen_name?
    if user is None:
        return dis

    mentions = get_mentions(orgs, t)

    for m,orgs in mentions:
        for org in orgs:
            org_id = org_lookup[org]
            try:
                dis[m][user].add(org_id)
            except KeyError:
                dis[m][user] = {org_id}
    return dis

# def add_edges(edges, a):
# for i in measures
# for users:

# user: { orgs mentioned }
# for each user
# for each org
# pairs = combinations(orgs, 2)

def sum_edges_di(norgs, dis):
    # list of dictionaries, user: set of org_ids
    edges = np.zeros((len(dis), norgs, norgs), dtype=np.int32)

    for i,di in enumerate(dis):
        for user,orgs in di.items():
            pairs = combinations(orgs, 2)
            for k,l in pairs:
                edges[i, k, l] += 1

    return edges

def edges_to_df(orgs, keys, edges):

    # make symmetric
    edges += edges.T

    x,y,z = edges.nonzero()


    # for i in range(len(x)):
    #     org_a, org_b = orgs[y[i]], orgs[z[i]]
    #     key = keys[x[i]]
    #     val = edges[x[i], y[i], z[i]]
    #     return { key: val}


def sum_edges(arr):
    # array is shape: mentions, users, orgs
    mentioned_types, nusers, norgs = arr.shape

    edges = np.zeros((mentioned_types, norgs, norgs), dtype=np.int32)

    for i in np.arange(mentioned_types):
        for j in np.unique(arr[i].nonzero()[0]):
            mentions = arr[i, j, :].nonzero()[0]
            pairs = combinations(mentions, 2)
            for k,l in pairs:
                edges[i, k, l] += 1

    return edges


# for each user in our list (key)
# tuple of sets
# each sets is the ids of all users who ___'d that user
# then you need to loop through all users afterwards... mess


# 3-d array
# 1d is user (idx lookup needed)
# 1d is mention-type
# 1d is mentioned org

# then go through users and assign edges to all combos in array


# make separate for all types of edges!!!


# def get_edge_weights(tweet_df, user):
#     '''
#     for all pairwise combinations of users, this function counts how many users
#     interact with both users of the respective combination. interaction is divided into
#     reply to tweets by user, retweet tweety by user or mention user

#     :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
#     :param list user: target users
#     :returns pd.DataFrame res: dataframe with edge counts
#     '''
#     user_combs = pairwise_combs(user)
#     res = []
#     print('...... Getting edge weights ...')
#     for u1, u2 in tqdm(user_combs):
#         w_reply = tweet_df\
#             [tweet_df['in_reply_to_screen_name'].isin([u1, u2])]\
#             .groupby('user.screen_name')\
#             ['in_reply_to_screen_name']\
#             .nunique()\
#             .ge(2)\
#             .sum()
#         w_retweet = tweet_df\
#             [tweet_df['retweeted_status.user.screen_name'].isin([u1, u2])]\
#             .groupby('user.screen_name')\
#             ['in_reply_to_screen_name']\
#             .nunique()\
#             .ge(2)\
#             .sum()
#         # w_mention
#         idxs = outerjoin_lists(
#             get_idxs_true(isin_listofsets(u1, tweet_df['entities.user_mentions.,screen_name'])),
#             get_idxs_true(isin_listofsets(u2, tweet_df['entities.user_mentions.,screen_name'])),
#         )
#         df_fil = tweet_df.iloc[idxs]
#         df_fil = df_fil \
#             .groupby('user.screen_name') \
#             .apply(lambda x: set.union(*x['entities.user_mentions.,screen_name']))
#         w_mention = np.array(list(isin_listofsets(u1, df_fil, return_int=True))) \
#             .dot(list(isin_listofsets(u2, df_fil, return_int=True)))
#         res += [(u1, u2, w_reply, w_retweet, w_mention)]
#     res = pd.DataFrame(res)
#     res.columns = ['user1', 'user2', 'weight replies', 'weight retweets', 'weight mentions']
#     return res


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
