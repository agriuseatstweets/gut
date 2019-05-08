import pandas as pd
import numpy as np
from functools import reduce
from toolz import curry
from itertools import combinations
from copy import deepcopy
from itertools import combinations, permutations


def flatten_counts_to_df(di, l1, l2):
    return pd.DataFrame([{l1: v1, l2: v2, 'count': count}
                         for v1, values in di.items()
                         for v2, count in values.items()])

@curry
def get_engagement(by_date, users, tweets):
    c = all_counts(users, tweets)

    keys = ['original_tweet_count', 'retweet_count', 'reply_count', 'mention_count']

    cdfs = [flatten_counts_to_df(d, 'date', 'user.screen_name').assign(metric = k)
            for k,d in zip(keys, c)]
    df = pd.concat(cdfs).set_index(['user.screen_name', 'date', 'metric']).unstack(fill_value=0)
    df.columns = df.columns.get_level_values(1)
    df = df.reset_index()

    if by_date:
        return df
    else:
        return df.groupby(['user.screen_name'], as_index=False).sum()


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
    key = t.get('created_at').date().strftime('%Y-%m-%d')
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

def get_follower_count(users, tweets):
    tweets = (t for t in tweets if t.get('user.screen_name') in users)
    keys = ['user.followers_count', 'user.friends_count']
    return reduce(_followers(keys), tweets, {})

def follower_count(users, tweets):
    c = get_follower_count(users, tweets)
    return pd.DataFrame(c).T.reset_index().rename(columns = {'index': 'user.screen_name'})

@curry
def _followers(keys, acc, t):
    for k in keys:
        user = t['user.screen_name']
        try:
            acc[user][k] = max(t[k], acc[user][k])
            acc[user]['created_at'] = max(acc[user]['created_at'], t['created_at'])
        except:
            acc[user] = {k:0 for k in keys}
            acc[user][k] = t[k]
            acc[user]['created_at'] = t['created_at']
    return acc


def get_mentions(orgs, t):
    keys = ['in_reply_to_screen_name',
            'retweeted_status.user.screen_name',
            'entities.user_mentions.,screen_name']

    names = [t.get(k) for k in keys]
    names = [n if type(n) is list else [n] for n in names]
    names = [[n for n in k if n in orgs] for k in names]
    mentions = list(zip([0,1,2], names))
    return [(m,li) for m,li in mentions if li]


def count_edges(users, tweets):
    keys = ['weight_replies', 'weight_retweets', 'weight_mentions']
    lookup = {org: i for i,org in enumerate(users)}
    dis = reduce(build_dis(users, lookup), tweets, [{}, {}, {}])
    edges = sum_edges_di(len(users), dis)
    return edges_to_df(users, keys, edges)


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

def sum_edges_di(norgs, dis):
    # list of dictionaries, user: set of org_ids
    edges = np.zeros((len(dis), norgs, norgs), dtype=np.int32)

    for i,di in enumerate(dis):
        for user,orgs in di.items():
            pairs = permutations(orgs, 2)
            for k,l in pairs:
                edges[i, k, l] += 1

    return edges

def edges_to_df(orgs, keys, edges):
    norgs = edges.shape[1]
    rows = []
    for i in range(norgs):
        for j in range(norgs):
            weights = edges[:, i, j]
            if weights.sum() > 0:
                row = [orgs[i], orgs[j]] + list(weights)
                rows.append(row)

    return pd.DataFrame(rows, columns = ['user1', 'user2'] + keys)
