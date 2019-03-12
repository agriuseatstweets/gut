import pandas as pd
import numpy as np
from tqdm import tqdm
from utils import *


def get_follower_counts(tweet_df, user):
    '''
    aggregates 'friends_count' and 'followers_count' for all users in user using the
    most recent observation according to 'created_at'

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df: dataframe with aggregated 'followers_count', 'friends_count'
                              and 'created_at'
    '''
    df_out = tweet_df[['user.screen_name', 'user.followers_count', 'user.friends_count', 'created_at']]
    df_out = df_out.loc[df_out['user.screen_name'].isin(user)]
    df_out = df_out.loc[df_out.groupby('user.screen_name').created_at.idxmax()]
    df_out = df_out.sort_values('user.followers_count', ascending=False)
    return df_out


def get_original_tweet_count(tweet_df, user):
    '''
    counts original tweets (tweet is both no retweet, reply and quoted_tweet) made
    by user and day

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with 'original_tweet_count' by user
    '''
    print('...... Getting original tweet count ...')
    df_out = tweet_df[['id_str', 'created_at_D', 'user.screen_name', 'in_reply_to_status_id_str',
                       'retweeted_status.id_str', 'quoted_status.id_str']]
    df_out = df_out.loc[df_out['user.screen_name'].isin(user)]
    df_out = df_out\
        [df_out['in_reply_to_status_id_str'].isnull()
         & df_out['retweeted_status.id_str'].isnull()
         & df_out['quoted_status.id_str'].isnull()] \
        .groupby(['user.screen_name', 'created_at_D']) \
        .id_str \
        .nunique()
    df_out.columns = ['original_tweet_count']
    df_out.index.names = ['user.screen_name', 'created_at_D']
    return df_out


def tweet_df():
    tweet_df = pd.DataFrame({
        'id_str': ['0', '1', '2', '3', '4', '5'],
        'created_at_D': [1, 2, 1, 1, 1, 1],
        'user.screen_name': ['a', 'a', 'a', 'b', 'd', 'e'],
        'in_reply_to_status_id_str': [None, None, None, None, None, None],
        'retweeted_status.id_str': [None, None, None, None, None, None],
        'quoted_status.id_str': [None, None, None, None, None, None]
    })
    return tweet_df


def get_reply_count(tweet_df, user):
    '''
    returns number of replies to tweets by user and day. retrieved by counting
    reply tweets in tweet_df

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with aggregated 'reply_count' by user
    '''
    print('...... Getting reply count ...')
    df_out = tweet_df\
        .loc[tweet_df['in_reply_to_screen_name'].isin(user)
             & tweet_df['in_reply_to_status_id_str'].isin(tweet_df['id_str'])] \
        .groupby(['in_reply_to_screen_name', 'created_at_D']) \
        .id_str \
        .nunique()
    df_out.columns = ['reply_count']
    df_out.index.names = ['user.screen_name', 'created_at_D']
    return df_out


def get_retweet_count(tweet_df, user):
    '''
    returns sum of retweet_counts over tweets by user and day retrieved by counting retweets in the dataset

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with 'retweeted_count_from_embedded_objects' and 'retweeted_count_counted'
                                  by user
    '''
    print('...... Getting retweet count ...')
    df_out = tweet_df\
        .loc[tweet_df['retweeted_status.user.screen_name'].isin(user)
             & tweet_df['retweeted_status.id_str'].isin(tweet_df['id_str'])] \
        .groupby(['retweeted_status.user.screen_name', 'created_at_D']) \
        .id_str \
        .nunique()
    df_out.columns = ['retweet_count']
    df_out.index.names = ['user.screen_name', 'created_at_D']
    return df_out


def get_mention_count(tweet_df, user):
    '''
    counts how many times user gets mentioned in tweets contained in tweet_df

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with 'mention_count' by user
    '''
    df_fil = tweet_df[['entities.user_mentions.,screen_name', 'created_at_D']].copy()
    res = []
    print('...... Getting mention count ...')
    for u in tqdm(user):
        df_fil.loc[:, 'mention_count'] = list(isin_listofsets(u, df_fil['entities.user_mentions.,screen_name'], True))
        df_fil.loc[:, 'user'] = u
        res += [
            df_fil
                .groupby(['user', 'created_at_D'])
                .mention_count
                .sum()
        ]
    df_res = pd.concat(res)
    df_res.columns = ['mention_count']
    df_res.index.names = ['user.screen_name', 'created_at_D']
    return df_res


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
