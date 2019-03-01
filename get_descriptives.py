import pandas as pd
from load_tweets import *
from test_get_descriptives import *
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
    return df_out


def get_reply_count(tweet_df, user):
    '''
    returns number of replies to tweets by user and day. retrieved by counting
    reply tweets in tweet_df

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with aggregated 'reply_count' by user
    '''
    #df = df[['id_str', 'user.screen_name', 'in_reply_to_status_id_str', 'in_reply_to_screen_name']]
    df_out = tweet_df\
        .loc[tweet_df['in_reply_to_screen_name'].isin(user)
             & tweet_df['in_reply_to_status_id_str'].isin(tweet_df['id_str'])] \
        .groupby(['in_reply_to_screen_name', 'created_at_D']) \
        .id_str \
        .nunique()
    df_out.columns = ['reply_count']
    return df_out


def get_retweet_count(tweet_df, user):
    '''
    returns sum of retweet_counts over tweets by user and day retrieved by counting retweets in the dataset

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with 'retweeted_count_from_embedded_objects' and 'retweeted_count_counted'
                                  by user
    '''
    #df = df[['created_at', 'id_str', 'user.screen_name', 'quoted_status.id_str',
    #         'quoted_status.retweet_count', 'quoted_status.user.screen_name',
    #         'retweeted_status.id_str', 'retweeted_status.retweet_count',
    #         'retweeted_status.user.screen_name']]
    df_out = tweet_df\
        .loc[tweet_df['retweeted_status.user.screen_name'].isin(user)
             & tweet_df['retweeted_status.id_str'].isin(tweet_df['id_str'])] \
        .groupby(['retweeted_status.user.screen_name', 'created_at_D']) \
        .id_str \
        .nunique()
    df_out.columns = ['retweet_count']
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
    for u in user:
        df_fil.loc[:, 'mention_count'] = list(isin_listofsets(u, df_fil['entities.user_mentions.,screen_name']))
        df_fil.loc[:, 'user'] = u
        res += [
            df_fil \
                .groupby(['user', 'created_at_D']) \
                .mention_count \
                .sum()
        ]
    df_res = pd.concat(res)
    df_res.columns = ['mention_count']
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
