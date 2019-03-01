import pandas as pd
from load_tweets import *
from test_get_descriptives import *


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
    by users in user

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with 'original_tweet_count' by user
    '''
    df_out = tweet_df[['id_str', 'user.screen_name', 'in_reply_to_status_id_str', 'retweeted_status.id_str',
             'quoted_status.id_str']]
    df_out = df_out.loc[df_out['user.screen_name'].isin(user)]
    df_out = df_out[df_out['in_reply_to_status_id_str'].isnull()
                & df_out['retweeted_status.id_str'].isnull()
                & df_out['quoted_status.id_str'].isnull()] \
               .groupby('user.screen_name') \
               .id_str \
               .nunique()
    df_out.columns = ['original_tweet_count']
    return df_out


def get_reply_count(tweet_df, user):
    '''
    returns number of replies to tweets by user. retrieved by counting
    reply tweets in tweet_df

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with aggregated 'reply_count' by user
    '''
    #df = df[['id_str', 'user.screen_name', 'in_reply_to_status_id_str', 'in_reply_to_screen_name']]
    df_out = tweet_df.loc[tweet_df['in_reply_to_screen_name'].isin(user)
                    & tweet_df['in_reply_to_status_id_str'].isin(tweet_df['id_str'])] \
                   .groupby('in_reply_to_screen_name') \
                   .id_str \
                   .nunique()
    df_out.columns = ['reply_count']
    return df_out



def get_retweet_counts(tweet_df, user):
    '''
    returns sum of retweet_counts over tweets by user using approaches
    'retweeted_count_from_embedded_objects': retrieves 'retweet_count' from embedded 'retweeted_status' and
                                             'quoted_status' objects by taking the maximum 'retweet_count'
    'retweeted_count_counted': counts the retweets to the original tweets in the dataset

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with 'retweeted_count_from_embedded_objects' and 'retweeted_count_counted'
                                  by user
    '''
    #df = df[['created_at', 'id_str', 'user.screen_name', 'quoted_status.id_str',
    #         'quoted_status.retweet_count', 'quoted_status.user.screen_name',
    #         'retweeted_status.id_str', 'retweeted_status.retweet_count',
    #         'retweeted_status.user.screen_name']]
    df_counted = tweet_df.loc[tweet_df['retweeted_status.user.screen_name'].isin(user)
                        & tweet_df['retweeted_status.id_str'].isin(tweet_df['id_str'])] \
                       .groupby('retweeted_status.user.screen_name') \
                       .id_str \
                       .nunique()
    df_from_retweet = tweet_df.loc[tweet_df['retweeted_status.user.screen_name'].isin(user)
                             & tweet_df['retweeted_status.id_str'].isin(tweet_df['id_str'])] \
                            .groupby('retweeted_status.user.screen_name') \
                            ['retweeted_status.retweet_count'] \
                            .max()
    df_from_quote = tweet_df.loc[tweet_df['quoted_status.user.screen_name'].isin(user)
                           & tweet_df['quoted_status.id_str'].isin(tweet_df['id_str'])] \
                          .groupby('quoted_status.user.screen_name') \
                          ['quoted_status.retweet_count'] \
                          .max()
    df_merged = pd.concat((df_from_retweet, df_from_quote), axis=1, sort=False).max(axis=1)
    df_merged = pd.concat((df_merged, df_counted), axis=1, sort=False)
    df_merged.columns = ['retweeted_count_from_embedded_objects', 'retweet_count_counted']
    return df_merged



def get_mention_count(tweet_df, user):
    '''
    counts how many times user gets mentioned in tweets contained in tweet_df

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with 'mention_count' by user
    '''
    mention_count = (
        sum(isin_listofsets(u, tweet_df['entities.user_mentions.,screen_name']))
        for u in user
    )
    df_mention_count = pd.DataFrame({'mention_count': list(mention_count)}, index=user)
    return df_mention_count