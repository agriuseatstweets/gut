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
    print('--- Getting original tweet count ...')
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


def get_reply_count(tweet_df, user):
    '''
    returns number of replies to tweets by user and day. retrieved by counting
    reply tweets in tweet_df

    :param pd.DataFrame tweet_df: tweet dataframe with tweet attributes as columns
    :param list user: target users
    :returns pd.DataFrame df_out: dataframe with aggregated 'reply_count' by user
    '''
    #df = df[['id_str', 'user.screen_name', 'in_reply_to_status_id_str', 'in_reply_to_screen_name']]
    print('--- Getting reply count ...')
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
    print('--- Getting retweet count ...')
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
    print('--- Getting mention count ...')
    for u in tqdm(user):
        df_fil.loc[:, 'mention_count'] = list(isin_listofsets(u, df_fil['entities.user_mentions.,screen_name']))
        df_fil.loc[:, 'user'] = u
        res += [
            df_fil\
                .groupby(['user', 'created_at_D'])\
                .mention_count\
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
    print('--- Getting edge weights ...')
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


def assert_concat_integrity(count_dfs, count_concat):
    for df in count_dfs:
        if len(df) == 0:
            continue
        idxs = np.random.choice(df.index, 10)
        for idx in idxs:
            assert df.loc[idx] == count_concat.loc[idx, df.columns[0]]


def get_descriptives(tweet_df, media_outlets, parties, candidates, dir_out_p):
    '''
    gets all types of counts from tweet dfs for users in media_outlets, parties
    and candidates. saves descriptives in csvs to dir_out_p


    :param pd.DataFrame tweet_df: dataframe with tweets
    :param list media_outlets: list of screen_name of media outlets
    :param list parties: list of screen_name of parties
    :param candidates: list of screen_name of candidates
    :param str dir_out_p: directory where to store the descriptives
    '''

    if dir_out_p[-1] != '/': dir_out_p += '/'
    all_user = (media_outlets, parties, candidates)
    all_user_n = ('media_outlets', 'parties', 'candidates')

    for i, (user, user_n) in enumerate(zip(all_user, all_user_n)):

        print('Getting descriptives for', user_n, str(i+1) + '/' + str(len(all_user_n)))

        # follower counts
        get_follower_counts(tweet_df, user).to_csv(dir_out_p + 'follower_counts_' + user_n + '.csv')

        # counts of original tweets, retweets, replies and mentions
        count_dfs = []
        count_dfs += [get_original_tweet_count(tweet_df, user)]
        count_dfs += [get_retweet_count(tweet_df, user)]
        count_dfs += [get_reply_count(tweet_df, user)]
        count_dfs += [get_mention_count(tweet_df, user)]
        count_concat = pd.concat(count_dfs, axis=1)
        count_concat.columns = [x.columns[0] for x in count_dfs]
        assert_concat_integrity(count_dfs, count_concat)
        del count_dfs
        dt_range = pd.date_range(tweet_df['created_at_D'].min(), tweet_df['created_at_D'].max(), freq='D')
        multiindex = pd.MultiIndex.from_product((user, dt_range), names=['user.screen_name', 'created_at_D'])
        count_concat = count_concat.reindex(multiindex)
        count_concat.sort_values('created_at_D', inplace=True)
        count_concat.fillna(0, inplace=True)
        count_concat.to_csv(dir_out_p + 'interaction_counts_by_day_' + user_n + '.csv')
        count_concat \
            .groupby('user.screen_name') \
            .sum() \
            .to_csv(dir_out_p + 'interaction_counts_' + user_n + '.csv')
        del count_concat

        # edges weights
        get_edge_weights(tweet_df, user).to_csv(dir_out_p + 'edges_weights_' + user_n + '.csv')

    print('Getting descriptives completed.')
