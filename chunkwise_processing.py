from load_tweets import *
from get_descriptives import *
from utils import *


def get_descriptives_chunkwise(all_user, tweet_fps, tz, chunksize=100):
    '''
    calls get_descriptives chunkwise

    :param dict all_user: dict with {'user_class': ['user1', 'user2'...]} structure
    :param list tweet_fps: list of filepaths to tweetfiles
    :param str tz: string of datetime timezome
    :param int chunksize: number of tweetfiles in one chunk
    :returns list: list with structure [{'user': {'descr1': descr1}}, {'user': {'descr1': descr1}}, ...]
    '''
    chu_descr = []
    n_chunks = len(list(chunks(tweet_fps, chunksize)))
    for i, c in enumerate(chunks(tweet_fps, chunksize)):
        print('LOADING AND EXTRACTING DESCRIPTIVES FOR CHUNK', str(i+1) + '/' + str(n_chunks))
        tweet_df = load_tweets(c, tz)
        chu_descr += [get_descriptives(tweet_df, all_user)]
    return chu_descr


def concat_follower_counts(follower_counts):
    '''concats list of follower counts'''
    concat = pd.concat(follower_counts, ignore_index=True)
    concat = concat.loc[concat.groupby('user.screen_name').created_at.idxmax()]
    concat = concat.sort_values('user.followers_count', ascending=False)
    concat.index = concat['user.screen_name']
    return concat


def concat_engagement_counts(engagement_counts):
    '''concats list of engagement_counts'''
    concat = pd.concat(engagement_counts, ignore_index=False)
    concat = concat.groupby(concat.index).sum()
    return concat


def concat_edges_weights(edges_weights):
    '''concats list of edges_weight'''
    concat = pd.concat(edges_weights).groupby(['user1', 'user2']).sum()
    return concat


def concat_chunks_descriptives(chu_descr):
    '''
    wraps concatenation for different descriptives

    :param list chu_descr: list with structure [{'user': {'descr1': descr1}}, {'user': {'descr1': descr1}}, ...]
    :returns dict: concatenated descriptives with structure {'user': {'descr1': descr1}}
    '''
    concat = {user_n: {} for user_n in chu_descr[0].keys()}
    for user_n in chu_descr[0].keys():
        concat[user_n]['follower_counts'] = concat_follower_counts((x[user_n]['follower_counts'] for x in chu_descr))
        concat[user_n]['engagement_counts'] = concat_engagement_counts((x[user_n]['engagement_counts']
                                                                        for x in chu_descr))
        concat[user_n]['engagement_counts_by_day'] = concat_engagement_counts((x[user_n]['engagement_counts']
                                                                               for x in chu_descr))
        concat[user_n]['edges_weights'] = concat_edges_weights((x[user_n]['edges_weights'] for x in chu_descr))
    return concat
