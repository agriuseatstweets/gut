from load_tweets import *
from get_descriptives import *
from utils import *


def get_descriptives_chunkwise(all_user, tweet_fps, tz, chunksize=100, fs=None):
    '''
    calls get_descriptives chunkwise

    :param dict all_user: dict with {'user_class': ['user1', 'user2'...]} structure
    :param list tweet_fps: list of filepaths to tweetfiles
    :param str tz: string of datetime timezome
    :param int chunksize: number of tweetfiles in one chunk
    :param gcsfs.GCSFileSystem fs: access to google cloud storeage
    :returns list: list with structure [{'user': {'descr1': descr1}}, {'user': {'descr1': descr1}}, ...]
    '''
    chu_descr = []
    n_chunks = len(list(chunks(tweet_fps, chunksize)))
    for i, c in enumerate(chunks(tweet_fps, chunksize)):
        print('Chunkwise processing for chunk', str(i+1) + '/' + str(n_chunks), '...')
        tweet_df = load_tweets(c, tz, fs=fs)
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


def concat_all_descriptives(chu_descr):
    '''
    wraps concatenation for different descriptives

    :param list chu_descr: list with structure [{'user': {'descr1': descr1}}, {'user': {'descr1': descr1}}, ...]
    :returns dict: concatenated descriptives with structure {'user': {'descr1': descr1}}
    '''

    print('Concatenating descriptives ...')
    concat = {user_n: {} for user_n in chu_descr[0].keys()}
    for user_n in chu_descr[0].keys():
        concat[user_n]['follower_counts'] = concat_follower_counts((x[user_n]['follower_counts'] for x in chu_descr))
        concat[user_n]['engagement_counts'] = concat_engagement_counts((x[user_n]['engagement_counts']
                                                                        for x in chu_descr))
        concat[user_n]['engagement_counts_by_day'] = concat_engagement_counts((x[user_n]['engagement_counts_by_day']
                                                                               for x in chu_descr))
        concat[user_n]['edges_weights'] = concat_edges_weights((x[user_n]['edges_weights'] for x in chu_descr))
    return concat


def dump2csv(concat, dir_p, tz, fs=None):
    if dir_p[-1] != '/': dir_p += '/'
    for user_n, v in concat.items():
        for descr_n, descr in v.items():
            if descr_n == 'engagement_counts_by_day':
                descr[['user.screen_name', 'day_tz=' + tz]] = pd.DataFrame(descr.index.to_list(), index=descr.index)
                descr['day_tz=' + tz] = descr['day_tz=' + tz].astype(str).str[:10]
                descr = descr[[
                                 'user.screen_name',
                                 'day_tz=' + tz,
                                 'original_tweet_count',
                                 'retweet_count',
                                 'reply_count',
                                 'mention_count'
                             ]]
            elif descr_n == 'engagement_counts':
                descr.reset_index(inplace=True)
            elif descr_n == 'edges_weights':
                descr[['user1', 'user2']] = pd.DataFrame(descr.index.to_list(), index=descr.index)
            pt = dir_p + descr_n + '_' + user_n + '.csv'
            if fs:
                with fs.open(pt, 'w') as f:
                    descr.to_csv(f, index=False)
                fs.du(pt)
            else:
                descr.to_csv(pt, index=False)


def chunkwise_processing(all_user, tweet_fps, tz, dir_p, chunksize=100, fs=None):
    '''

    :param dict all_user: dict with {'user_class': ['user1', 'user2'...]} structure
    :param list tweet_fps: list of filepaths to tweetfiles
    :param str tz: string of datetime timezome
    :param str dir_p: directory to store csv output
    :param int chunksize: number of tweetfiles in one chunk
    :param gcsfs.GCSFileSystem fs: access to google cloud storeage
    '''

    res = get_descriptives_chunkwise(all_user, tweet_fps, tz, chunksize=chunksize, fs=fs)
    res = concat_all_descriptives(res)
    dump2csv(res, dir_p, tz, fs)
    print('Chunkwise processing completed.')
