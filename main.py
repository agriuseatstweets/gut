# this is just a dummy so far
import numpy as np
from load_tweets import *
from get_descriptives import *

def chunkwise_tweet_loader





def timezone():
    return 'Asia/Kolkata'

tweets_dir = 'sample_data'
output_dir = 'sample_output'
ext = '.txt'
tweet_df = load_tweets(tweets_dir, timezone(), ext)
np.random.seed(40)
media_outlets = np.random.choice(np.unique(tweet_df['user.screen_name']), 20, replace=False)
parties = np.random.choice(np.unique(tweet_df['user.screen_name']), 20, replace=False)
candidates = np.random.choice(np.unique(tweet_df['user.screen_name']), 20, replace=False)
all_user = {
    'media_outlets': media_outlets,
    'parties': parties,
    'candidates': candidates
}

r = get_descriptives(tweet_df, all_user)

dump_pickle('sample_output/stuff.pickle', r)