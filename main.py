# this is just a dummy so far
import numpy as np
from load_tweets import *
from get_descriptives import *

def timezone():
    return 'Asia/Kolkata'

tweets_dir = 'sample_data'
output_dir = 'sample_output'
ext = '.txt'
tweet_df = load_tweets(tweets_dir, timezone(), ext)
np.random.seed(40)
media_outlets = np.random.choice(np.unique(tweet_df['user.screen_name']), 50, replace=False)
parties = np.random.choice(np.unique(tweet_df['user.screen_name']), 50, replace=False)
candidates = np.random.choice(np.unique(tweet_df['user.screen_name']), 50, replace=False)
get_descriptives(tweet_df, media_outlets, parties, candidates, output_dir)