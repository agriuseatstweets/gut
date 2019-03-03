# this is just a dummy so far
from load_tweets import *
from get_descriptives import *

def timezone():
    return 'Asia/Kolkata'

tweets_dir = 'sample_data'
output_dir = 'sample_output'
ext = '.txt'
tweet_df = load_tweets(tweets_dir, timezone(), ext)