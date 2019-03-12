



def timezone():
    return 'Asia/Kolkata'

tweets_dir = 'sample_data'
output_dir = 'sample_output'
ext = '.txt'
tweet_fps = list(get_abs_fps(tweets_dir, ext))
tweet_df = load_tweets(tweet_fps, timezone())


np.random.seed(40)
media_outlets = np.random.choice(np.unique(tweet_df['user.screen_name']), 20, replace=False)
parties = np.random.choice(np.unique(tweet_df['user.screen_name']), 20, replace=False)
candidates = np.random.choice(np.unique(tweet_df['user.screen_name']), 20, replace=False)
all_user = {
    'media_outlets': media_outlets,
    'parties': parties,
    'candidates': candidates
}
