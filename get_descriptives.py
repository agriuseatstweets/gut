import pandas as pd
from load_tweets import *


def get_follower_count(df, user):
    df = df[['user.screen_name', 'user.followers_count', 'user.friends_count', 'created_at']]
    df = df.loc[df['user.screen_name'].isin(user)]
    df = df.loc[df.groupby('user.screen_name').created_at.idxmax()]
    df = df.sort_values('user.followers_count', ascending=False)
    return df

