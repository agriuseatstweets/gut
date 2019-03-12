import pytest
import pandas as pd
from chunkwise_processing import *


def test_concat_follower_counts():
    follower_counts = [
        pd.DataFrame({
            'user.screen_name': ['a', 'b'],
            'user.followers_count': [10, 20],
            'user.friends_count': [10, 20],
            'created_at': [1, 0]
        }),
        pd.DataFrame({
            'user.screen_name': ['a', 'b'],
            'user.followers_count': [100, 200],
            'user.friends_count': [100, 200],
            'created_at': [0, 1]
        })
    ]
    target_follower_counts_concat = pd.DataFrame(
        {
            'user.screen_name': ['a', 'b'],
            'user.followers_count': [10, 200],
            'user.friends_count': [10, 200],
            'created_at': [1, 1]
        },
        index=['a', 'b']
    )
    assert concat_follower_counts(follower_counts).to_dict() == target_follower_counts_concat.to_dict()


def test_concat_engagement_counts():
    engagement_counts = [
        pd.DataFrame({'retweet_count': [1, 2, 3]}, index=['a', 'b', 'c']),
        pd.DataFrame({'retweet_count': [4, 5, 6]}, index=['a', 'b', 'c'])
    ]
    target_concat = pd.DataFrame({'retweet_count': [5, 7, 9]}, index=['a', 'b', 'c'])
    assert concat_engagement_counts(engagement_counts).to_dict() == target_concat.to_dict()


def test_concat_edges_weights():
    edges_weights = [
        pd.DataFrame({
            'user1': ['a', 'a', 'b'],
            'user2': ['b', 'c', 'c'],
            'weight_mentions': [10, 20, 30]
        }),
        pd.DataFrame({
            'user1': ['a', 'a', 'b'],
            'user2': ['b', 'c', 'c'],
            'weight_mentions': [1, 2, 3]
        })
    ]
    target_concat = {'weight_mentions': {
        ('a', 'b'): 11,
        ('a', 'c'): 22,
        ('b', 'c'): 33}
    }
    assert concat_edges_weights(edges_weights).to_dict() == target_concat
