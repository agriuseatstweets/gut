from .filter_tweets import *

def test_user_relevant():
    tweet = {
        'user.screen_name': 'foo',
        'in_reply_to_screen_name': 'qux',
    }
    assert(user_relevant(tweet, ['foo', 'qux']))
    assert(user_relevant(tweet, ['foo', 'bar']))
    assert(user_relevant(tweet, ['qux', 'bar']))
    assert(not user_relevant(tweet, ['bar', 'baz']))


def test_text_relevant():
    tweet = {
        'collected_text': 'mymodiman is the man'
    }
    assert(text_relevant(tweet, ['modi', 'foo']))
    assert(not user_relevant(tweet, ['bar', 'foo']))
    assert(not user_relevant(tweet, ['modifoo', 'foo']))
