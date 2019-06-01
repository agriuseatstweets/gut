# Filtering
from sheets import get_user_groups, get_keywords, get_additional_terms

def text_relevant(tweet, terms):
    txt = tweet['collected_text'].lower()
    return any([t in txt for t in terms])

def user_relevant(tweet, users):
    if None in users:
        raise Exception('Inappropriate users list! Cannot contain None.')

    keys = ['user.screen_name',
            'retweeted_status.user.screen_name',
            'quoted_status.user.screen_name']

    return any([tweet.get(k) in users for k in keys])

def _get_users():
    ugs = get_user_groups()
    users = ugs['party'] + ugs['political_candidate']
    return users

def _get_terms():
    terms = get_additional_terms() + get_keywords()
    return [t.strip().lower() for t in terms]

def only_tweets_of_interest(tweets):
    users, terms = _get_users(), _get_terms()
    is_relevant = lambda t: text_relevant(t, terms) or user_relevant(t, users)
    return (t for t in tweets if is_relevant(t, users, terms))
