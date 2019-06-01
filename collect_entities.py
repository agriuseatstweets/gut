

# Should not actually be necessary to look here:
# entities (combined from text, extended text, retweeted status, and quoted_status)

# entitites (for user mentions)

# Definitely should be taken, in this order, from:
# retweeted_status.extended_tweet.entites, retweeted_status.entities, extended_tweet.entites, entities

# Also potentially taken from:
# quoted_text.entities




# A retweet of a quote is a quote, and should be considered a quote of a candidate

# A quote of a retweet should really just be a quote of the original tweet... the previous retweeter should dissapear as in multiple retweets...

from copy import copy


def dict_conjer(a, b = None):
    """ Takes two dictionary with list values and conjs values together"""
    b = b or {}

    union_keys = set(a.keys()) | set(b.keys())
    return {k: a.get(k, []) + b.get(k, []) for k in union_keys}

def str_conjer(a, b = None):
    b = b or ''
    return (a + ' ' + b).strip()


def _deep_extension_getter(tweet, status, key, key_ext):
    try:
        return tweet[status]['extended_tweet'][key_ext]
    except KeyError:
        return tweet[status][key]


def _replace(key, key_ext, tweet):
    entities = tweet[key]

    if tweet.get('retweeted_status'):
        entities = _deep_extension_getter(tweet, 'retweeted_status', key, key_ext)

    elif tweet.get('extended_tweet'):
        entities = tweet['extended_tweet'][key_ext]

    tweet[key] = entities
    return tweet

def _collect(key, key_ext, new_key, conjer, tweet):
    extras = None

    if tweet.get('quoted_status'):
        extras = _deep_extension_getter(tweet, 'quoted_status', key, key_ext)

    tweet[new_key] = conjer(tweet[key], extras)
    return tweet

def collect_entities(tweet):
    tweet = copy(tweet)
    tweet =  _replace('entities', 'entities', tweet)
    tweet = _collect('entities', 'entities', 'entities', dict_conjer, tweet)
    return tweet


def collect_text(tweet):
    tweet = copy(tweet)
    tweet =  _replace('text', 'full_text', tweet)
    tweet = _collect('text', 'full_text', 'collected_text', str_conjer, tweet)
    return tweet
