from sheets.sheets import get_user_groups, get_keywords, get_additional_terms

def text_relevant(tweet, terms):
    txt = tweet['collected_text'].lower()
    return any([t in txt for t in terms])

def user_relevant(tweet, users):
    if None in users:
        raise Exception('Inappropriate users list! Cannot contain None.')

    keys = ['user.screen_name',
            'in_reply_to_screen_name',
            'retweeted_status.user.screen_name',
            'quoted_status.user.screen_name']

    return any([tweet.get(k) in users for k in keys])

def _get_users(sheets):
    ugs = get_user_groups(sheets)
    users = ugs['party'] + ugs['political_candidate']
    return users

def _get_terms(sheets):
    terms = get_additional_terms(sheets) + get_keywords(sheets)
    return [t.strip().lower() for t in terms]

def only_tweets_of_interest(tweets, sheets):
    users, terms = _get_users(sheets), _get_terms(sheets)
    print(users)
    is_relevant = lambda t: text_relevant(t, terms) or user_relevant(t, users)
    return (t for t in tweets if is_relevant(t))
