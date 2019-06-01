from collect_entities import *


quoted_tweet = {
    "truncated": True,
    "user" : { "screen_name": "max" },
    "text" : "foo",
    "entities" : {"hashtags" : [{"text": "foo"}]},
    "extended_tweet" : {
        "full_text" : "foobar",
        "entities" : { "hashtags" : [{"text" : "foo"}, {"text" : "bar"}]}
    },
    "quoted_status" : {
        "text": "bar",
        "user" : { "screen_name" : "matt", "nope" : "gone" },
        "created_at" : "Mon Apr 17 13:53:38 +0000 2017",
        "id_str" : "123",
        "entities" : {"hashtags" : [{"text": "foo"}]},
    }
}


quoted_tweet_ext = {
    "truncated": True,
    "user" : { "screen_name": "max" },
    "text" : "foo",
    "entities" : {"hashtags" : [{"text": "foo"}]},
    "extended_tweet" : {
        "full_text" : "foobar",
        "entities" : { "hashtags" : [{"text" : "foo"}, {"text" : "bar"}]}
    },
    "quoted_status" : {
        "user" : { "screen_name" : "matt", "nope" : "gone" },
        "created_at" : "Mon Apr 17 13:53:38 +0000 2017",
        "truncated" : True,
        "id_str" : "123",
        "entities" : {"hashtags" : [{"text": "foo"}]},
        "extended_tweet" : {
            "full_text" : "foobarbaz",
            "entities" : { "hashtags" : [{"text" : "foo"}, {"text" : "bar"}, {"text" : "qux"}]}
        }
    }
}

retweet_ext = {
    "user" : {},
    "truncated" : True,
    "text" : "foo",
    "entities" : {"hashtags" : [{"text": "foo"}]},
    "extended_tweet" : {
        "full_text" : "foobar",
        "entities" : { "hashtags" : [{"text" : "foo"}, {"text" : "bar"}]}
    },
    "retweeted_status" : {
        "user" : { "screen_name" : "matt", "nope" : "gone" },
        "truncated" : True,
        "created_at" : "Mon Apr 17 13:53:38 +0000 2017",
        "id_str" : "123",
        "entities" : {"hashtags" : [{"text": "foo"}]},
        "extended_tweet" : {
            "full_text" : "foobarbaz",
            "entities" : { "hashtags" : [{"text" : "foo"}, {"text" : "bar"}, {"text" : "qux"}]}
        }
    }
}

retweet = {
    "user" : {},
    "truncated" : True,
    "text" : "foo",
    "entities" : {"hashtags" : [{"text": "foo"}]},
    "retweeted_status" : {
        "user" : { "screen_name" : "matt", "nope" : "gone" },
        "truncated" : True,
        "created_at" : "Mon Apr 17 13:53:38 +0000 2017",
        "id_str" : "123",
        "text": "foobar",
        "entities" : {"hashtags" : [{"text": "foo"}, {"text": "bar"}]}
    }
}

ext = {
    "user" : {},
    "truncated" : True,
    "text" : "foo",
    "entities" : {"hashtags" : [{"text": "foo"}]},
    "extended_tweet" : {
        "full_text" : "foobar",
        "entities" : { "hashtags" : [{"text" : "foo"}, {"text" : "bar"}]}
    }
}

def test_collect_entities_qt():
    rt = collect_entities(quoted_tweet)
    assert(len(rt['entities']['hashtags']) == 3)

def test_collect_entities_extended_qt():
    rt = collect_entities(quoted_tweet_ext)
    assert(len(rt['entities']['hashtags']) == 5)

def test_collect_entities_extended_rt():
    rt = collect_entities(retweet_ext)
    assert(len(rt['entities']['hashtags']) == 3)

def test_collect_entities_rt():
    rt = collect_entities(retweet)
    assert(len(rt['entities']['hashtags']) == 2)

def test_collect_entities_rt():
    rt = collect_entities(ext)
    assert(len(rt['entities']['hashtags']) == 2)

def test_collect_text_extended_qt():
    rt = collect_text(quoted_tweet_ext)
    assert(rt['collected_text'] == 'foobar foobarbaz')

def test_collect_text_qt():
    rt = collect_text(quoted_tweet)
    assert(rt['collected_text'] == 'foobar bar')

def test_collect_text_ext():
    rt = collect_text(ext)
    assert(rt['text'] == 'foobar')

def test_collect_text_rt():
    rt = collect_text(retweet)
    assert(rt['text'] == 'foobar')

def test_collect_text_extended_rt():
    rt = collect_text(retweet_ext)
    assert(rt['text'] == 'foobarbaz')

def test_dict_conjer_all_present():
    a = { 'foo': [1,2], 'bar': [2,3] }
    b = { 'foo': [2,3], 'bar': [3,4] }
    c = dict_conjer(a, b)
    assert(c['foo'] == [1,2,2,3])
    assert(c['bar'] == [2,3,3,4])

def test_dict_conjer_when_missing():
    a = { 'foo': [1,2], }
    b = { 'bar': [3,4] }
    c = dict_conjer(a, b)
    assert(c['foo'] == [1,2])
    assert(c['bar'] == [3,4])
