import redis
import orjson
from datetime import datetime
import pytz
import logging

class DB():
    def __init__(self):
        pass

    def load(self, tweets):
        pass

class RedisDB(DB):
    def __init__(self, host, port):
        self.client = redis.Redis(host=host, port=port, db=0)

    def load(self, tweets):
        for t in tweets:
            self.client.set(t['id_str'], orjson.dumps(t))

    def get_tweets(self, tz):
        for i,k in enumerate(self.client.scan_iter()):
            if i % 10000 == 0:
                logging.info(f'Tweet: {i}')
            d = orjson.loads(self.client.get(k))

            # some hoops to get same offset as other localized times
            created = (datetime.fromisoformat(d['created_at']).
                       replace(tzinfo = None))
            d['created_at'] = pytz.timezone(tz).localize(created)

            yield d
