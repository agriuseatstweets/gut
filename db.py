import redis
import orjson
from datetime import datetime
import pytz
import logging

# from kafka import KafkaProducer, KafkaConsumer

# class KafkaDB(DB):
#     def __init__(self, servers):
#         self.servers = servers

#     def load(self, tweets):
#         producer = KafkaProducer(bootstrap_servers=self.servers)
#         for t in tweets:
#             producer.send('tweets', orjson.dumps(t))


#     def get_tweets(self):
#         consumer = KafkaConsumer('tweets', bootstrap_servers=self.servers, consumer_timeout_ms=500)
#         for msg in consumer:
#             yield orjson.loads(msg.value)


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
            d['created_at'] = (datetime
                               .fromisoformat(d['created_at'])
                               .replace(tzinfo = pytz.timezone(tz)))
            yield d
