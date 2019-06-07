from preprocessing.loading import *
from utils import *
import logging, os
import gcsfs
from joblib import Parallel, delayed
from tqdm import tqdm
from db import RedisDB
from math import ceil

logging.basicConfig(level = logging.INFO)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID')
BELLY_LOCATION = os.getenv('BELLY_LOCATION')
CHUNK_SIZE = int(os.getenv('GUT_CHUNK_SIZE'))
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))

def load_tweets(fps, fs, tz):
    db = RedisDB(REDIS_HOST, REDIS_PORT)
    tweets = get_tweets(fps, fs, tz)
    db.load(tweets)

def process(limit=None, mode='parallel'):
    fs = gcsfs.GCSFileSystem(project=GOOGLE_PROJECT_ID,
                             token=GOOGLE_APPLICATION_CREDENTIALS,
                             access='read_write')
    fps = sorted(fs.ls(BELLY_LOCATION))

    tz = 'Asia/Kolkata'
    fps = filter_start_time(fps, tz)

    if limit:
        fps = fps[:int(limit)]

    logging.info(f'Processing {len(fps)} files')

    if mode == 'parallel':
        Parallel(n_jobs=-1)(delayed(load_tweets)(list(f), fs, tz)
                            for f in tqdm(chunk(CHUNK_SIZE, fps),
                                          total=ceil(len(fps)/CHUNK_SIZE)))
    else:
        load_tweets(fps, fs, tz)

    logging.info(f'Loaded all tweets')


from clize import run
if __name__ == '__main__':
    run(process)
