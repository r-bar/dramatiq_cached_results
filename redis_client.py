import os

import redis


REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))


client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
