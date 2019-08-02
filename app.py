import collections
import datetime as dt
import logging
import os
import typing

import dramatiq
from dramatiq.brokers.redis import RedisBroker
import redis

import cache

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

logger = logging.getLogger(__name__)
client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

broker = RedisBroker(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
dramatiq.set_broker(broker)

cache_backend = cache.CacheBackend(client=client)
broker.add_middleware(dramatiq.results.Results(backend=cache_backend))


@dramatiq.actor(store_results=True)
def adder(a, b):
    answer = a + b
    logger.info(f'{a} + {b} = {answer}')
    return answer


def main():
    # make sure stuff is cleaned up
    cache_keys = client.keys('adder:*')
    if cache_keys:
        client.delete(*cache_keys)
    broker.flush_all()

    try:
        adder.message(1, 2).get_result()
    except dramatiq.results.ResultMissing:
        print('result not there, as expected')

    print('sending first message')
    message1 = adder.send(1, 2)

    print('creating a second message with the same arguments')
    message2 = adder.message(1, 2)
    assert message1.message_id != message2.message_id

    print('getting the results...')
    result1 = message1.get_result(block=True)
    result2 = message2.get_result()

    assert result2 == 3 and result1 == result2
    print('got the correct answer! both messages match. results were'
          ' successfully fetched from the cache.')


if __name__ == '__main__':
    main()
