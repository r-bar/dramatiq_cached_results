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
results_backend = cache.CacheBackend(client=client, broker=broker)
broker.add_middleware(dramatiq.results.Results(backend=results_backend))
dramatiq.set_broker(broker)


def clean_arg(arg):
    """Recussivly attempt to clean the argument to make the """
    if isinstance(arg, (dt.date, dt.datetime)):
        return arg.isoformat()
    if isinstance(arg, str):
        return arg
    # this block will also handle defaultdicts and OrderedDicts
    if isinstance(arg, dict):
        output = collections.OrderedDict()
        for k, v in sorted(arg.items()):
            output[k] = clean_arg(arg)
        return output
    if isinstance(arg, typing.Iterable):
        return sorted(iter(arg))
    return arg


@dramatiq.actor(store_results=True)
def adder(a, b):
    answer = a + b
    logger.info(f'{a} + {b} = {answer}')
    return answer


def main():
    try:
        adder.message(1, 2).get_result()
    except dramatiq.results.ResultMissing:
        print('result not there')
    print('sending message')
    adder.send(1, 2)
    assert adder.message(1, 2).get_result() == 3
    print('got the correct answer!')


if __name__ == '__main__':
    main()
