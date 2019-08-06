import logging
import time

from dramatiq.brokers.redis import RedisBroker
import dramatiq

import cache
import middleware
import redis_client


logger = logging.getLogger(__name__)


broker = RedisBroker(
    host=redis_client.REDIS_HOST,
    port=redis_client.REDIS_PORT,
    db=redis_client.REDIS_DB
)
dramatiq.set_broker(broker)

cache_backend = cache.CacheBackend(client=redis_client.client)
results_middleware = dramatiq.results.Results(backend=cache_backend)
broker.add_middleware(results_middleware)
progress_middleware = middleware.ProgressMiddleware(cache.client)
broker.add_middleware(progress_middleware)


@dramatiq.actor(actor_class=cache.CachedActor, store_results=True)
def adder(a, b):
    answer = a + b
    logger.info(f'{a} + {b} = {answer}')
    return answer


@dramatiq.actor(exclusive=True)
def exclusive_actor():
    logger.info('start')
    time.sleep(1)
    logger.info('stop')

