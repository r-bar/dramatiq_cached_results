import datetime as dt
import logging
import time

from dramatiq.brokers.redis import RedisBroker
import dramatiq

import cache
import middleware
import redis_client
import mapper


logger = logging.getLogger(__name__)


broker = RedisBroker(
    host=redis_client.REDIS_HOST,
    port=redis_client.REDIS_PORT,
    db=redis_client.REDIS_DB
)


class Backend(
        middleware.TypedResultsBackendMixin,
        cache.CacheBackendMixin,
        dramatiq.results.backends.redis.RedisBackend
):
    pass


def initialize_broker(broker, results_backend=None):
    dramatiq.set_broker(broker)
    dramatiq.set_encoder(middleware.TypedEncoder())

    if results_backend:
        results_middleware = dramatiq.results.Results(backend=results_backend)
        broker.add_middleware(results_middleware)
    progress_middleware = middleware.ProgressMiddleware(cache.client)
    broker.add_middleware(progress_middleware)


cache_backend = Backend(client=redis_client.client)
initialize_broker(broker, cache_backend)


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


@dramatiq.actor(store_results=True)
@mapper.bind_schema
def now() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)
