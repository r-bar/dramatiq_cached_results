import functools as fn
import datetime as dt
import time
import inspect
import threading
from contextlib import suppress
from typing import (
    Any,
    Optional,
)

import dramatiq

import cache
import mapper


TEN_MINS_IN_MS = 600 * 1000


class AlreadyQueued(dramatiq.middleware.SkipMessage):
    pass


class ProgressMiddleware(dramatiq.Middleware):
    """Middleware allows making queuing messaged exclusive. Setting the
    `exclusive option on the message will cause dramatiq to check that a
    matching message has not already been enqueued. If a matching message is
    found an AlreadyQueued error will be raised.

    Example:
    ```
    import time
    import redis
    import dramatiq

    @dramatiq.actor(exclusive=True, max_age=15 * 1000)
    def myactor():
        print('started')
        time.sleep(10)
        print('done!')

    client = redis.Redis()
    broker = dramatiq.brokers.RedisBroker()
    broker.add_middleware(ProgressMiddleware(client))

    myactor.send()
    myactor.send()  # raises AlreadyQueued!
    time.sleep(11)
    myactor.send()  # previous run should have completed, runs again
    ```
    """

    def __init__(self, client, key_prefix='actor_progress',
                 progress_timeout=TEN_MINS_IN_MS):
        self.client = client
        self.key_prefix = key_prefix
        self.progress_timeout = progress_timeout

    @property
    def actor_options(self):
        return {'exclusive'}

    def build_message_key(self, message):
        broker = dramatiq.get_broker()
        actor = broker.get_actor(message.actor_name)
        signature = inspect.signature(actor)
        bound_args = signature.bind(*message.args, **message.kwargs)
        message_key = cache.signature_key(bound_args, message.actor_name)
        return f'{self.key_prefix}:{message_key}'

    def before_enqueue(self, broker, message, delay):
        enqueue_time = time.time()
        message_key = self.build_message_key(message)

        def check_key(pipe):
            started_at = self.client.get(message_key)
            started_at = None if started_at is None else float(started_at)
            if started_at is not None and \
                    enqueue_time < started_at + progress_timeout:
                started_datetime = dt.datetime.fromtimestamp(started_at)
                raise AlreadyQueued(
                    'Matching message already queued at '
                    f'{started_datetime}: {message.asdict()}'
                )
            else:
                self.client.set(message_key, enqueue_time)

        if message.options.get('exclusive', False):
            progress_timeout = message.options.get('max_age',
                                                   self.progress_timeout)
            self.client.transaction(check_key, message_key)

    def before_ack(self, broker, message, *, result=None, exception=None):

        if message.options.get('exclusive', False):
            message_key = self.build_message_key(message)
            # self.client.transaction(lambda pipe: pipe.delete(message_key),
            #                         message_key)
            self.client.delete(message_key)


@fn.lru_cache(maxsize=100)
def _get_actor_schema(actor_name: str) -> Optional[mapper.BoundSchema]:
    with suppress(AttributeError):
        broker = dramatiq.get_broker()
        actor = broker.get_actor(actor_name)
        return actor.fn.schema


def _get_message_schema(message_data: Any):
    # Despite the type annotations on encode and decode  we can be passed
    # non-MessageData.  Seems to be most common in the Results middleware.
    with suppress(TypeError, AttributeError):
        return _get_actor_schema(message_data.actor_name)


class TypedEncoderMixin:

    def encode(self, data: dramatiq.encoder.MessageData) -> bytes:
        schema = _get_message_schema(data)
        if schema is None:
            return super().encode(data)

        output = {**data}
        args, kwargs = schema.serialize_arguments(*data['args'],
                                                  **data['kwargs'])
        output['args'] = args
        output['kwargs'] = kwargs
        return super().encode(output)

    def decode(self, data: bytes) -> dramatiq.encoder.MessageData:
        output = super().decode(data)
        schema = _get_message_schema(data)
        if schema is None:
            return output
        args, kwargs = schema.deserialize_arguments(*data['args'],
                                                    **data['kwargs'])
        output['args'] = args
        output['kwargs'] = kwargs
        return output


class TypedResultsBackendMixin:
    """Serializes and deserializes the results from a typed actor"""

    def store_result(self, message, result: Any, ttl: int):
        schema = _get_message_schema(message)
        if schema is not None:
            result = schema.serialize_result(result)
        super().store_result(message, result, ttl=ttl)

    def get_result(self, message, *, block=False, timeout=None) -> Any:
        result = super().get_result(message, block=block, timeout=timeout)
        schema = _get_message_schema(message)
        if schema is not None:
            result = schema.deserialize_result(result)
        return result


class TypedEncoder(TypedEncoderMixin, dramatiq.JSONEncoder):
    pass
