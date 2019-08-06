import datetime as dt
import time
import inspect

import dramatiq

import cache


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
