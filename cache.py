from collections import deque
import functools as fn
import hashlib
import inspect
import json
import logging
from typing import (
    Callable,
    Iterable,
    Optional,
)

from dramatiq.results.backends import RedisBackend
import dramatiq

logger = logging.getLogger(__name__)


def generate_key(prefix: str, iterable: Iterable, *,
                 hashalg: str = 'md5',
                 sep: str = ':',
                 ignore_errors: bool = False) -> str:
    """Generate a cache key for use with Redis of the form:

        '<prefix><sep><hashed_args>'

    The prefix is left unhashed for readability and wildcard scanning
    of related keys in Redis.

    The bounded arguments are filled with defaults.  Arguments that are
    not JSON serializable need to be excluded or ``ignore_errors`` set
    to True.

    :param prefix: unhashed prefix of the cache key
    :param iterable: JSON serializable objects to use in the hash key
    :param hashalg: optional different hash algorithm to use
    :param sep: optional alternative separator to use after ``prefix``
    :param ignore_errors: whether to skip JSON serializing value errors

    """
    h = hashlib.new(hashalg)
    for obj in iterable:
        if not isinstance(obj, (bytes, str)):
            try:
                obj = json.dumps(obj, sort_keys=True)
            except ValueError:
                if not ignore_errors:
                    raise
        h.update(obj if isinstance(obj, bytes) else obj.encode())

    return f'{prefix}{sep}{h.hexdigest()}'


def signature_key(bound_args: inspect.BoundArguments, prefix: str,
                  only: Optional[Iterable] = None,
                  exclude: Optional[Iterable] = None,
                  **generate_kwargs) -> str:
    """Generate a cache key based on function arguments for use with
    Redis of the form:

        '<prefix><sep><hashed_args>'

    :param bound_args: function signature bounded arguments
    :param prefix: unhashed prefix of the cache key
    :param only: optional whitelist of arguments to include in cache key
    :param exclude: optional blacklist of arguments to exclude in cache key
    :param generate_kwargs: keyword arguments passed to ``generate_key``

    """
    only = None if only is None else set(only)
    exclude = None if exclude is None else set(exclude)

    bound_args.apply_defaults()
    objects = (
        obj
        for argname, value in bound_args.arguments.items()
        for obj in [argname, value]
        if (only is None or argname in only)
        and (exclude is None or argname not in exclude)
    )
    return generate_key(prefix, objects, **generate_kwargs)


# def memoized(func: Optional[Callable] = None, *,
#              ttl: int = 300,
#              prefix: Optional[str] = None,
#              only: Optional[Iterable[str]] = None,
#              exclude: Optional[Iterable[str]] = None,
#              ) -> Callable:
#     """Decorates ``func`` with a wrapper that caches the results of
#     ``func`` in Redis with a key based on the arguments passed.

#     :param prefix: cache key prefix that defaults to function name

#     """
#     only = None if only is None else set(only)
#     exclude = None if exclude is None else set(exclude)

#     def decorator(func: Callable):
#         nonlocal prefix
#         if prefix is None:
#             prefix = func.__name__
#         sig = inspect.signature(func)

#         @fn.wraps(func)
#         def wrapper(*args, **kwargs):
#             key = signature_key(
#                 sig.bind(*args, **kwargs), prefix,
#                 only=only, exclude=exclude,
#             )
#             result = client.get(key)
#             if result is None:
#                 result = func(*args, **kwargs)
#                 client.set(key, json.dumps(result), ex=ttl)
#             else:
#                 result = json.loads(result)
#             return result

#         return wrapper

#     if callable(func):
#         return decorator(func)
#     return decorator


class CacheBackendMixin:
    def __init__(self, *, broker=None, **kwargs):
        super().__init__(**kwargs)
        assert broker is not None, \
            'You must specify a broker to build the cache key'
        self.broker = broker

    def build_message_key(self, message) -> str:
        actor = self.broker.get_actor(message.actor_name)
        signature = inspect.signature(actor.fn)
        # even though .bind() will sort any declared keyword arguments, if the
        # actor.fn accepts any **kwargs then this ensures that those are sorted
        # in the final bound args as well
        kwargs = {k: v for k, v in sorted(message.kwargs.items())}
        try:
            bound_args = signature.bind(*message.args, **kwargs)
        except TypeError:
            raise TypeError('Cannot cache partial messages')
        message_key = signature_key(bound_args, message.actor_name)
        return message_key


class CacheBackend(CacheBackendMixin, RedisBackend):
    pass


# class pipeline(dramatiq.pipeline):
#     """If you use the cache pipeline mixin you cannot use the normal pipeline
#     mechanism due to not being able to inspect a 'partially applied'
#     function. This pipeline adds a pipeline_key to the message options as a
#     second target to save results"""

#     def __init__(self, children, *, broker=None):
#         self.broker = broker or dramatiq.get_broker()
#         self.messages = messages = []

#         for child in children:
#             if isinstance(child, pipeline):
#                 messages.extend(message.copy() for message in child.messages)
#             else:
#                 messages.append(child.copy())

#         window = deque([None, None], 3)
#         for message in messages:
#             window.append(message)
#             prev_msg, current_msg, next_msg = window
#             if current_msg is None:
#                 continue
#             if prev_msg:
#                 prev_msg.options['pipe_target'] = current_msg.asdict()
#             if next_msg:



#     pass
