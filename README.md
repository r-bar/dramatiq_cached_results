# dramatiq_cached_results
This is a simple POC to see if it is possible to use dramatiq's results
middleware to cache based on arguments instead of generating unique keys per
message. The new caching middleware is in the `cache` module. `app.py` is a
simple test to see if it works.

## Running

```
docker-compose build
docker-compose run app
```

## Known Issues

#### Cannot use actor pipelines
Pipelines work by fetching the result based on last message ID. The current
CacheResultBackend generated the message_key (the cache key) by inspecting the
actor's signature, binding the message arguments, and hashing the result. You
cannot `signature.bind()` partial arguments.

It may be possible to modify the `pipeline` class to add another message option
to each pipeline message containing the cumulative hash of the messages before
it. Given the same set of inputs this should always generate the same keys for
each step. The results would have to be written out twice in this case. Once to
the "pipeline key" and once to the key generated by the fully applied arguments
as usual.
