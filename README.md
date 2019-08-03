# dramatiq_cached_results
This is a simple POC to see if it is possible to use dramatiq's `Results`
middleware to cache based on arguments instead of generating unique keys per
message. The new caching middleware is in the `cache` module. `app.py` is a
simple test to see if it works.

## Running
```
docker-compose build
docker-compose run app
```
