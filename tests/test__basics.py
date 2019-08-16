import datetime as dt
import logging
import time

import dramatiq
import pytest

import actors
import cache
import middleware


@pytest.fixture
def broker():
    actors.broker.client.flushdb()
    return actors.broker


@pytest.fixture
def stub_broker():
    from dramatiq.brokers.stub import StubBroker
    broker = StubBroker()
    actors.initialize_broker(broker)
    yield broker


@pytest.fixture
def stub_worker():
    broker = dramatiq.get_broker()
    worker = dramatiq.Worker(broker, worker_timeout=1000)
    worker.start()
    try:
        yield worker
    finally:
        worker.stop()


def test_basic_happy_path(broker, stub_worker):
    with pytest.raises(dramatiq.results.ResultMissing):
        actors.adder.message(1, 2).get_result()

    direct_result = actors.adder(1, 2)

    message1 = actors.adder.send(1, 2)
    message2 = actors.adder.message(1, 2)

    assert message1.message_id != message2.message_id

    result1 = message1.get_result(block=True)
    result2 = message2.get_result(backend=actors.cache_backend)

    assert result2 == direct_result, \
        'cached result should match the direct computed result'
    assert result1 == result2, \
        'calculated and queued results should match'


def test_should_not_cache_partial_messages():
    with pytest.raises(TypeError):
        actors.cache_backend.build_message_key(actors.adder(1))


def test_pipelines_can_be_used(broker, stub_worker):
    pipe = cache.pipeline([
        actors.adder.message(3, 4),
        actors.adder.message(3),
    ])
    pipe.run()
    result = pipe.get_result(block=True)
    assert result == 10


def test_pipelines_are_cachable(broker, stub_worker):
    pipe = cache.pipeline([
        actors.adder.message(3, 1),
        actors.adder.message(6),
    ])
    pipe.run()
    assert pipe.get_result(block=True) == 10

    pipe2 = cache.pipeline([
        actors.adder.message(3, 1),
        actors.adder.message(6),
    ])
    result = pipe2.get_result()

    assert result == 10, \
        'should get the result of a identical pipeline without running it'
    assert actors.adder.message(6, 4).get_result() == 10, \
        'intermediate results should be cached'


def test_should_cache_direct_call_result(broker, stub_worker):
    result = actors.adder(1, 2)
    cached_result = actors.adder.message(1, 2).get_result()
    assert result == cached_result


def test_progress_middleware(broker, stub_worker, caplog):
    actors.exclusive_actor.send_with_options(exclusive=True)
    with pytest.raises(middleware.AlreadyQueued):
        actors.exclusive_actor.send_with_options(exclusive=True)
    # messages queued non exclusively should not raise
    actors.exclusive_actor.send_with_options(exclusive=False)
    stub_worker.join()
    time.sleep(4)
    actors.exclusive_actor.send_with_options(exclusive=True)


def test_typed_backend():
    message = actors.now.message()
    result = actors.now()
    actors.cache_backend.store_result(message, result, ttl=10000)

    cached = message.get_result()
    assert isinstance(cached, dt.datetime)
    assert cached == result


def test_cache_datetime(broker, stub_worker):
    message1 = actors.now.message()
    with pytest.raises(dramatiq.results.ResultMissing):
        message1.get_result()
    broker.enqueue(message1)
    stub_worker.join()
    result = message1.get_result(block=True, timeout=1000)
    assert isinstance(result, dt.datetime)
