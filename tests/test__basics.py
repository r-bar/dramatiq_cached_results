import dramatiq
import pytest

import app
import cache


@pytest.fixture
def broker():
    app.broker.client.flushdb()
    return app.broker


@pytest.fixture
def stub_worker(broker):
    worker = dramatiq.Worker(broker, worker_timeout=1000)
    worker.start()
    yield worker
    worker.stop()


def test_basic_happy_path(stub_worker):
    with pytest.raises(dramatiq.results.ResultMissing):
        app.adder.message(1, 2).get_result()

    direct_result = app.adder(1, 2)

    message1 = app.adder.send(1, 2)
    message2 = app.adder.message(1, 2)

    assert message1.message_id != message2.message_id

    result1 = message1.get_result(block=True)
    result2 = message2.get_result(backend=app.cache_backend)

    assert result2 == direct_result, \
        'cached result should match the direct computed result'
    assert result1 == result2, \
        'calculated and queued results should match'


def test_should_not_cache_partial_messages():
    with pytest.raises(TypeError):
        app.cache_backend.build_message_key(app.adder(1))


def test_pipelines_can_be_used(stub_worker):
    pipe = cache.pipeline([
        app.adder.message(3, 4),
        app.adder.message(3),
    ])
    pipe.run()
    result = pipe.get_result(block=True)
    assert result == 10


def test_pipelines_are_cachable(stub_worker):
    pipe = cache.pipeline([
        app.adder.message(3, 1),
        app.adder.message(6),
    ])
    pipe.run()
    assert pipe.get_result(block=True) == 10

    pipe2 = cache.pipeline([
        app.adder.message(3, 1),
        app.adder.message(6),
    ])
    result = pipe2.get_result()

    assert result == 10, \
        'should get the result of a identical pipeline without running it'
    assert app.adder.message(6, 4).get_result() == 10, \
        'intermediate results should be cached'
