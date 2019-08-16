import datetime as dt
from collections import deque

from marshmallow import fields
import marshmallow
import pytest

import mapper


@pytest.fixture(scope='module')
def default_mapper():
    return mapper.FieldMapper()


def test_is_core_type():
    assert mapper.FieldMapper().is_core_type(str)


def test_core_types_field_mapper(default_mapper):
    assert isinstance(default_mapper.to_field(int), fields.Integer)
    assert isinstance(default_mapper.to_field(float), fields.Float)
    assert isinstance(default_mapper.to_field(str), fields.String)
    assert isinstance(default_mapper.to_field(dt.datetime), fields.DateTime)
    assert isinstance(default_mapper.to_field(bool), fields.Bool)

    mapped_dict = default_mapper.to_field(dict)
    assert isinstance(mapped_dict, fields.Dict)
    assert mapped_dict.key_field.__class__ is fields.Raw
    assert mapped_dict.value_field.__class__ is fields.Raw

    mapped_list = default_mapper.to_field(list)
    assert isinstance(mapped_list, fields.List)
    assert mapped_list.inner.__class__ is fields.Raw

    mapped_deque = default_mapper.to_field(deque)
    assert isinstance(mapped_deque, fields.List)
    assert mapped_deque.inner.__class__ is fields.Raw

    # use a list because the fields.Tuple implementation requires declaring the
    # types of all inner items
    mapped_tuple = default_mapper.to_field(tuple)
    assert isinstance(mapped_tuple, fields.List)
    assert mapped_deque.inner.__class__ is fields.Raw


@pytest.mark.parametrize('field_type', (
    fields.Boolean,
    fields.Date,
    fields.DateTime,
    fields.Dict,
    fields.Float,
    fields.Integer,
    fields.String,
))
def test_field_types_field_mapper(default_mapper, field_type):
    assert isinstance(default_mapper.to_field(field_type), field_type)


@pytest.fixture(scope='module')
def kwargs_fn():
    @mapper.bind_schema
    def fn(a: str = 'a', b: int = 1) -> str:
        return a * b
    return fn


@pytest.fixture(scope='module')
def args_fn():
    @mapper.bind_schema
    def fn(a: str, b: int) -> str:
        return a * b
    return fn


@pytest.fixture(scope='module')
def mixed_fn():
    @mapper.bind_schema
    def fn(a: str, b: int = 1) -> str:
        return a * b
    return fn


@pytest.fixture(scope='module')
def star_fn():
    @mapper.bind_schema
    def fn(*args, **kwargs):
        return args, kwargs
    return fn


def test_bind_args(args_fn):
    assert isinstance(args_fn.schema.args['a'], fields.String)
    assert isinstance(args_fn.schema.args['b'], fields.Integer)
    assert isinstance(args_fn.schema.result, fields.String)

    ser_args, ser_kwargs = args_fn.schema.serialize_arguments(a='a', b=2)
    assert not ser_args
    assert ser_kwargs == {'a': 'a', 'b': 2}

    ser_args, ser_kwargs = args_fn.schema.serialize_arguments('a', 2)
    assert not ser_args
    assert ser_kwargs == {'a': 'a', 'b': 2}

    assert args_fn('foo', 2) == 'foofoo', 'function should still be callable'


def test_bind_kwargs(kwargs_fn):
    assert isinstance(kwargs_fn.schema.args['a'], fields.String)
    assert isinstance(kwargs_fn.schema.args['b'], fields.Integer)
    assert kwargs_fn.schema.args['b'].default == 1
    assert isinstance(kwargs_fn.schema.result, fields.String)

    ser_args, ser_kwargs = kwargs_fn.schema.serialize_arguments(a='a', b=2)
    assert not ser_args
    assert ser_kwargs == {'a': 'a', 'b': 2}

    ser_args, ser_kwargs = kwargs_fn.schema.serialize_arguments('a', b=3)
    assert not ser_args
    assert ser_kwargs == {'a': 'a', 'b': 3}

    assert kwargs_fn('foo', 3) == 'foofoofoo', \
        'function should still be callable'


def test_default_args_applied(kwargs_fn):
    _ser_args, ser_kwargs = kwargs_fn.schema.serialize_arguments()
    assert ser_kwargs == {'a': 'a', 'b': 1}


@pytest.fixture(scope='module')
def date_fn():
    @mapper.bind_schema
    def fn(date: dt.datetime) -> dt.datetime:
        return date + dt.timedelta(days=1)
    return fn


def test_bind_date_arg(date_fn):
    assert isinstance(date_fn.schema.args['date'], fields.DateTime)
    date = dt.datetime(2019, 1, 1, 13, 30)
    ser_args, ser_kwargs = date_fn.schema.serialize_arguments(date)
    assert ser_kwargs['date'] == '2019-01-01T13:30:00'

    de_args, de_kwargs = date_fn.schema.deserialize_arguments(*ser_args,
                                                              **ser_kwargs)
    de_date = de_kwargs['date']
    assert isinstance(de_date, dt.datetime)
    assert de_date.tzinfo is dt.timezone.utc, \
        'deserialized dates should be coerced to utc by default'
    ser_results = date_fn.schema.round_trip(*ser_args, **ser_kwargs)
    assert ser_results == '2019-01-02T13:30:00+00:00'


def test_bind_mixed_args(mixed_fn):
    assert mixed_fn('foo', 2) == 'foofoo', \
        'function should still be callable'


def test_bind_star_args(star_fn):
    assert star_fn('a', 'b', foo='bar') == (('a', 'b'), {'foo': 'bar'})
