from collections import deque, OrderedDict
import datetime as dt
import inspect
import typing as t
import uuid

from marshmallow import fields


STDTYPING_MAP = {
    'Dict': fields.Dict,
    'List': fields.List,
    'Set': fields.List,
    'Any': fields.Raw,
}


CORE_MAP = OrderedDict({
    bool: fields.Bool,
    dict: fields.Dict,
    list: fields.List,
    set: fields.List,
    # datetime muct come before date ecause datetime is a subclass of date
    dt.datetime: fields.AwareDateTime('iso', default_timezone=dt.timezone.utc),
    dt.date: fields.Date('iso'),
    dt.time: fields.Time,
    dt.timedelta: fields.TimeDelta(precision='seconds'),
    str: fields.String,
    int: fields.Int,
    float: fields.Float,
    # use a list because the fields.Tuple implementation requires declaring the
    # types of all inner items
    tuple: fields.List,
    # no upstream implementation of fields.Deque
    deque: fields.List,
    uuid.UUID: fields.UUID,
})


def bind_schema(function=None, *, mapper=None):
    def decorator(function):
        setattr(function, 'schema', BoundSchema(function, mapper=mapper))
        return function

    if function is None:
        return decorator
    else:
        return decorator(function)


class FieldMapper:
    __slots__ = ('stdtyping_map', 'core_map', 'fields_options')

    def __init__(self, stdtyping_map=None, core_map=None, **fields_options):
        """Declares a mapping from any annotation object to a marshmallow
        Field

        :param dict core_map: A mapping from any python class object to
            the desired marshmallow field class
        :param dict stdtyping_map: A mapping from python's `typing` module to
            the desired marhmallow field class
        :param field_options: Any extra arguments to pass to marshmallow fields
        """
        self.stdtyping_map = stdtyping_map or STDTYPING_MAP
        self.core_map = core_map or CORE_MAP
        self.fields_options = fields_options

    def is_core_type(self, typex):
        if isinstance(typex, type):
            return issubclass(typex, tuple(self.core_map.keys()))
        return False

    def core_to_field(self, core: type):
        for core_type, field_type in self.core_map.items():
            if issubclass(core, core_type):
                return field_type

        raise ValueError(f'{core} type is not supported')

    @staticmethod
    def is_stdtyping_type(typex):
        return isinstance(typex, t._GenericAlias)

    def stdtyping_to_field(self, stdtyping: t._GenericAlias, **kwargs):
        """Converts a stdtyping type into a field"""
        try:
            field_type = self.stdtyping_map[stdtyping._name]
        except KeyError:
            raise ValueError(f'{stdtyping._name} type is not supported')
        args = (self.to_field(t, **kwargs) for t in stdtyping.__args__)
        return field_type(*args, **kwargs)

    @staticmethod
    def is_fields_type(typex):
        return all((
            callable(getattr(typex, 'deserialize', None)),
            callable(getattr(typex, 'serialize', None)),
            callable(getattr(typex, '_validate', None)),
            isinstance(typex, type),
        ))

    @staticmethod
    def is_fields_instance(typex):
        return all((
            callable(getattr(typex, 'deserialize', None)),
            callable(getattr(typex, 'serialize', None)),
            callable(getattr(typex, '_validate', None)),
            not isinstance(typex, type),
        ))

    @staticmethod
    def is_schema(typex):
        return all((
            hasattr(typex, 'load'),
            hasattr(typex, 'dump'),
            not isinstance(typex, type),
        ))

    @staticmethod
    def _instantiate_field(field_cls, **kwargs):
        # since passing a python type / class does not let you specify
        # field paramaters fill any required field parameters with Any
        field_params = inspect.signature(field_cls).parameters
        field_args = (
            fields.Raw for arg in field_params.values()
            if arg.kind is inspect.Parameter.POSITIONAL_ONLY
            or arg.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        return field_cls(*field_args, **kwargs)

    def to_field(self, typex, **field_options):
        if self.is_schema(typex):
            field = fields.Nested(typex, **field_options)
        elif self.is_core_type(typex):
            field = self.core_to_field(typex)
        elif self.is_stdtyping_type(typex, **field_options):
            field = self.stdtyping_to_field(typex, **field_options)
        elif typex is inspect._empty:
            field = fields.Raw(**field_options)
        else:
            field = typex

        if self.is_fields_instance(field):
            return field
        elif self.is_fields_type(field):
            return self._instantiate_field(field, **field_options)

        raise ValueError(f'{typex} type is not supported')


class BoundSchema:
    # the "ser" and "de" prefixes used throughout this class stand for
    # serialize and deserialize respectively

    __slots__ = (
        'args',
        'fn',
        'mapper',
        'result',
        'signature',
    )

    def __init__(self, function: callable, mapper: FieldMapper = None):
        self.fn = function
        self.mapper = mapper or FieldMapper()
        self.signature = signature = inspect.signature(function)
        self.args = {}
        for name, param in signature.parameters.items():
            self.args[name] = self._parameter_to_field(param)
        self.result = self.mapper.to_field(signature.return_annotation)

    def _parameter_to_field(self, param):
        options = {}
        if param.default is not inspect._empty:
            options['default'] = param.default
        if param.default is None:
            options['allow_none'] = True
        return self.mapper.to_field(param.annotation, **options)

    def serialize_arguments(self, *de_args, **de_kwargs):
        bound = self.signature.bind(*de_args, **de_kwargs)
        bound.apply_defaults()
        ser_args = []
        ser_kwargs = {}
        for name, value in bound.arguments.items():
            ser_value = self.args[name].serialize(attr=name,
                                                  obj=bound.arguments)
            kind = self.signature.parameters[name].kind
            if kind is inspect.Parameter.POSITIONAL_ONLY:
                ser_args.append(ser_value)
            elif kind is inspect.Parameter.VAR_POSITIONAL:
                ser_args.extend(ser_value)
            elif kind is inspect.Parameter.VAR_KEYWORD:
                ser_kwargs.update(ser_value)
            else:
                ser_kwargs[name] = ser_value
        return ser_args, ser_kwargs

    def deserialize_arguments(self, *ser_args, **ser_kwargs):
        bound = self.signature.bind(*ser_args, **ser_kwargs)
        bound.apply_defaults()
        de_args = []
        de_kwargs = {}
        for name, value in bound.arguments.items():
            de_value = self.args[name].deserialize(value=value, attr=name,
                                                   obj=bound.arguments)
            kind = self.signature.parameters[name].kind
            if kind is inspect.Parameter.POSITIONAL_ONLY:
                de_args.append(de_value)
            elif kind is inspect.Parameter.VAR_POSITIONAL:
                de_args.extend(de_value)
            elif kind is inspect.Parameter.VAR_KEYWORD:
                de_kwargs.update(de_value)
            else:
                de_kwargs[name] = de_value
        return de_args, de_kwargs

    def validate_arguments(self, *args, **kwargs):
        bound = self.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        for name, value in bound.arguments.items():
            field = self.args[name]
            if callable(getattr(field, 'validate', None)):
                field.validate(value)
            elif callable(getattr(field, '_validate', None)):
                field._validate(value)

    def serialize_result(self, result):
        return self.result.serialize(attr='result', obj={'result': result})

    def deserialize_result(self, result):
        return self.result.deserialize(result, attr='result',
                                       data={'result': result})

    def validate_result(self, result):
        if callable(getattr(self.result, 'validate', None)):
            self.result.validate(result)
        elif callable(getattr(self.result, '_validate', None)):
            self.result._validate(result)

    def round_trip(self, *ser_args, **ser_kwargs):
        """Deserializes arguments, executes the bound function, and returns the
        serialized result"""
        de_args, de_kwargs = \
            self.deserialize_arguments(*ser_args, **ser_kwargs)
        de_result = self.fn(*de_args, **de_kwargs)
        return self.serialize_result(de_result)
