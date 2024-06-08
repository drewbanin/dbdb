from dataclasses import dataclass
from typing import Optional

import enum
import inspect


AGG_FUNCTION_INCOMPLETE = object()


class FunctionTypes(enum.Enum):
    SCALAR = enum.auto()
    AGGREGATE = enum.auto()
    TABLE = enum.auto()
    WINDOW = enum.auto()


class BaseFunction:
    NAMES = []
    TYPE = None

    def get_name(self):
        raise NotImplementedError()

    def eval(self, row):
        raise NotImplementedError()


class ScalarFunction(BaseFunction):
    NAMES = []
    TYPE = FunctionTypes.SCALAR


class AggregateFunction:
    NAMES = []
    TYPE = FunctionTypes.AGGREGATE

    def __init__(self, modifiers=None):
        self.accum = None
        self.modifiers = modifiers or dict()

    def start(self):
        self.accum = None

    def eval(self, row):
        raise NotImplementedError()


class TableFunction:
    NAMES = []
    TYPE = FunctionTypes.TABLE

    def generate(self):
        raise NotImplementedError()

    def details(self):
        return {}


def is_subclass_of(value, base_class):
    return inspect.isclass(value) and issubclass(value, base_class)


def list_scalar_functions():
    from dbdb.expressions.functions import scalar_functions

    func_map = {}
    for classname in dir(scalar_functions):
        klass = getattr(scalar_functions, classname)
        if is_subclass_of(klass, ScalarFunction):
            for name in klass.NAMES:
                func_map[name.upper()] = klass

    return func_map


def list_aggregate_functions():
    from dbdb.expressions.functions import aggregate_functions

    func_map = {}
    for classname in dir(aggregate_functions):
        klass = getattr(aggregate_functions, classname)
        if is_subclass_of(klass, AggregateFunction):
            for name in klass.NAMES:
                func_map[name.upper()] = klass

    return func_map


def list_table_functions():
    from dbdb.expressions.functions import table_functions

    func_map = {}
    for classname in dir(table_functions):
        klass = getattr(table_functions, classname)
        if is_subclass_of(klass, TableFunction):
            for name in klass.NAMES:
                func_map[name.upper()] = klass

    return func_map
