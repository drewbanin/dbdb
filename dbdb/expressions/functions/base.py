from dbdb.tuples.context import ExecutionContext

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

    def eval(self, context: ExecutionContext):
        raise NotImplementedError()


class ScalarFunction(BaseFunction):
    NAMES = []
    TYPE = FunctionTypes.SCALAR

    def __init__(self, expr):
        self.expr = expr


class AggregateFunction:
    NAMES = []
    TYPE = FunctionTypes.AGGREGATE

    def __init__(self, expr, modifiers=None):
        self.accum = None
        self.expr = expr
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
