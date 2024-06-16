from dbdb.expressions.functions.base import WindowFunction
from dbdb.expressions.expressions import Literal
from dbdb.tuples.context import ExecutionContext


class WindowCount(WindowFunction):
    NAMES = ["COUNT"]

    def eval(self, context: ExecutionContext):
        import ipdb

        ipdb.set_trace()
        return self.accum
