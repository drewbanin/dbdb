from dbdb.expressions.functions.base import AggregateFunction
from dbdb.expressions.expressions import Literal
from dbdb.tuples.context import ExecutionContext


class AggregationMin(AggregateFunction):
    NAMES = ["MIN"]

    def eval(self, context: ExecutionContext):
        value = self.expr[0].eval(context)
        if self.accum is None:
            self.accum = value

        elif value < self.accum:
            self.accum = value

        return self.accum


class AggregationMax(AggregateFunction):
    NAMES = ["MAX"]

    def eval(self, context: ExecutionContext):
        value = self.expr[0].eval(context)
        if self.accum is None:
            self.accum = value

        elif value > self.accum:
            self.accum = value

        return self.accum


class AggregationSum(AggregateFunction):
    NAMES = ["SUM"]

    def eval(self, context: ExecutionContext):
        value = self.expr[0].eval(context)
        if self.accum is None:
            self.accum = value

        else:
            self.accum += value

        return self.accum


class AggregationAverage(AggregateFunction):
    NAMES = ["AVG"]

    def start(self):
        self.accum = 0
        self.seen = 0

    def eval(self, context: ExecutionContext):
        value = self.expr[0].eval(context)
        self.accum += value
        self.seen += 1

        return self.accum / self.seen


class AggregationCount(AggregateFunction):
    NAMES = ["COUNT"]

    def start(self):
        self.accum = []

    def eval(self, context: ExecutionContext):
        # This is a dumb implementation for normal COUNT(), but
        # I don't want to try to connect the sql parser to two
        # different functions both called "count" rn - TODO
        value = self.expr[0].eval(context)
        self.accum.append(value)

        if self.modifiers.get("DISTINCT"):
            return len(set(self.accum))
        else:
            return len(self.accum)


class AggregationListAgg(AggregateFunction):
    NAMES = ["LIST_AGG", "LISTAGG"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.expr, self.delim = self.make_delim()

    def start(self):
        self.accum = []

    def make_delim(self):
        delim = ","

        if len(self.expr) == 1:
            expr = self.expr[0]

        elif len(self.expr) == 2:
            expr, delim = self.expr

            delim_is_literal = isinstance(delim, Literal)
            delim_is_string = delim.is_string()
            if not (delim_is_literal and delim_is_string):
                raise RuntimeError("LIST_AGG expects a string delimiter")

            delim = delim.eval(None)

        return expr, delim

    def eval(self, context: ExecutionContext):
        value = self.expr.eval(context)
        self.accum.append(value)

        if self.modifiers.get("DISTINCT"):
            values = []
            seen = set()
            for item in self.accum:
                if item not in seen:
                    values.append(item)
                    seen.add(item)

        else:
            values = self.accum

        return self.delim.join([str(v) for v in values])
