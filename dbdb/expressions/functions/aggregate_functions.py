
from dbdb.expressions.functions.base import AggregateFunction

class AggregationMin(AggregateFunction):
    NAMES = ['MIN']

    def eval(self, expr, row):
        value = expr[0].eval(row)
        if self.accum is None:
            self.accum = value

        elif value < self.accum:
            self.accum = value


class AggregationMax(AggregateFunction):
    NAMES = ['MAX']

    def eval(self, expr, row):
        value = expr[0].eval(row)
        if self.accum is None:
            self.accum = value

        elif value > self.accum:
            self.accum = value


class AggregationSum(AggregateFunction):
    NAMES = ['SUM']

    def eval(self, expr, row):
        value = expr[0].eval(row)
        if self.accum is None:
            self.accum = value

        else:
            self.accum += value


class AggregationAverage(AggregateFunction):
    NAMES = ['AVG']

    def start(self):
        self.accum = 0
        self.seen = 0

    def eval(self, expr, row):
        value = expr[0].eval(row)
        self.accum += value
        self.seen += 1

    def result(self):
        if self.seen == 0:
            return None

        return self.accum / self.seen


class AggregationCount(AggregateFunction):
    NAMES = ['COUNT']

    def start(self):
        self.accum = []

    def eval(self, expr, row):
        # This is a dumb implementation for normal COUNT(), but
        # I don't want to try to connect the sql parser to two
        # different functions both called "count" rn - TODO
        value = expr[0].eval(row)
        self.accum.append(value)

    def result(self):
        if self.modifiers.get('DISTINCT'):
            return len(set(self.accum))
        else:
            return len(self.accum)


class AggregationListAgg(AggregateFunction):
    NAMES = ['LIST_AGG', 'LISTAGG']

    def start(self):
        self.accum = []
        self.delim = ','

    def eval(self, expr, row):
        if len(expr) == 1:
            value = expr[0].eval(row)
        elif len(expr) == 2:
            value, delim = values
            self.delim = delim

            delim_is_literal = isinstance(delim, Literal)
            delim_is_string = delim.is_string()
            if not (delim_is_literal and delim_is_string):
                raise RuntimeError("LIST_AGG expects a string delimiter")

        self.accum.append(value)

    def result(self):
        if self.modifiers.distinct:
            values = []
            seen = set()
            for item in self.accum:
                if item not in seen:
                    values.append(item)
                    seen.add(item)

        else:
            values = self.accum

        return self.delim.join([str(v) for v in values])
