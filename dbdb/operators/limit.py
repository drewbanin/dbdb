from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows


class LimitConfig(OperatorConfig):
    def __init__(
        self,
        limit,
    ):
        self.limit = limit


class LimitOperator(Operator):
    Config = LimitConfig

    def make_iterator(self, tuples):
        limit = self.config.limit

        # Do not read _any_ rows if limit is zero
        if limit == 0:
            raise StopIteration()

        for i, val in enumerate(tuples):
            yield val

            if i >= limit - 1:
                break

    def run(self, rows):
        iterator = self.make_iterator(rows)
        return rows.new(iterator)
