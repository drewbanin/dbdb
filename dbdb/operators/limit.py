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

        for i, row in enumerate(tuples):
            self.stats.update_row_processed(row)
            yield row
            self.stats.update_row_emitted(row)

            if i >= limit - 1:
                break

        self.stats.update_done_running()

    def run(self, rows):
        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        return rows.new(iterator)
