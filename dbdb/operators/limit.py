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

    def name(self):
        return "Limit"

    def details(self):
        return {"Limit": self.config.limit}

    async def make_iterator(self, tuples):
        limit = self.config.limit

        # Do not read _any_ rows if limit is zero
        if limit == 0:
            raise StopIteration()

        i = 0
        async for row in tuples:
            self.stats.update_row_processed(row)

            if i < limit:
                yield row
                self.stats.update_row_emitted(row)

            # This is dumb! We would ideally break, but we
            # actually need to go back and drain our parent
            # iterators or else they will "hang".
            i += 1

        self.stats.update_done_running()

    async def run(self, rows):
        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        iterator = self.add_exit_check(iterator)
        return rows.new(iterator)
