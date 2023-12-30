from dbdb.operators.base import Operator, OperatorConfig
import itertools


class UnionConfig(OperatorConfig):
    def __init__(
        self,
    ):
        pass


class UnionOperator(Operator):
    Config = UnionConfig

    def name(self):
        return "Union"

    async def make_iterator(self, row_producers):
        for rows in row_producers:
            async for row in rows:
                self.stats.update_row_processed(row)
                yield row
                self.stats.update_row_emitted(row)

        self.stats.update_done_running()

    async def run(self, rows):
        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        self.iterator = iterator

        # Just use fields from first input. Probably
        # would be a good idea to validate shape of inputs.
        # too bad!
        return rows[0].new(iterator)
