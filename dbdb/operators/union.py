from dbdb.operators.base import Operator, OperatorConfig
import itertools


class UnionConfig(OperatorConfig):
    def __init__(
        self,
        distinct = False
    ):
        self.distinct = distinct


class UnionOperator(Operator):
    Config = UnionConfig

    def name(self):
        return "Union"

    async def make_iterator(self, row_producers):
        # Reverse operators since they were added in reverse order
        seen = set()
        for rows in row_producers[::-1]:
            async for row in rows:
                self.stats.update_row_processed(row)

                if self.config.distinct and tuple(row) not in seen:
                    yield row
                    seen.add(tuple(row))
                elif not self.config.distinct:
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
