from dbdb.operators.base import Operator, OperatorConfig


class DistinctConfig(OperatorConfig):
    def __init__(
        self,
    ):
        pass


class DistinctOperator(Operator):
    Config = DistinctConfig

    def name(self):
        return "Order"

    async def make_iterator(self, rows):
        seen = set()

        async for row in rows:
            self.stats.update_row_emitted(row)
            row_tuple = row.as_tuple()

            if row_tuple in seen:
                continue

            seen.add(row_tuple)
            yield row

        self.stats.update_done_running()

    async def run(self, rows):
        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        return rows.new(iterator)
