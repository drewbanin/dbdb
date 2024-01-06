
from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows

import asyncio
import numpy as np

class GenerateSeriesConfig(OperatorConfig):
    def __init__(
        self,
        table,
        count
    ):
        self.table = table
        self.count = count


class GenerateSeriesOperator(Operator):
    Config = GenerateSeriesConfig

    def name(self):
        return "Generator"

    async def make_iterator(self):
        buffer = np.arange(self.config.count)
        for i in buffer:
            row = (int(i),)
            yield row

            # would be cooler if range() was async...
            if i % 100 == 0:
                await asyncio.sleep(0.0)
            # self.stats.update_row_emitted(row)

        self.stats.update_done_running()

    async def run(self):
        self.stats.update_start_running()
        iterator = self.make_iterator()
        self.iterator = iterator

        fields = [self.config.table.field("i")]
        return Rows(
            self.config.table,
            fields,
            iterator,
        )
