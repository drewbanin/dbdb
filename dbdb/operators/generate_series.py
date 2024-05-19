
from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows

import asyncio
import numpy as np

class GenerateSeriesConfig(OperatorConfig):
    def __init__(
        self,
        table,
        function_args,
    ):
        self.table = table

        if len(function_args) not in [1, 2]:
            raise RuntimeError("GENERATE_SERIES function expects 1 or 2 args")

        self.count = function_args[0]
        self.delay = function_args[1] if len(function_args) == 2 else None


class GenerateSeriesOperator(Operator):
    Config = GenerateSeriesConfig

    def name(self):
        return "Generator"

    @classmethod
    def function_name(cls):
        return "GENERATE_SERIES"

    async def make_iterator(self):
        buffer = np.arange(self.config.count)
        for i in buffer:
            row = (int(i),)
            self.stats.update_row_processed(row)

            await asyncio.sleep(self.config.delay or 0.0)
            yield row
            self.stats.update_row_emitted(row)


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
