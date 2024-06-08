
from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows

import asyncio

class TableFunctionConfig(OperatorConfig):
    def __init__(
        self,
        table,
        function_name,
        function_args,
        function_class
    ):
        self.table = table
        self.function_name = function_name
        self.function_args = function_args
        self.function_class = function_class


class TableFunctionOperator(Operator):
    Config = TableFunctionConfig

    def name(self):
        return "Generator"

    @classmethod
    def function_name(cls):
        return self.function_name

    async def make_iterator(self, processor):
        async for row in processor.generate():
            self.stats.update_row_processed(row)
            yield row
            self.stats.update_row_emitted(row)

        self.stats.update_done_running()

    async def run(self):
        self.stats.update_start_running()

        processor = self.config.function_class(self.config.function_args)
        iterator = self.make_iterator(processor)

        fields = [
            self.config.table.field(field_name)
            for field_name in await processor.fields()
        ]

        return Rows(
            self.config.table,
            fields,
            iterator,
        )
