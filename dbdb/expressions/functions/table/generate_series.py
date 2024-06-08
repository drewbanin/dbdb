import asyncio
from dbdb.expressions.functions.base import TableFunction


class GenerateSeriesTableFunction(TableFunction):
    NAMES = ['GENERATE_SERIES']

    def __init__(self, args):
        if len(args) not in [1, 2]:
            raise RuntimeError("GENERATE_SERIES function expects 1 or 2 args")

        self.count = args[0]
        self.delay = args[1] if len(args) == 2 else 0

    async def fields(self):
        return ["i"]

    async def generate(self):
        for i in range(self.count):
            yield (int(i),)
            await asyncio.sleep(self.delay)
