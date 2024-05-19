
from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows

import asyncio
import os

from openai import AsyncOpenAI


API_KEY = os.getenv('DBDB_OPENAI_API_KEY')
PROMPT = """
You are an operator inside of a columnar database. When you are asked a question, it is because the query planner decided
that you are the best suited operator to execute the user's query. Respond to the user's query in the most reasonable way
possible, but ALWAYS return data in tabular format. If the user asks you to use specific column names, make sure to include
them in your response or the query will fail. Structure your response to return csv-formatted data.

If you are asked to play a song, using the column headers "time" (start time in seconds), "freq" (note frequency in hz),
"length" (duration of note in seconds), and "func" (one of "sin" or "sqr" to control the waveform produced for your music).
Use less than 100 notes to play your song unless you are specifically requested to play a longer song.

If you are asked to draw a picture, use the column names point_x for the x-coordinate, point_y for the y-coordinate, and
point_color (using hex color codes) to control the color of each point. Lines will not automatically be drawn between
your points, so make sure to include some extra points to denote lines if need be. Use less than 100 points to draw
your shape unless requested otherwise.

Do not editorialize your response. Reply only with CSV text. Do not use code blocks to format your answer. Always include
a header row in your response.
"""


def check_api_key():
    if not API_KEY:
        raise RuntimeError("dbdb was not initialized with an OpenAI API key & can't query do llm magic :/")


class AskGPTConfig(OperatorConfig):
    def __init__(
        self,
        table,
        function_args,
    ):
        self.table = table

        if len(function_args) != 1:
            raise RuntimeError("ASK_GPT function expects 1 arg")

        self.prompt = function_args[0]
        self.client = AsyncOpenAI(api_key=API_KEY)


class AskGPTOperator(Operator):
    Config = AskGPTConfig

    def name(self):
        return "GPT-4"

    @classmethod
    def function_name(cls):
        return "ASK_GPT"

    async def make_iterator(self):
        stream = await self.config.client.chat.completions.create(
            model="gpt-4o",
            stream=True,
            messages=[
                {
                    "role": "system",
                    "content": PROMPT,
                },
                {
                    "role": "user",
                    "content": self.config.prompt,
                }
            ],
        )

        accum = ""
        async for chunk in stream:
            value = chunk.choices[0].delta.content or ""
            accum += value

            if '\n' in accum:
                lines = accum.split("\n")

                # Omit last element - it might be incomplete...
                for line in lines[:-1]:
                    row = [v.strip() for v in line.split(",")]

                    self.stats.update_row_processed(row)

                    yield row

                    self.stats.update_row_emitted(row)

                # Reset the accumulator to the last (incomplete?) line
                accum = lines[-1]

        if len(accum) > 0:
            lines = accum.split("\n")
            for line in lines:
                row = [v.strip() for v in line.split(",")]
                yield row

        self.stats.update_done_running()

    async def run(self):
        self.stats.update_start_running()
        iterator = self.make_iterator()
        self.iterator = iterator

        header_row = await self.iterator.__anext__()
        fields = [self.config.table.field(col_name) for col_name in header_row]

        return Rows(
            self.config.table,
            fields,
            iterator,
        )
