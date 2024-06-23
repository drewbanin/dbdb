from dbdb.expressions.functions.base import TableFunction

import os

from openai import AsyncOpenAI


API_KEY = os.getenv("DBDB_OPENAI_API_KEY")
PROMPT = """
You are an operator inside of a columnar database. When you are asked a question, it is because the query planner decided
that you are the best suited operator to execute the user's query. Respond to the user's query in the most reasonable way
possible, but ALWAYS return data in tabular format. If the user asks you to use specific column names, make sure to include
them in your response or the query will fail. Structure your response to return csv-formatted data.

If you are asked to play a song, using the column headers "time" (start time in seconds), "freq" (note frequency in hz),
"length" (duration of note in seconds), and "func" (one of "sin" or "sqr" to control the waveform produced for your music).
Use less than 100 notes to play your song unless you are specifically requested to play a longer song.

If you are asked to draw a picture, use the column names x for the x-coordinate, y for the y-coordinate, and
color (using hex color codes) to control the color of each point. Lines will not automatically be drawn between
your points, so make sure to include some extra points to denote lines if need be. Use less than 100 points to draw
your shape unless requested otherwise.

Do not editorialize your response. Reply only with CSV text. Do not use code blocks to format your answer. Always include
a header row in your response.
"""


class AskGPTTableFunction(TableFunction):
    NAMES = ["ASK_GPT"]

    def __init__(self, args):
        if len(args) != 1:
            raise RuntimeError("ASK_GPT function expects 1 arg")

        self.check_api_key()

        self.prompt = args[0]
        self.client = AsyncOpenAI(api_key=API_KEY)

        self.iterator = self.make_iterator()
        self._fields = None

    def check_api_key(self):
        if not API_KEY:
            raise RuntimeError(
                "dbdb was not initialized with an OpenAI API key & can't do llm magic :/"
            )

    async def make_iterator(self):
        stream = await self.client.chat.completions.create(
            model="gpt-4o",
            stream=True,
            messages=[
                {
                    "role": "system",
                    "content": PROMPT,
                },
                {
                    "role": "user",
                    "content": self.prompt,
                },
            ],
        )

        accum = ""
        async for chunk in stream:
            value = chunk.choices[0].delta.content or ""
            accum += value

            if "\n" in accum:
                lines = accum.split("\n")

                # Omit last element - it might be incomplete...
                for line in lines[:-1]:
                    row = [v.strip() for v in line.split(",")]
                    yield row

                # Reset the accumulator to the last (incomplete?) line
                accum = lines[-1]

        if len(accum) > 0:
            lines = accum.split("\n")
            for line in lines:
                row = [v.strip() for v in line.split(",")]
                yield row

    async def fields(self):
        # We need to crank the iterator to get the first row
        # back from gpt4, BUT we also need to make sure to
        # only do that once or we'll accidentally consume data
        if self._fields is None:
            self._fields = await self.iterator.__anext__()

        return self._fields

    async def generate(self):
        async for row in self.iterator:
            yield row
