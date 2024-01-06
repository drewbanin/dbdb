from dbdb.operators.base import Operator, OperatorConfig

from dbdb.operators.music_tools import music_player
from dbdb.operators.music_tools.track import Track
from dbdb.operators.music_tools.timeline import Timeline
from dbdb.tuples.identifiers import TableIdentifier
from dbdb.tuples.rows import Rows

import asyncio


class MusicConfig(OperatorConfig):
    def __init__(
        self,
        bpm
    ):
        self.bpm = bpm


class PlayMusicOperator(Operator):
    Config = MusicConfig

    def name(self):
        return "Music"

    async def make_iterator(self, tuples):
        timeline = Timeline(bpm=int(self.config.bpm))

        async for row in tuples:
            # self.stats.update_row_processed(row)

            try:
                length = row.field('length')
            except RuntimeError:
                length = 1

            try:
                amplitude = row.field('amplitude')
            except RuntimeError:
                amplitude = 1

            timeline.add_tone(
                start_time=row.field('time'),
                frequency=row.field('freq'),
                length=length,
                amplitude=amplitude,
            )

            # self.stats.update_row_emitted(row)

        # Generate track so we can stream data to client
        print("Generating track")
        async for row in timeline.gen_track():
            yield row

        self.stats.update_done_running()

    async def run(self, rows):
        music_player.init()
        self.stats.update_start_running()

        temp_table = TableIdentifier.temporary()
        fields = [
            temp_table.field('time'),
            temp_table.field('freq')
        ]

        iterator = self.make_iterator(rows)
        self.iterator = iterator

        return Rows(
            temp_table,
            fields,
            iterator,
        )
