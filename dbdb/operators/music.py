from dbdb.operators.base import Operator, OperatorConfig
import itertools

from dbdb.operators.music_tools import music_player
from dbdb.operators.music_tools.track import Track
from dbdb.operators.music_tools.timeline import Timeline


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

            # dumb
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

            yield row

            # self.stats.update_row_emitted(row)

        await timeline.wait_for_completion()
        self.stats.update_done_running()

    async def run(self, rows):
        music_player.init()

        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        self.iterator = iterator
        return rows.new(iterator)
