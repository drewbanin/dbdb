from dbdb.operators.base import Operator, OperatorConfig
import itertools

from dbdb.operators.music_tools import music_player
from dbdb.operators.music_tools.track import Track


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
        track = Track(bpm=int(self.config.bpm))

        async for row in tuples:
            self.stats.update_row_processed(row)

            track.add_note(
                note=row.field('note'),
                octave=int(row.field('octave')),
                length=float(row.field('length')),
                amplitude=float(row.field('amplitude')),
            )
            yield row

            self.stats.update_row_emitted(row)

        # WAIT for music here?
        music_player.play_track(track)
        self.stats.update_done_running()

    async def run(self, rows):
        music_player.init()

        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        self.iterator = iterator
        return rows.new(iterator)
