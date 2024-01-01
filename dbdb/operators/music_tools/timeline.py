
import pygame
import numpy as np
from collections import defaultdict
import asyncio


class SoundBuffer:
    def __init__(self, max_size=60):
        self.buffer = self._make_buffer(max_size)
        self.max_pos = 0

        self.current_pos = 0

    def _make_buffer(self, length_in_s):
        return np.zeros(44100 * length_in_s, dtype = np.int16)

    def set_freq(self, pos, freq):
        self.buffer[pos] = freq
        self.max_pos = max(pos, self.max_pos)

    def norm_buffer(self, buffer):
        # min_hz = min(buffer)
        # max_hz = max(buffer)
        # norm_hz = 2 * ((note_hz - min_hz) / (max_hz - min_hz)) - 1
        # pass
        return buffer

    def get_next_chunk(self, flush=False):
        start = self.current_pos
        if flush:
            end = self.max_pos
        else:
            end = start + 44100

        if not flush and self.max_pos < end:
            return None
        else:
            self.current_pos = end
            segment = self.buffer[start:end]
            normed = self.norm_buffer(segment)
            return normed


class Timeline:
    def __init__(self):
        self.buffer = SoundBuffer()
        self.channel = None

    def add_tone(self, t, frequency):
        bits = 16
        max_sample = 2**(bits - 1) - 1

        freq = int(round(max_sample * frequency))
        self.buffer.set_freq(t, freq)

    async def buffered_play(self, flush=False):
        buf = self.buffer.get_next_chunk(flush=flush)
        if buf is None:
            return

        if self.channel is None:
            sound = pygame.sndarray.make_sound(buf)
            sound.set_volume(0.2)
            self.channel = sound.play(loops = 0)
        else:
            sound = pygame.sndarray.make_sound(buf)
            sound.set_volume(0.2)
            self.channel.queue(sound)

    async def wait_for_completion(self):
        if self.buffer.current_pos < self.buffer.max_pos:
            await self.buffered_play(flush=True)

        while self.channel and self.channel.get_busy():
            await asyncio.sleep(0.01)
