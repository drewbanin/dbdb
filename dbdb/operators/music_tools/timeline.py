
import pygame
import numpy as np
from collections import defaultdict
import asyncio
import math


SAMPLE_RATE = 44100

def sin(t,f,a):
    return a * math.sin( 2 * math.pi *  t * f)

def sqr(t,f,a):
    return  ((sin(t,f,a) > 0) * 2 - 1) * a

class Timeline:
    def __init__(self, bpm, max_length=600):
        self.bpm = bpm
        self.buffer = np.zeros(SAMPLE_RATE * max_length, dtype = np.double)
        self.note_count = np.zeros(SAMPLE_RATE * max_length, dtype = np.double)
        self.max_length = max_length

        self._scale = self.scale_factor(bpm)
        self._note_offset = int(self._scale * 0.01)
        self._max_tone_pos = 0

    def scale_factor(self, bpm):
        bps = bpm / 60.0
        spb = 1.0 / bps
        note_length = spb * SAMPLE_RATE

        return note_length

    def add_tone(self, start_time, length, frequency, amplitude):
        start_index = int(self._scale * start_time) + self._note_offset
        end_index = int(start_index + length * self._scale) - self._note_offset

        self._max_tone_pos = max(self._max_tone_pos, end_index)

        if start_index > len(self.buffer) or end_index > len(self.buffer):
            raise RuntimeError(f"Max supported audio length is {self.max_length} seconds!")

        for i in range(start_index, end_index):
            t = i / SAMPLE_RATE
            freq = sqr(t, frequency, amplitude)
            self.buffer[i] += freq
            self.note_count[i] += 1

    async def wait_for_completion(self):
        bits = 16
        max_sample = 2**(bits - 1) - 1
        data = self.buffer[0:self._max_tone_pos]
        count = self.note_count[0:self._max_tone_pos]

        averaged = np.nan_to_num(data / count)
        clipped = np.clip(averaged, -1, 1)

        normed = np.zeros(len(clipped), dtype = np.int16)
        for i, value in enumerate(clipped):
            normed[i] = int(round(max_sample * value))

        sound = pygame.sndarray.make_sound(normed)
        sound.set_volume(0.2)
        channel = sound.play(loops = 0)

        while channel.get_busy():
            await asyncio.sleep(0.01)
