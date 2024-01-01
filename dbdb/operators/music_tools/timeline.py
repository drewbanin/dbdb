
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
    def __init__(self, bpm, max_length=60):
        self.bpm = bpm
        self.buffer = np.zeros(SAMPLE_RATE * max_length, dtype = np.int16)

        self._scale = self.scale_factor(bpm)
        self._note_offset = int(self._scale * 0.01)

    def scale_factor(self, bpm):
        bps = bpm / 60.0
        spb = 1.0 / bps
        note_length = spb * SAMPLE_RATE

        return note_length

    def add_tone(self, start_time, length, frequency):
        bits = 16
        max_sample = 2**(bits - 1) - 1

        start_index = int(self._scale * start_time) + self._note_offset
        end_index = int(start_index + length * self._scale) - self._note_offset

        for i in range(start_index, end_index):
            t = i / SAMPLE_RATE
            freq = sqr(t, frequency, 1)
            normed = int(round(max_sample * freq))
            self.buffer[i] += normed

    async def wait_for_completion(self):
        sound = pygame.sndarray.make_sound(self.buffer)
        sound.set_volume(0.2)
        channel = sound.play(loops = 0)

        while channel.get_busy():
            await asyncio.sleep(0.01)
