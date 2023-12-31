
import pygame
import numpy as np
from collections import defaultdict

FACTOR = 1

class Timeline:
    def __init__(self):
        # TODO : BPM?
        self.tones = defaultdict(list)
        self.duration_ms = 0

    def add_tone(self, start_time_ms, frequency):
        # t = int(start_time_ms * 44100)
        t = start_time_ms
        self.tones[t].append(frequency)

        self.duration_ms = max(t, self.duration_ms)

    def _make_buffer(self):
        # n_samples = int(44100 * self.duration_ms / FACTOR)
        n_samples = max(self.tones)
        print("NSAMPLES = ", n_samples, "DURATION = ", self.duration_ms)
        buf = np.zeros(n_samples, dtype = np.int16)
        bits = 16
        max_sample = 2**(bits - 1) - 1

        combined = {k: sum(v) for (k,v) in self.tones.items()}
        min_hz = min(combined.values())
        max_hz = max(combined.values())

        seen = set()
        for t in range(n_samples):
            # t = int(FACTOR * (i / 44100))
            note_hz = combined.get(t, 0)
            norm_hz = 2 * ((note_hz - min_hz) / (max_hz - min_hz)) - 1
            freq = int(round(max_sample * norm_hz))
            buf[t] = freq

        return buf

    def play(self):
        print("Making buffer")
        buf = self._make_buffer()

        print("Making sound")
        sound = pygame.sndarray.make_sound(buf)
        sound.set_volume(0.2)

        print("Playing")
        channel = sound.play(loops = 0)

        print("Waiting")
        import time
        st = time.time()
        print("STARTED AT:", st)
        while channel.get_busy():
            pygame.time.wait(10)  # ms
            elapsed = time.time() - st
            try:
                print(elapsed, buf[int(elapsed * 44100)])
            except:
                pass
