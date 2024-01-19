import pygame
from pygame.locals import *

import math
import numpy as np

bits = 16
sample_rate = 44100
initialized = False

def init():
    global initialized
    if initialized:
        return

    initialized=True

    pygame.mixer.pre_init(sample_rate, -bits, channels=1)
    pygame.init()


def make_track(duration, note_func):
    n_samples = int(round(duration*sample_rate))

    notes = []
    for s in range(n_samples):
        t = s / sample_rate
        hz = note_func(t)
        notes.append(hz)

    return notes


def make_buf(notes):
    n_samples = len(notes)
    buf = np.zeros(n_samples, dtype = np.int16)
    max_sample = 2**(bits - 1) - 1

    min_hz = min(notes)
    max_hz = max(notes)

    for i, note_hz in enumerate(notes):
        norm_hz = (note_hz - min_hz) / (max_hz - min_hz)
        freq = int(round(max_sample * norm_hz))
        buf[i] = freq

    return buf


def play_track(track):
    print("Making track")
    notes = make_track(duration=60, note_func=track.tone)

    print("Making buffer")
    buf = make_buf(notes)

    print("Making sound")
    sound = pygame.sndarray.make_sound(buf)
    sound.set_volume(0.2)

    print("Playing")
    channel = sound.play(loops = 0)

    print("Waiting")
    while channel.get_busy():
        pygame.time.wait(100)  # ms
