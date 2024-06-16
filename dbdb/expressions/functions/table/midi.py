from dbdb.expressions.functions.base import TableFunction

import asyncio
import mido
from mido import MidiFile
import itertools


NOTES = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]
OCTAVES = list(range(11))
NOTES_IN_OCTAVE = len(NOTES)
NOTE_FREQ = {
    "A": 440.00,
    "A#": 466.16,
    "B": 493.88,
    "Bb": 466.16,
    "C": 523.25,
    "C#": 554.36,
    "D": 587.33,
    "D#": 622.25,
    "Eb": 622.25,
    "E": 659.25,
    "F": 698.46,
    "F#": 739.99,
    "G": 783.99,
    "G#": 830.61,
}


def note_to_frequency(note: str, octave: int) -> float:
    base_freq = NOTE_FREQ[note]
    transposed = base_freq * (2 ** (octave - 5))
    return transposed


def number_to_note(number: int) -> tuple:
    octave = number // NOTES_IN_OCTAVE
    assert octave in OCTAVES, errors["notes"]
    assert 0 <= number <= 127, errors["notes"]
    note = NOTES[number % NOTES_IN_OCTAVE]

    return note, octave


class MIDITableFunction(TableFunction):
    NAMES = ["MIDI"]

    def __init__(self, args):
        if len(args) != 1:
            raise RuntimeError("MIDI function expects 1 arg")

        self.fname = args[0]

    def details(self):
        return {"table": self.config.fname, "columns": []}

    async def fields(self):
        return ["time", "note", "octave", "freq", "length", "amplitude"]

    async def generate(self):
        midi_file = MidiFile(self.fname)

        tempo = 0
        for i, track in enumerate(midi_file.tracks):
            now_playing = {}
            t = 0
            for msg in track:
                t += msg.time
                if msg.type == "set_tempo":
                    bpm = mido.tempo2bpm(msg.tempo)
                    tempo = msg.tempo
                elif msg.type == "note_on":
                    now_playing[msg.note] = t
                elif msg.type == "note_off":
                    if msg.note not in now_playing:
                        continue

                    start_tick = now_playing.pop(msg.note)
                    start_time = mido.tick2second(
                        start_tick, midi_file.ticks_per_beat, tempo
                    )
                    end_time = mido.tick2second(t, midi_file.ticks_per_beat, tempo)
                    duration = end_time - start_time

                    note, octave = number_to_note(msg.note)
                    frequency = note_to_frequency(note, octave)

                    yield (
                        start_time,
                        note,
                        int(octave),
                        frequency,
                        duration,
                        1,
                    )
