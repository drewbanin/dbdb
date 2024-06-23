#!/usr/bin/env python

import sys
import os
import asyncio
import csv

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from dbdb.expressions.functions.table.midi import MIDITableFunction, note_to_frequency

if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} [path-to-midi-file]")
    sys.exit(1)

midi_file = sys.argv[1]

print(f"Reading file {midi_file}")


# async, uh, finds a way
async def get_rows(midi):
    res = []
    async for row in midi.generate():
        res.append(row)

    return res


midi = MIDITableFunction([midi_file])
res = asyncio.run(get_rows(midi))

rows = []
for row in res:
    row = list(row)
    start_time = float(row.pop(0))
    note = row.pop(0)
    octave = int(row.pop(0))
    frequency = float(row.pop(0))
    duration = float(row.pop(0))
    amplitude = float(row.pop(0))
    track = float(row.pop(0))

    freq = note_to_frequency(note, octave)
    rows.append([freq, note, octave, start_time, duration, amplitude, track])


fields = ["freq", "note", "octave", "time", "length", "amp", "track"]
with open("out.csv", "w") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(fields)
    writer.writerows(rows)
