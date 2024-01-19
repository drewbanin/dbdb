import math

freqs = {
    "A": 440.00,
    "A#": 466.16,
    "B": 493.88,
    "Bb": 466.16,
    "C": 523.25,
    "D": 587.33,
    "Eb": 622.25,
    "E": 659.25,
    "F": 698.46,
    "F#": 739.99,
    "G": 783.99,
}

def sin(t,f,a):
    return a * math.sin( 2 * math.pi *  t * f)

def sqr(t,f,a):
    return  ((sin(t,f,a) > 0) * 2 - 1) * a


class Note:
    def __init__(self, note, octave=4, length=1, amplitude=1):
        self.note = note
        self.octave = octave
        self.length = length
        self.amplitude = amplitude
        self.hz = self.freq()

        self._started_at = None

    def to_csv(self):
        return (
            self.note,
            self.octave,
            self.length,
            self.amplitude
        )

    def copy(self):
        return Note(
            note=self.note,
            octave=self.octave,
            length=self.length,
            amplitude=self.amplitude,
        )

    def freq(self):
        hz = freqs[self.note]
        power = self.octave - 4
        return hz * 2**power

    def play_hz(self, t, note_length):
        if not self._started_at:
            self._started_at = t

        elapsed = (t - self._started_at)
        remaining = note_length - elapsed

        if elapsed < 0.01:
            return 0
        elif remaining < 0.01:
            return 0

        return self.hz


class Rest(Note):
    def __init__(self, length=1):
        self.hz = 0
        self.length = length
        self.amplitude = 0

    def to_csv(self):
        return (
            'Rest',
            0,
            self.length,
            0
        )

    def copy(self):
        return Rest(
            length=self.length
        )

    def play_hz(self, t, note_length):
        return 0


class Chord(Note):
    def __init__(self, *notes):
        self.notes = notes
        self.length = max(n.length for n in notes)

    def copy(self):
        notes = [n.copy() for n in self.notes]
        return Chord(*notes)

    def play_hz(self, t, note_length):
        total = sum(n.play_hz(t, note_length) for n in self.notes)
        return total

class Track:
    def __init__(self, notes=None, bpm=100):
        self.notes = notes or []
        self.bpm = bpm

        self._last_note_idx = -1
        self._advance_at_time = 0
        self._time_in_note = 0

    def to_csv(self):
        rows = []
        for note in self.notes:
            rows.append(note.to_csv())

        return rows

    def add(self, *notes):
        self.notes += list(notes)

    def add_note(self, note, octave, length, amplitude):
        if note == "Rest":
            noteObj = Rest(length)
        else:
            noteObj = Note(note, octave, length, amplitude)

        self.notes.append(noteObj)

    def repeat(self, times):
        new_notes = []
        for i in range(times):
            for note in self.notes:
                new_notes.append(note.copy())

        return Track(new_notes, bpm=self.bpm)

    def get_note_at_time(self, t):
        if t >= self._advance_at_time:
            self._last_note_idx += 1
            if len(self.notes) <= self._last_note_idx:
                return None

            spb = 60.0 / self.bpm
            note = self.notes[self._last_note_idx]
            note_length = note.length * spb
            self._advance_at_time += note_length
            return note

        elif len(self.notes) > self._last_note_idx:
            return self.notes[self._last_note_idx]

        else:
            return None

    def tone(self, t):
        spb = 60.0 / self.bpm
        note = self.get_note_at_time(t)

        if note is None:
            return 0

        hz = note.play_hz(t, note.length * spb)
        note_hz = sqr(t, hz, note.amplitude)
        return note_hz
