

class DBDB_PLAY_TONE:
    def __init__(self, note, octave, length, amplitude):
        pass

    @classmethod
    def validate(cls, args):
        if len(args) != 4:
            raise RuntimeError("PLAY_TONE function requires 4 args")

    @classmethod
    def eval(cls, args, row):
        cls.validate(args)

        names = ['note', 'octave', 'length', 'amplitude']
        values = [arg.eval(row) for arg in args]

        cmd = dict(zip(names, values))

        return cmd

"""
TODO : you stopped here!

- PLAY_TONE() needs some sort of timing info
- We can do that in SQL if we pass in a row_number
  - Either support window functions (lol) or add it in as a global?
- So, PLAY_TONE gets note info + start time
  - Should it start playing immediately?
  - Or, change PLAY_TONE() to ADD_TONE() then call PLAY_SONG() at the end?

- Join to frequency table...
  - Do a cute thing with octaves...
  - ADD_TONE(hz, start_time, end_time)
  - PLAY_TONE()
  - I like that!

TODO TODO:
- Support window functions
- Two new functions
- You're good to go!

CAVEAT:
- This will run the whole query up-front
- I kind of like the idea of streaming results to the browser
- IE. so we can draw a waveform or whatever

How to do it in a streaming way?
- Each PLAY_TONE() would need to block
- What about when one is silent for 32 beats and one is not?
- hmmmm shit
"""
