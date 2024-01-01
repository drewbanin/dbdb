

class DBDB_PLAY_TONE:
    def __init__(self, note, octave, length, amplitude):
        pass

    @classmethod
    def validate(cls, args):
        if len(args) != 5:
            raise RuntimeError("PLAY_TONE function requires 5 args")

    @classmethod
    def eval(cls, args, row):
        cls.validate(args)

        names = ['note', 'octave', 'length', 'amplitude', 'start']
        values = [arg.eval(row) for arg in args]

        cmd = dict(zip(names, values))

        return cmd


