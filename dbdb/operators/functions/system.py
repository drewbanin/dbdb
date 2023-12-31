
import math

class DBDB_SIN:
    def __init__(self, number):
        pass

    @classmethod
    def eval(cls, args, row):

        value = args[0].eval(row)
        return math.sin(value)
