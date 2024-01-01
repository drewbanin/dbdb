
import math
import numpy as np

class DBDB_SIN:
    @classmethod
    def eval(cls, args, row):
        value = args[0].eval(row)
        return np.sin(value)


class DBDB_SQR:
    def __init__(self, number):
        pass

    @classmethod
    def eval(cls, args, row):
        value = args[0].eval(row)
        value = np.sin(value)
        if value > 0:
            return 1
        else:
            return -1


class DBDB_IFF:
    @classmethod
    def eval(cls, args, row):
        if len(args) != 3:
            raise RuntimeError("IFF requires 3 args")

        cond = args[0]
        if cond.eval(row):
            return args[1].eval(row)
        else:
            return args[2].eval(row)


class DBDB_POW:
    @classmethod
    def eval(cls, args, row):
        if len(args) != 2:
            raise RuntimeError("POW requires 2 args")

        base = args[0].eval(row)
        exp = args[1].eval(row)

        return base ** exp
