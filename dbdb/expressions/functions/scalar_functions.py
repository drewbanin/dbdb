
import math
from dbdb.expressions.functions.base import ScalarFunction


class FunctionSin(ScalarFunction):
    NAMES = ['sin']

    def eval(cls, args, row):
        value = args[0].eval(row)
        return math.sin(value)


class FunctionCos(ScalarFunction):
    NAMES = ['cos']

    def eval(cls, args, row):
        value = args[0].eval(row)
        return math.cos(value)


class FunctionSquare(ScalarFunction):
    NAMES = ['sqr']

    def eval(cls, args, row):
        value = args[0].eval(row)
        value = math.sin(value)
        if value > 0:
            return 1
        else:
            return -1


class FunctionIff(ScalarFunction):
    NAMES = ['iff']

    def eval(cls, args, row):
        if len(args) != 3:
            raise RuntimeError("IFF requires 3 args")

        cond = args[0]
        if cond.eval(row):
            return args[1].eval(row)
        else:
            return args[2].eval(row)


class FunctionPow(ScalarFunction):
    NAMES = ['pow']

    def eval(cls, args, row):
        if len(args) != 2:
            raise RuntimeError("POW requires 2 args")

        base = args[0].eval(row)
        exp = args[1].eval(row)

        return base ** exp
