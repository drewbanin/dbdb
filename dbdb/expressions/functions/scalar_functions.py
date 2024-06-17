from dbdb.expressions.functions.base import ScalarFunction
from dbdb.tuples.context import ExecutionContext

import math


class FunctionSin(ScalarFunction):
    NAMES = ["sin"]

    def eval(self, context: ExecutionContext):
        value = self.expr[0].eval(context)
        return math.sin(value)


class FunctionCos(ScalarFunction):
    NAMES = ["cos"]

    def eval(self, context: ExecutionContext):
        value = self.expr[0].eval(context)
        return math.cos(value)


class FunctionSquare(ScalarFunction):
    NAMES = ["sqr"]

    def eval(self, context: ExecutionContext):
        value = self.expr[0].eval(context)
        value = math.sin(value)
        if value > 0:
            return 1
        else:
            return -1


class FunctionIff(ScalarFunction):
    NAMES = ["iff"]

    def eval(self, context: ExecutionContext):
        if len(self.expr) != 3:
            raise RuntimeError("IFF requires 3 args")

        cond = self.expr[0]
        if cond.eval(context):
            return self.expr[1].eval(context)
        else:
            return self.expr[2].eval(context)


class FunctionPow(ScalarFunction):
    NAMES = ["pow"]

    def eval(self, context: ExecutionContext):
        if len(self.expr) != 2:
            raise RuntimeError("POW requires 2 args")

        base = self.expr[0].eval(context)
        exp = self.expr[1].eval(context)

        return base**exp
