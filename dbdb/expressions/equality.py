import enum


class EqualityTypes(enum.Enum):
    EQ = '='
    NEQ = '!='
    LT = '<'
    GT = '>'
    LTE = '<='
    GTE = '>='
    IS = 'is'
    IS_NOT = 'is not'


class Equality:
    def __init__(self, lexpr, rexpr, equality):
        self.lexpr = lexpr
        self.rexpr = rexpr
        self.equality = equality

    def evaluate(self, row):
        lval = self.lexpr.evaluate(row)
        rval = self.rexpr.evaluate(row)

        if self.equality == EqualityTypes.EQ:
            return lval == rval
        elif self.equality == EqualityTypes.NEQ:
            return lval != rval
        elif self.equality == EqualityTypes.LT:
            return lval < rval
        elif self.equality == EqualityTypes.GT:
            return lval > rval
        elif self.equality == EqualityTypes.LTE:
            return lval <= rval
        elif self.equality == EqualityTypes.GTE:
            return lval >= rval
        elif self.equality == EqualityTypes.IS:
            return lval is rval
        elif self.equality == EqualityTypes.IS_NOT:
            return not(lval is rval)
        else:
            raise NotImplementedError()
