def nullcheck(inner):
    def check_nulls(*args):
        if None in args:
            return None
        else:
            return inner(*args)

    return check_nulls


@nullcheck
def op_add(l, r):
    return l + r


@nullcheck
def op_sub(l, r):
    return l - r


@nullcheck
def op_mul(l, r):
    return l * r


@nullcheck
def op_div(l, r):
    return l / r


@nullcheck
def op_and(l, r):
    return l and r


@nullcheck
def op_or(l, r):
    return l or r


@nullcheck
def op_eq(l, r):
    return l == r


@nullcheck
def op_neq(l, r):
    return l != r


@nullcheck
def op_lt(l, r):
    return l < r


@nullcheck
def op_gt(l, r):
    return l > r


@nullcheck
def op_lte(l, r):
    return l <= r


@nullcheck
def op_gte(l, r):
    return l >= r


@nullcheck
def op_cast(l, r):
    return r(l)


def op_is(l, r):
    return l is r


def op_is_not(l, r):
    return l is not r


OP_MAP = {
    "+": op_add,
    "-": op_sub,
    "*": op_mul,
    "/": op_div,
    "AND": op_and,
    "OR": op_or,
    "=": op_eq,
    "!=": op_neq,
    "IS": op_is,
    "IS_NOT": op_is_not,
    "<": op_lt,
    ">": op_gt,
    "<=": op_lte,
    ">=": op_gte,
    "::": op_cast,
}
