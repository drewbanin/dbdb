

import functools
from dbdb.operators.aggregate import Aggregates


class ASTToken:
    def has_aggregate(self):
        return False


class ColumnIdentifier(ASTToken):
    def __init__(self, table, column):
        self.table = table
        self.column = column

        self._qualified = self.qualify()

    def qualify(self):
        if self.table:
            name = f"{self.table}.{self.column}"
        else:
            name = self.column

        return name

    def eval(self, row):
        return row.field(self._qualified)

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        name = self.qualify()
        return {name}

    def __str__(self):
        return self.qualify()


class TableIdent(ASTToken):
    def __init__(self, table_name, alias=None):
        self.table_name = table_name
        self.alias = alias

    def eval(self, row):
        # If we call this, something bad happened...
        raise NotImplementedError()


class Literal(ASTToken):
    def __init__(self, val):
        self.val = val

    def eval(self, row):
        return self.val

    def value(self):
        return self.val

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        return set()

    def is_int(self):
        return isinstance(self.val, int)

    def __str__(self):
        return f"{self.val}[{type(self.val)}]"


class FunctionCall(ASTToken):
    def __init__(self, func_name, func_expr, agg_type):
        self.func_name = func_name
        self.func_expr = func_expr
        self.agg_type = agg_type

    def eval(self, row):
        from dbdb.operators.functions import find_func

        func = find_func(self.func_name)
        value = func.eval(self.func_expr, row)
        return value

    def get_aggregated_fields(self):
        # If this is a scalar function, return the set of fields
        # that are aggregated within the function expression
        if self.agg_type == Aggregates.SCALAR:
            return self.func_expr.get_aggregated_fields()
        else:
            # If it's an aggregate function, then confirm that the func_expr
            # is _not_ also an aggregate. Otherwise, return the non-agg fields
            # contained within the function expression
            scalar_fields = set()

            for expr in self.func_expr:
                aggs = expr.get_aggregated_fields()
                if len(aggs) > 0:
                    raise RuntimeError("Tried to agg an agg")

                scalars = expr.get_non_aggregated_fields()
                scalar_fields.update(scalars)

            # So these are the un-agg fields that become aggregated via being
            # contained within this function
            return scalar_fields

    def get_non_aggregated_fields(self):
        if self.agg_type == Aggregates.SCALAR:
            return self.func_expr.get_non_aggregated_fields()
        else:
            return set()


# Operators!
def op_add(l, r):
    return l + r

def op_sub(l, r):
    return l - r

def op_mul(l, r):
    return l * r

def op_div(l, r):
    return l / r

def op_and(l, r):
    return l and r

def op_or(l, r):
    return l or r

def op_eq(l, r):
    return l == r

def op_neq(l, r):
    return l != r

def op_is(l, r):
    return l is r

def op_is_not(l, r):
    return l is not r

def op_lt(l, r):
    return l < r

def op_gt(l, r):
    return l > r

def op_lte(l, r):
    return l <= r

def op_gte(l, r):
    return l >= r

def op_cast(l, r):
    return r(l)


OP_MAP = {
    '+': op_add,
    '-': op_sub,
    '*': op_mul,
    '/': op_div,
    'AND': op_and,
    'OR': op_or,
    '=': op_eq,
    '!=': op_neq,
    'IS': op_is,
    'IS_NOT': op_is_not,
    "<": op_lt,
    ">": op_gt,
    "<=": op_lte,
    ">=": op_gte,
    '::': op_cast,
}


class BinaryOperator:
    def __init__(self, lhs, operator, rhs):
        self.lhs = lhs
        self.operator = operator
        self.rhs = rhs

    def short_circuit_and(self, row):
        lhs = bool(self.lhs.eval(row))
        if not lhs:
            return False
        rhs = bool(self.rhs.eval(row))
        return rhs

    def eval(self, row):
        # implement short-circuit for AND conjunctions
        if self.operator == "AND":
            return self.short_circuit_and(row)

        op = OP_MAP.get(self.operator)
        if not op:
            raise RuntimeError(f"Unexpected operator: {self.operator}")

        # print("OPERATION", id(self), id(row), self.lhs, self.operator, self.rhs)
        return op(
            self.lhs.eval(row),
            self.rhs.eval(row)
        )

    def get_aggregated_fields(self):
        return self.lhs.get_aggregated_fields().union(
            self.rhs.get_aggregated_fields()
        )

    def get_non_aggregated_fields(self):
        return self.lhs.get_non_aggregated_fields().union(
            self.rhs.get_non_aggregated_fields()
        )

    def __str__(self):
        return f"({self.lhs} {self.operator} {self.rhs})"


class CaseWhen(ASTToken):
    def __init__(self, when_exprs, else_expr):
        self.when_exprs = when_exprs
        self.else_expr = else_expr

    def eval(self, row):
        for (when_cond, when_value) in self.when_exprs:
            if when_cond.eval(row):
                return when_value.eval(row)

        return self.else_expr.eval(row)


class CastExpr(ASTToken):
    def __init__(self, ttype):
        self.ttype = ttype

    def eval(self, row):
        return self.ttype

    @classmethod
    def make(cls, string, loc, toks):
        ttype = toks[0]

        if ttype.upper() == 'INT':
            ttype = int
        elif ttype.upper() == 'FLOAT':
            ttype = float
        elif ttype.upper() in ['STRING', 'TEXT']:
            ttype = str
        else:
            raise RuntimeError(f"Unknown type: {ttype}")

        return CastExpr(ttype)


class JoinClause(ASTToken):
    def __init__(self, to, join_type, on):
        self.to = to
        self.join_type = join_type
        self.on = on

    @classmethod
    def new_qualified(cls, to, join_type, on):
        return cls(to, join_type, on)

    @classmethod
    def new_unqualified(cls, to, join_type):
        return cls(to, join_type, on=True)


class JoinCondition:
    def __init__(self, join_type, join_expr):
        self.join_type = join_type
        self.join_expr = join_expr

    def eval(self, row):
        return self.join_expr.eval(row)

    @classmethod
    def make_from_on_expr(cls, tokens):
        # ON <expr>
        return JoinCondition('ON', tokens[1])

    @classmethod
    def make_from_using_expr(cls, tokens):
        # USING ( <field list> )
        # Build our own expression...
        # TODO: What is the LHS and RHS???
        join_expr = None
        return JoinCondition('USING', join_expr)


