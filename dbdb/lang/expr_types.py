import functools


class Expression:
    "Base interface for all expressions"
    def copy(self):
        raise NotImplementedError()

    def get_aggregated_fields(self):
        raise NotImplementedError()

    def make_name(self):
        raise NotImplementedError()

    def can_derive_name(self):
        return False

    def get_non_aggregated_fields(self):
        raise NotImplementedError()

    def eval(self, row):
        raise NotImplementedError()

    def result(self):
        # Only implemented for aggregate types
        raise NotImplementedError()


class ColumnIdentifier(Expression):
    def __init__(self, table, column):
        self.table = table
        self.column = column

        self._qualified = self.qualify()

    def make_name(self):
        return self.column

    def can_derive_name(self):
        return True

    def copy(self):
        return ColumnIdentifier(
            table=self.table,
            column=self.column
        )

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


# TODO : Should this derive from expression? probably not...
class TableIdent(Expression):
    def __init__(self, table_name, schema=None, database=None, alias=None):
        self.table_name = table_name
        self.schema = schema
        self.database = database

        self.alias = alias

class Literal(Expression):
    def __init__(self, val):
        self.val = val

    def copy(self):
        return Literal(val=self.val)

    def eval(self, row):
        return self.val

    def value(self):
        return self.val

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        return set()

    def get_type(self):
        return type(self.val).__name__

    def is_int(self):
        return isinstance(self.val, int)

    def is_string(self):
        return isinstance(self.val, str)

    def __str__(self):
        return f"{self.val}[{type(self.val)}]"


class Null(Literal):
    def __init__(self):
        self.val = None

    def copy(self):
        return Null()


class Star(Expression):
    def __init__(self):
        self.val = None

    def copy(self):
        return Star()

    def eval(self, row):
        return self.val

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        return set()


class ScalarFunctionCall(Expression):
    def __init__(self, func_name, func_expr, func_class):
        self.func_name = func_name
        self.func_expr = func_expr
        self.func_class = func_class

        self.processor = func_class()

    def eval(self, row):
        return self.processor.eval(self.func_expr, row)

    def copy(self):
        return ScalarFunctionCall(
            func_name = self.func_name,
            func_expr = self.func_expr.copy(),
            func_class = self.func_class
        )

    def get_aggregated_fields(self):
        aggs = set()
        for expr in self.func_expr:
            aggs.update(expr.get_non_aggregated_fields())
        return aggs

    def get_non_aggregated_fields(self):
        scalars = set()
        for expr in self.func_expr:
            scalars.update(expr.get_non_aggregated_fields())
        return scalars


class AggregateFunctionCall(Expression):
    def __init__(self, func_name, func_expr, func_class, is_distinct):
        self.func_name = func_name
        self.func_expr = func_expr
        self.func_class = func_class
        self.is_distinct = is_distinct

        self.is_started = False
        self.processor = func_class({
            "DISTINCT": is_distinct
        })

    def copy(self):
        return AggregateFunctionCall(
            func_name = self.func_name,
            func_expr = self.func_expr.copy(),
            func_class = self.func_class,
            is_distinct = self.is_distinct
        )

    def eval(self, row):
        if not self.is_started:
            self.is_started = True
            self.processor.start()

        return self.processor.eval(self.func_expr, row)

    def result(self):
        return self.processor.result()

    def get_aggregated_fields(self):
        aggs = set()
        for expr in self.func_expr:
            aggs.update(expr.get_non_aggregated_fields())
            if len(expr.get_aggregated_fields()) > 0:
                raise RuntimeError(f"Cannot aggregate an aggregate: {self.func_name}")

        return aggs

    def get_non_aggregated_fields(self):
        return set()


class TableFunctionCall:
    def __init__(self, func_name, func_expr, func_class):
        self.func_name = func_name
        self.func_expr = func_expr
        self.func_class = func_class


def nullcheck(inner):
    def check_nulls(*args):
        if None in args:
            return None
        else:
            return inner(*args)
    return check_nulls

# Operators!
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


class NegationOperator:
    def __init__(self, expr):
        self.expr = expr

    def copy(self):
        return NegationOperator(expr=self.expr)

    def eval(self, row):
        return -self.expr.eval(row)

    def get_aggregated_fields(self):
        return self.expr.get_aggregated_fields()

    def get_non_aggregated_fields(self):
        return self.expr.get_non_aggregated_fields()

    def __str__(self):
        return f"-{self.expr}"


class BinaryOperator:
    def __init__(self, lhs, operator, rhs):
        self.lhs = lhs
        self.operator = operator
        self.rhs = rhs

    def copy(self):
        return BinaryOperator(
            lhs = self.lhs.copy(),
            operator = self.operator,
            rhs = self.rhs.copy()
        )

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


class CaseWhen(Expression):
    def __init__(self, when_exprs, else_expr):
        self.when_exprs = when_exprs
        self.else_expr = else_expr

    def copy(self):
        return CaseWhen(
            when_exprs = [e.copy() for e in self.when_exprs],
            else_expr = self.else_expr.copy()
        )

    def iter_exprs(self):
        for expr in self.when_exprs + [self.else_expr]:
            yield expr

    def get_aggregated_fields(self):
        fields = set()
        for expr in self.iter_exprs():
            fields.update(expr.get_aggregated_fields())
        return fields

    def get_non_aggregated_fields(self):
        fields = set()
        for expr in self.iter_exprs():
            fields.update(expr.get_non_aggregated_fields())
        return fields

    def eval(self, row):
        for (when_cond, when_value) in self.when_exprs:
            if when_cond.eval(row):
                return when_value.eval(row)

        return self.else_expr.eval(row)


class CastExpr(Expression):
    def __init__(self, ttype):
        self.ttype = ttype

    def copy(self):
        return CastExpr(ttype=self.ttype)

    def eval(self, row):
        return self.ttype

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        return set()

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


class JoinClause(Expression):
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
    ...

class JoinConditionOn(JoinCondition):
    def __init__(self, join_expr):
        self.join_expr = join_expr

    def eval(self, row):
        return self.join_expr.eval(row)

    @classmethod
    def from_tokens(cls, toks):
        on, expr = toks
        return JoinConditionOn(expr)


class JoinConditionUsing(JoinConditionOn):
    def __init__(self, fields):
        self.fields = fields

    def eval(self, row):
        field_values = {}

        for field in self.fields:
            values = list(row.iter_values_for_field(field))
            if len(values) != 2:
                raise RuntimeError(f"Unexpected results while processing join: {values}, {self.fields}")

            if values[0] != values[1]:
                return False

        return True

    @classmethod
    def from_tokens(cls, toks):
        return JoinConditionUsing(toks.fields)
