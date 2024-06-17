from __future__ import annotations
from dbdb.expressions.math import OP_MAP
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbdb.tuples.context import ExecutionContext


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

    def eval(self, context: ExecutionContext):
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
        return ColumnIdentifier(table=self.table, column=self.column)

    def qualify(self):
        if self.table:
            name = f"{self.table}.{self.column}"
        else:
            name = self.column

        return name

    def eval(self, context: ExecutionContext):
        return context.row.field(self._qualified)

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        name = self.qualify()
        return {name}

    def __str__(self):
        return self.qualify()


class Literal(Expression):
    def __init__(self, val):
        self.val = val

    def copy(self):
        return Literal(val=self.val)

    def eval(self, context: ExecutionContext):
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

    def eval(self, context: ExecutionContext):
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

        self.processor = func_class(expr=self.func_expr)

    def eval(self, context: ExecutionContext):
        return self.processor.eval(context)

    def copy(self):
        return ScalarFunctionCall(
            func_name=self.func_name,
            func_expr=self.func_expr.copy(),
            func_class=self.func_class,
        )

    def get_aggregated_fields(self):
        aggs = set()
        for expr in self.func_expr:
            aggs.update(expr.get_aggregated_fields())
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
        self.processor = func_class(
            expr=self.func_expr, modifiers={"DISTINCT": is_distinct}
        )

    def copy(self):
        return AggregateFunctionCall(
            func_name=self.func_name,
            func_expr=self.func_expr.copy(),
            func_class=self.func_class,
            is_distinct=self.is_distinct,
        )

    def eval(self, context: ExecutionContext):
        if not self.is_started:
            self.is_started = True
            self.processor.start()

        return self.processor.eval(context)

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


class WindowFunctionCall(Expression):
    def __init__(
        self,
        func_name,
        func_expr,
        func_class,
        partition_cols,
        order_cols,
        frame_start,
        frame_end,
    ):
        self.func_name = func_name
        self.func_expr = func_expr
        self.func_class = func_class

        if not self.func_class:
            raise RuntimeError(f"Window function {self.func_name} not found")

        self.processor = func_class(
            expr=func_expr,
            partition_cols=partition_cols,
            order_cols=order_cols,
            frame_start=frame_start,
            frame_end=frame_end,
        )

    def eval(self, context: ExecutionContext):
        return self.processor.eval(context)

    def result(self):
        return self.processor.result()

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        fields = set()
        for expr in self.func_expr:
            fields.update(expr.get_non_aggregated_fields())
            if len(expr.get_aggregated_fields()) > 0:
                raise RuntimeError(f"Cannot window an aggregate: {self.func_name}")
        return fields


class NegationOperator(Expression):
    def __init__(self, expr):
        self.expr = expr

    def copy(self):
        return NegationOperator(expr=self.expr)

    def eval(self, context: ExecutionContext):
        return -self.expr.eval(context)

    def get_aggregated_fields(self):
        return self.expr.get_aggregated_fields()

    def get_non_aggregated_fields(self):
        return self.expr.get_non_aggregated_fields()

    def __str__(self):
        return f"-{self.expr}"


class BinaryOperator(Expression):
    def __init__(self, lhs, operator, rhs):
        self.lhs = lhs
        self.operator = operator
        self.rhs = rhs

    def copy(self):
        return BinaryOperator(
            lhs=self.lhs.copy(), operator=self.operator, rhs=self.rhs.copy()
        )

    def short_circuit_and(self, context):
        lhs = bool(self.lhs.eval(context))
        if not lhs:
            return False
        rhs = bool(self.rhs.eval(context))
        return rhs

    def eval(self, context: ExecutionContext):
        # implement short-circuit for AND conjunctions
        if self.operator == "AND":
            return self.short_circuit_and(context)

        op = OP_MAP.get(self.operator)
        if not op:
            raise RuntimeError(f"Unexpected operator: {self.operator}")

        return op(self.lhs.eval(context), self.rhs.eval(context))

    def get_aggregated_fields(self):
        return self.lhs.get_aggregated_fields().union(self.rhs.get_aggregated_fields())

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
            when_exprs=[e.copy() for e in self.when_exprs],
            else_expr=self.else_expr.copy(),
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

    def eval(self, context: ExecutionContext):
        for when_cond, when_value in self.when_exprs:
            if when_cond.eval(context):
                return when_value.eval(context)

        return self.else_expr.eval(context)


class CastExpr(Expression):
    def __init__(self, ttype):
        self.ttype = ttype

    def copy(self):
        return CastExpr(ttype=self.ttype)

    def eval(self, context: ExecutionContext):
        return self.ttype

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        return set()

    @classmethod
    def make(cls, string, loc, toks):
        ttype = toks[0]

        if ttype.upper() == "INT":
            ttype = int
        elif ttype.upper() == "FLOAT":
            ttype = float
        elif ttype.upper() in ["STRING", "TEXT"]:
            ttype = str
        else:
            raise RuntimeError(f"Unknown type: {ttype}")

        return CastExpr(ttype)
