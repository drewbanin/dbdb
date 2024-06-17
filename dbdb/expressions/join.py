from dbdb.expressions.expressions import Expression
from dbdb.tuples.context import ExecutionContext


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


class JoinCondition: ...


class JoinConditionOn(JoinCondition):
    def __init__(self, join_expr):
        self.join_expr = join_expr

    def eval(self, context: ExecutionContext):
        return self.join_expr.eval(context)

    @classmethod
    def from_tokens(cls, toks):
        on, expr = toks
        return JoinConditionOn(expr)


class JoinConditionUsing(JoinConditionOn):
    def __init__(self, fields):
        self.fields = fields

    def eval(self, context: ExecutionContext):
        for field in self.fields:
            values = list(context.row.iter_values_for_field(field))
            if len(values) != 2:
                raise RuntimeError(
                    f"Unexpected results while processing join: {values}, {self.fields}"
                )

            if values[0] != values[1]:
                return False

        return True

    @classmethod
    def from_tokens(cls, toks):
        return JoinConditionUsing(toks.fields)
