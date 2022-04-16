from dbdb.operators.base import Operator, OperatorConfig


class JoinConfig(OperatorConfig):
    def __init__(
        self,
        # TODO: Make an enum of options?
        inner=True,
        expression=None
    ):
        self.inner = inner
        self.expression = expression


class JoinOperator(Operator):
    Config = JoinConfig

    def run(self, left_values, right_values):
        yield from self._join(left_values, right_values)


class NestedLoopJoinOperator(JoinOperator):
    def _join(self, left_values, right_values):
        for lval in left_values:
            for rval in right_values:
                if self.config.expression(lval, rval):
                    yield lval + rval


class HashJoinOperator(JoinOperator):
    def _join(self, left_values, right_values):
        raise NotImplementedError()


class MergeJoinOperator(JoinOperator):
    def _join(self, left_values, right_values):
        raise NotImplementedError()
