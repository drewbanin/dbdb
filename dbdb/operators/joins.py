from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows
from dbdb.expressions import EqualityTypes


class JoinConfig(OperatorConfig):
    def __init__(
        self,
        # TODO: Make an enum of options?
        inner=True,
        expression=None,
    ):
        self.inner = inner
        self.expression = expression


class JoinOperator(Operator):
    Config = JoinConfig

    def run(self, left_rows, right_rows):
        iterator = self._join(left_rows, right_rows)
        return Rows.merge([left_rows, right_rows], iterator)


class NestedLoopJoinOperator(JoinOperator):
    def _join(self, left_values, right_values):
        # Unfortunate thing: we need to materialize the
        # left and right iterators in order to loop over
        # them multiple times. I guess that's fine...?
        lvals = left_values.materialize()
        rvals = right_values.materialize()

        for lval in lvals:
            included = False
            for rval in rvals:
                merged = Rows.merge_rows(lval, rval)
                if self.config.expression.evaluate(merged):
                    included = True
                    yield merged

            if not self.config.inner and not included:
                yield lval + right_values.nulls()


class HashJoinOperator(JoinOperator):
    def _hash_to_list(self, rows, expr):
        hashed = {}
        for row in rows:
            val = expr.evaluate(row)
            if val not in hashed:
                hashed[val] = []

            hashed[val].append(row)
        return hashed

    def _join(self, left_values, right_values):
        equality = self.config.expression

        if equality.equality != EqualityTypes.EQ:
            raise RuntimeError(
                "HashJoin is only implemented for equi-joins. This is"
                f" a join with operator: {equality.equality}"
            )

        lvals = left_values.materialize()
        lhash = self._hash_to_list(lvals, equality.lexpr)

        rvals = right_values.materialize()
        rhash = self._hash_to_list(rvals, equality.rexpr)

        for key, lrows in lhash.items():
            rrows = rhash.get(key, [])
            for lval in lrows:
                included = False
                for rval in rrows:
                    included = True
                    merged = Rows.merge_rows(lval, rval)
                    yield merged

                if not self.config.inner and not included:
                    yield lval + right_values.nulls()


class MergeJoinOperator(JoinOperator):
    def _join(self, left_values, right_values):
        raise NotImplementedError()
