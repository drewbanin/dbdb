from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows
from dbdb.expressions import EqualityTypes

from enum import Enum

import enum
class JoinType(enum.Enum):
    INNER = enum.auto()
    LEFT_OUTER = enum.auto()
    RIGHT_OUTER = enum.auto()
    FULL_OUTER = enum.auto()
    NATURAL = enum.auto()
    CROSS = enum.auto()


class JoinStrategy(Enum):
    NestedLoop = 1
    HashJoin = 2

    def create(self, *args, **kwargs):
        if self == JoinStrategy.NestedLoop:
            JoinClass = NestedLoopJoinOperator
        elif self == JoinStrategy.HashJoin:
            JoinClass = HashJoinOperator
        else:
            raise NotImplementedError()

        return JoinClass(*args, **kwargs)


class JoinConfig(OperatorConfig):
    def __init__(
        self,
        # TODO: Make an enum of options?
        join_type,
        expression,
    ):
        self.join_type = join_type
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

        # Handle cross join
        if self.config.join_type == JoinType.CROSS:
            yield from self.cross_join(lvals, rvals)
            return
        elif self.config.join_type == JoinType.FULL_OUTER:
            yield from self.full_outer_join(lvals, rvals)

        is_outer = False
        if self.config.join_type == JoinType.LEFT_OUTER:
            is_outer = True
        if self.config.join_type == JoinType.RIGHT_OUTER:
            lvals, rvals = rvals, lvals
            is_outer = True

        yielded = 0
        for lval in lvals:
            for rval in rvals:
                merged = lval.merge(rval)
                if self.config.expression.eval(merged):
                    yield merged
                    yielded += 1
                elif is_outer:
                    yield lval.as_tuple() + rval.nulls()
                    yielded += 1


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
                    merged = lval.merge(rval)
                    yield merged

                # TODO: Handle different join types here?
                import ipdb; ipdb.set_trace()
                if not self.config.inner and not included:
                    yield lval.as_tuple() + right_values.nulls()


class MergeJoinOperator(JoinOperator):
    def _join(self, left_values, right_values):
        raise NotImplementedError()
