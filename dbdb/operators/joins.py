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


JoinTypeNames = {
    JoinType.INNER: "Inner",
    JoinType.LEFT_OUTER: "Left Outer",
    JoinType.RIGHT_OUTER: "Right Outer",
    JoinType.FULL_OUTER: "Full Outer",
    JoinType.NATURAL: "Natural",
    JoinType.CROSS: "Cross Join",
}


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

    def name(self):
        return "Join"

    def details(self):
        return {
            "type": JoinTypeNames[self.config.join_type]
        }

    async def run(self, left_rows, right_rows):
        self.stats.update_start_running()
        iterator = self._join(left_rows, right_rows)
        self.iterator = iterator
        return Rows.merge([left_rows, right_rows], iterator)


class NestedLoopJoinOperator(JoinOperator):
    def name(self):
        return "Nested Loop Join"

    async def _join(self, left_values, right_values):
        # Unfortunate thing: we need to materialize the
        # left and right iterators in order to loop over
        # them multiple times. I guess that's fine...?
        lvals = await left_values.materialize()
        rvals = await right_values.materialize()

        # Handle cross join
        if self.config.join_type == JoinType.CROSS:
            async for row in self.cross_join(lvals, rvals):
                yield row
            return
        elif self.config.join_type == JoinType.FULL_OUTER:
            raise NotImplementedError()

        is_outer = False
        if self.config.join_type == JoinType.LEFT_OUTER:
            is_outer = True
        if self.config.join_type == JoinType.RIGHT_OUTER:
            raise RuntimeError("right joins are not supported, sorry")
            lvals, rvals = rvals, lvals
            is_outer = True

        for i, lval in enumerate(lvals):
            # self.stats.update_row_processed(lval)
            matched = False
            for rval in rvals:
                # self.stats.update_row_processed(rval)
                merged = lval.merge(rval)
                if self.config.expression.eval(merged):
                    matched = True
                    yield merged
                    # self.stats.update_row_emitted(merged)

            # If there was not match & it's an outer join,
            # emit a row w/ right side null
            if not matched and is_outer:
                # TODO : Flip for right outer join...
                merged = lval.as_tuple() + right_values.nulls()
                # self.stats.update_row_emitted(merged)
                yield merged

        self.stats.update_done_running()


class HashJoinOperator(JoinOperator):
    def name(self):
        return "Hash Join"

    def _hash_to_list(self, rows, expr):
        hashed = {}
        for row in rows:
            val = expr.evaluate(row)
            if val not in hashed:
                hashed[val] = []

            hashed[val].append(row)
        return hashed

    async def _join(self, left_values, right_values):
        equality = self.config.expression

        if equality.equality != EqualityTypes.EQ:
            raise RuntimeError(
                "HashJoin is only implemented for equi-joins. This is"
                f" a join with operator: {equality.equality}"
            )

        lvals = await left_values.materialize()
        lhash = await self._hash_to_list(lvals, equality.lexpr)

        rvals = right_values.materialize()
        rhash = self._hash_to_list(rvals, equality.rexpr)

        # TODO : I think i can/should change this to only hash one side up-front...
        for key, lrows in lhash.items():
            rrows = rhash.get(key, [])
            for lval in lrows:
                self.stats.update_row_processed(lval)
                included = False
                for rval in rrows:
                    self.stats.update_row_processed(rval)
                    included = True
                    merged = lval.merge(rval)
                    yield merged
                    self.stats.update_row_emitted(merged)

                # TODO : Does this work?
                if not self.config.inner and not included:
                    yield lval.as_tuple() + right_values.nulls()
                    self.stats.update_row_emitted(merged)

        self.stats.update_done_running()


class MergeJoinOperator(JoinOperator):
    def name(self):
        return "Merge Join"

    def _join(self, left_values, right_values):
        raise NotImplementedError()
