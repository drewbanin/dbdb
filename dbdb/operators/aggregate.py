from dbdb.operators.base import Operator, OperatorConfig
from dbdb.operators import functions
from dbdb.tuples.rows import Rows
from dbdb.tuples.identifiers import TableIdentifier

import enum
import itertools


class Aggregates(enum.Enum):
    MIN = enum.auto()
    MAX = enum.auto()
    SUM = enum.auto()
    AVG = enum.auto()
    COUNT = enum.auto()
    COUNTD = enum.auto()
    LISTAGG = enum.auto()

    SCALAR = enum.auto()


_agg_funcs = {
    Aggregates.MIN: functions.agg_min,
    Aggregates.MAX: functions.agg_max,
    Aggregates.SUM: functions.agg_sum,
    Aggregates.AVG: functions.agg_avg,

    Aggregates.COUNT: functions.agg_count,
    Aggregates.COUNTD: functions.agg_countd,

    Aggregates.LISTAGG: functions.agg_list,
}


def lookup(agg):
    func = _agg_funcs.get(agg)
    if func is None:
        raise RuntimeError(f"Aggregate {agg} is not implemented")
    return func


Identity = object()


class AggregateConfig(OperatorConfig):
    def __init__(
        self,
        fields,
    ):
        self.fields = fields


class AggregateOperator(Operator):
    Config = AggregateConfig

    def grouping_set(self, exprs, values):
        groups = {}

        for value in values:
            key = tuple(expr_f(value) for _, expr_f, _ in exprs)
            if key not in groups:
                groups[key] = []

            groups[key].append(value)

        return groups

    def make_iterator(self, grouping):
        for (key, values) in grouping.items():
            result = []
            scalar_index = 0
            for agg_type, expr, project in self.config.fields:
                if agg_type == Aggregates.SCALAR:
                    result.append(key[scalar_index])
                    scalar_index += 1
                else:
                    aggregate_func = lookup(agg_type)
                    # res is a scalar value
                    res = aggregate_func(expr(val) for val in values)
                    result.append(res)

            yield tuple(result)

    def run(self, rows):
        scalars = [p for p in self.config.fields if p[0] == Aggregates.SCALAR]
        projection = tuple([project for _, _, project in self.config.fields])

        grouping = self.grouping_set(scalars, rows)
        iterator = self.make_iterator(grouping)

        # This gets a temporary name because we do not know the name
        # of this table... in fact... there is none!? I might need to
        # think about that more because fields are still scoped to their
        # parent locations.. which is kind of confusing.... hm....
        table_identifier = TableIdentifier.temporary()
        return Rows(projection, iterator, table_identifier)
