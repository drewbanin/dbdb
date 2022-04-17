from dbdb.operators.base import Operator, OperatorConfig
from dbdb.operators import functions
from dbdb.tuples.rows import Rows

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
        aggregates,
        group_by=None,
        project=None
    ):
        self.aggregates = aggregates

        # TODO: Validate & all that (where?)
        if group_by is None:
            self.group_by = []
        else:
            self.group_by = group_by

        self.project = project


class AggregateOperator(Operator):
    Config = AggregateConfig

    def grouping_set(self, values):
        groups = {}

        for value in values:
            key = tuple(expr_f(value) for expr_f in self.config.group_by)
            if key not in groups:
                groups[key] = []

            groups[key].append(value)

        return groups

    def make_iterator(self, grouping):
        for (key, values) in grouping.items():
            result = []
            for agg, expr in self.config.aggregates:
                aggregate_func = lookup(agg)

                # res is a scalar value
                res = aggregate_func(expr(val) for val in values)
                result.append(res)

            yield key + tuple(result)

    def run(self, rows):
        grouping = self.grouping_set(rows)
        iterator = self.make_iterator(grouping)
        return Rows(self.config.project, iterator)
