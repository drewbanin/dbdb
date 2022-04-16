from dbdb.operators.base import Operator, OperatorConfig
from dbdb.operators import functions

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
        group_by=None
    ):
        self.aggregates = aggregates
        if group_by is None:
            self.group_by = []
        else:
            self.group_by = group_by


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

    def run(self, values):
        # iterators = itertools.tee(values, len(self.config.aggregates))

        grouping = self.grouping_set(values)

        for (key, values) in grouping.items():
            result = []
            for agg, expr in self.config.aggregates:
                aggregate_func = lookup(agg)

                # res is a scalar value
                res = aggregate_func(expr(val) for val in values)
                result.append(res)
            yield key + tuple(result)
