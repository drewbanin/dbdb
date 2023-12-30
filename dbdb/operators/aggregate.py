from dbdb.operators.base import Operator, OperatorConfig
from dbdb.operators import aggregations
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
    Aggregates.MIN: aggregations.AggregationMin,
    Aggregates.MAX: aggregations.AggregationMax,
    Aggregates.SUM: aggregations.AggregationSum,
    Aggregates.AVG: aggregations.AggregationAverage,

    Aggregates.COUNT: aggregations.AggregationCount,
    Aggregates.COUNTD: aggregations.AggregationCountDistinct,

    Aggregates.LISTAGG: aggregations.AggregationListAgg,
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
        group_by_list,
        projections,
    ):
        self.group_by_list = group_by_list
        self.projections = projections


class AggregateOperator(Operator):
    Config = AggregateConfig

    def name(self):
        return "Aggregate"

    def grouping_set(self, exprs, values):
        groups = {}

        for value in values:
            key = tuple(expr.eval(value) for expr in exprs)
            if key not in groups:
                groups[key] = []

            groups[key].append(value)

        return groups

    async def make_iterator(self, scalar_fields, grouping, rows):
        projections = self.config.projections.projections

        resolve_funcs = []
        for i, projection in enumerate(projections):
            if i not in scalar_fields:
                agg_class = lookup(projection.expr.agg_type)
                resolve_funcs.append(agg_class)

        grouped_sets = {}
        async for row in rows:
            self.stats.update_row_processed(row)
            grouping_values = []
            agg_values = []
            for i, projection in enumerate(projections):
                if i in scalar_fields:
                    value = projection.expr.eval(row)
                    grouping_values.append(value)
                else:
                    # Calc value inside the func - this is dumb
                    # would be better to make the agg func do this
                    values = [fe.eval(row) for fe in projection.expr.func_expr]
                    agg_values.append(values)

            grouping_set = tuple(grouping_values)
            if grouping_set not in grouped_sets:
                grouped_sets[grouping_set] = [agg() for agg in resolve_funcs]

            resolver = grouped_sets[grouping_set]

            for (agg, value) in zip(resolver, agg_values):
                agg.process(value)

        # OK - I now understand why we implemented this without
        # a notion of iterator streaming before -- we cannot return
        # any results until the aggregation has completed! how could
        # we? Maybe if there is an ANY_VALUE() function we could return
        # early or something... could also apply this for window functions
        # in the future too... but this was kind of a big waste of time lmao

        # TODO : this is dumb and assumed groups will come before aggs
        for (grouped, agged) in grouped_sets.items():
            grouped_values = list(grouped)
            agged_values = [agg.result() for agg in agged]
            merged = tuple(grouped_values + agged_values)
            yield merged
            self.stats.update_row_emitted(merged)

        self.stats.update_done_running()

    async def run(self, rows):
        self.stats.update_start_running()
        from dbdb.lang.lang import Literal
        # Check group by fields against aggregate fields
        # and make sure it all tracks

        groupings = self.config.group_by_list
        projections = self.config.projections.projections

        grouped_fields = set()
        for grouping in groupings:
            if isinstance(grouping, Literal) and grouping.is_int():
                # GROUP BY <INT>
                index = grouping.value()
                # TODO: Check for out of range
                group_field = projections[index - 1]
                fields_to_group = group_field.get_non_aggregated_fields()
                agg_fields = group_field.get_aggregated_fields()
                if len(agg_fields) > 0:
                    raise RuntimeError("you're bad at writing SQL")

                grouped_fields.update(fields_to_group)
            else:
                # GROUP BY <EXPR>
                import ipdb; ipdb.set_trace()
                pass

        scalar_fields = []

        for i, projection in enumerate(projections):
            # Check that referenced fields are either grouped or aggregated...
            # Also, no expr should have both agg'd and unagg'd fields...

            aggregated = projection.get_aggregated_fields()
            scalar = projection.get_non_aggregated_fields()

            if len(scalar) > 0 and len(aggregated) > 0:
                raise RuntimeError("You're bad at writing SQL")

            for field in scalar:
                if field not in grouped_fields:
                    raise RuntimeError(
                        f"Field {field} is neither grouped"
                        " nor aggregated"
                    )

            if len(scalar) > 0:
                scalar_fields.append(i)

        iterator = self.make_iterator(scalar_fields, grouping, rows)
        self.iterator = iterator

        # This gets a temporary name because we do not know the name
        # of this table... in fact... there is none!? I might need to
        # think about that more because fields are still scoped to their
        # parent locations.. which is kind of confusing.... hm....
        table_identifier = TableIdentifier.temporary()
        field_names = [p.alias for p in self.config.projections.projections]
        fields = [table_identifier.field(f) for f in field_names]
        return Rows(table_identifier, fields, iterator)
