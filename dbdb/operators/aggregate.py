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
        group_by_list,
        projections,
    ):
        self.group_by_list = group_by_list
        self.projections = projections


class AggregateOperator(Operator):
    Config = AggregateConfig

    def grouping_set(self, exprs, values):
        groups = {}

        for value in values:
            key = tuple(expr.eval(value) for expr in exprs)
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

        for projection in projections:
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
                scalar_fields.append(projection.expr)

        grouping = self.grouping_set(scalar_fields, rows)
        # TODO : You stopped here...
        iterator = self.make_iterator(grouping)

        # This gets a temporary name because we do not know the name
        # of this table... in fact... there is none!? I might need to
        # think about that more because fields are still scoped to their
        # parent locations.. which is kind of confusing.... hm....
        table_identifier = TableIdentifier.temporary()
        fields = [table_identifier.field(f) for f in field_names]
        return Rows(table_identifier, fields, iterator)
