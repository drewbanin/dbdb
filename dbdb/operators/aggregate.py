from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows
from dbdb.tuples.identifiers import TableIdentifier

from collections import defaultdict
import enum
import itertools


def lookup(agg, is_distinct):
    func = _agg_funcs.get(agg)
    if func is None:
        raise RuntimeError(f"Aggregate {agg} is not implemented")
    elif agg == Aggregates.COUNT and is_distinct:
        return _agg_funcs[Aggregates.COUNTD]
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

    async def make_iterator(self, rows):
        projections = self.config.projections.projections

        group_projections = []
        agg_projections = []

        # Use this to "remember" the order of columns across agg'd
        # and group'd fields. There is probably a way smarter way
        # of doing this, but i am not smart enough to know what it is!
        column_agg_list = []

        for i, projection in enumerate(projections):
            if len(projection.get_non_aggregated_fields()) > 0:
                group_projections.append(projection)
                column_agg_list.append(True)

            else:
                agg_projections.append(projection)
                column_agg_list.append(False)

        group_exprs = dict()
        proj_results = dict()
        async for row in rows:
            self.stats.update_row_processed(row)

            grouping = tuple([proj.eval(row) for proj in group_projections])

            # It's a new grouping set
            if grouping not in group_exprs:
                for projection in agg_projections:
                    group_exprs[grouping] = [proj.copy() for proj in agg_projections]

            # Process incremental step for aggregate functions
            # This is kind of dumb because we are processing and _saving the results_
            # of the expression, even if it's an incomplete aggregation! I think that's
            # a singal that this isn't The Right Way To Do This. But, it does work because
            # the partial agg results are overwritten for each row, and the final output
            # for the last row is indeed the correct response to pass onto the next operator.
            #
            # My guess is that i should partition the execution graph up-front and then calculate
            # all of the aggs independently before computing scalar transformations over the results.
            for proj in group_exprs[grouping]:
                proj_results[proj] = proj.eval(row)

        for key, agg_projections in group_exprs.items():
            grouped = list(key)
            aggregated = [proj_results[p] for p in agg_projections]

            # Reconsitute an output row in the order described
            # by the input list of projections. Both groups and
            # aggs retain order, so we just need to splice them
            # together in the same order that they were pulled apart
            mapped = []
            for is_group in column_agg_list:
                if is_group:
                    mapped.append(grouped.pop(0))
                else:
                    mapped.append(aggregated.pop(0))

            yield mapped

        self.stats.update_done_running()

    def field_names(self, table):
        fields = []
        for i, field in enumerate(self.config.projections.projections):
            if field.alias:
                field_name = field.alias
            elif field.can_derive_name():
                field_name = field.make_name()
            else:
                field_name = f"col_{i + 1}"

            fields.append(table.field(field_name))
        return fields

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
                raise RuntimeError("Grouping by expressions is not current supported")

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

        table_identifier = TableIdentifier.temporary()
        fields = self.field_names(table_identifier)

        iterator = self.make_iterator(rows)
        self.iterator = iterator

        # This gets a temporary name because we do not know the name
        # of this table... in fact... there is none!? I might need to
        # think about that more because fields are still scoped to their
        # parent locations.. which is kind of confusing.... hm....

        return Rows(table_identifier, fields, iterator)
