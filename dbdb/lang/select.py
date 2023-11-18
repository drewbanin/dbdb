from dbdb.operators.file_operator import TableScanOperator, TableGenOperator
from dbdb.operators.sorting import SortOperator
from dbdb.operators.limit import LimitOperator
from dbdb.operators.filter import FilterOperator
from dbdb.operators.project import ProjectOperator
from dbdb.operators.joins import (
    NestedLoopJoinOperator,
    HashJoinOperator,
    JoinStrategy,
    JoinType
)

from dbdb.operators.aggregate import AggregateOperator, Aggregates
from dbdb.tuples.rows import Rows
from dbdb.tuples.identifiers import TableIdentifier, FieldIdentifier
from dbdb.expressions import Expression, Equality, EqualityTypes

import networkx as nx


class Select:
    def __init__(
        self,
        projections,
        where=None,
        source=None,
        joins=None,
        group_by=None,
        having=None,
        order_by=None,
        limit=None,
    ):
        self.projections = projections
        self.where = where
        self.source = source
        self.joins = joins or []
        self.group_by = group_by
        self.order_by = order_by
        self.limit = limit

    def make_plan(self):
        """
        Order of operations:
        1. Table scan from FROM clause
        2. Apply JOINs
        3. Apply WHERE
        5. Apply projections (including aggregates)
        6. Apply ORDER clause
        7. Apply LIMIT
        """

        plan = nx.DiGraph()

        # FROM
        source_op = self.source.as_operator()
        plan.add_node(source_op, label="FROM", is_root=True)

        # JOIN
        table_op = source_op
        for join in self.joins:
            join_op = join.as_operator()
            plan.add_node(join_op, label="JOIN")
            plan.add_edge(table_op, join_op, input_arg="left_rows")

            join_to_op = join.to.as_operator()
            # TODO: How do we make this its own node?
            plan.add_node(join_to_op, label="FROM (join)", is_root=True)
            plan.add_edge(join_to_op, join_op, input_arg="right_rows")

            # Future operations are on the output of this operation
            table_op = join_op

        # WHERE
        if self.where:
            filter_op = self.where.as_operator()
            plan.add_node(filter_op, label="Filter")
            plan.add_edge(table_op, filter_op, input_arg="rows")
            table_op = filter_op

        # GROUP BY
        if self.group_by:
            aggregate_op = self.group_by.as_operator()
            plan.add_node(aggregate_op, label="Aggregate")
            plan.add_edge(table_op, aggregate_op, input_arg="rows")
            table_op = aggregate_op
            # We can do the actual grouping here.... should this
            # just be it's own operator? why not??????
            # I feel like i already tried this though...
        else:
            # Scalar projections
            project_op = self.projections.as_operator()
            plan.add_node(project_op, label="Project")
            plan.add_edge(table_op, project_op, input_arg="rows")
            table_op = project_op

        if self.order_by:
            order_by_op = self.order_by.as_operator()
            plan.add_node(order_by_op, label="Order")
            plan.add_edge(table_op, order_by_op, input_arg="rows")
            table_op = order_by_op

        if self.limit:
            limit_op = self.limit.as_operator()
            plan.add_node(limit_op, label="Limit")
            plan.add_edge(table_op, limit_op, input_arg="rows")
            table_op = limit_op

        return plan

    def __str__(self):
        return f"""
        Projections: {self.projections}
        Filters: {self.where}
        Source: {self.source}
        Joins: {self.joins}
        Order: {self.order_by}
        Limit: {self.limit}
        """


class SelectClause:
    def as_operator(self):
        raise NotImplementedError()


class SelectList(SelectClause):
    def __init__(self, projections):
        self.projections = projections

    def as_operator(self):
        return ProjectOperator(
            project=self.projections
        )


class SelectProjection(SelectClause):
    def __init__(self, expr, alias):
        self.expr = expr
        self.alias = alias

    def get_aggregated_fields(self):
        return self.expr.get_aggregated_fields()

    def get_non_aggregated_fields(self):
        return self.expr.get_non_aggregated_fields()


class SelectFilter(SelectClause):
    def __init__(self, expr):
        self.expr = expr

    def as_operator(self):
        return FilterOperator(
            predicate=self.expr
        )


class SelectFileSource(SelectClause):
    def __init__(self, file_path, table_identifier, columns):
        self.file_path = file_path
        self.table_identifier = table_identifier
        self.columns = columns

    def as_operator(self):
        return TableScanOperator(
            table_ref=self.file_path,
            table=self.table_identifier,
            columns=self.columns
        )


class SelectMemorySource(SelectClause):
    def __init__(self, table_identifier, rows):
        self.table_identifier = table_identifier
        self.rows = rows

    def as_operator(self):
        return TableGenOperator(
            table=self.table_identifier,
            rows=self.rows
        )


class SelectJoin(SelectClause):
    def __init__(self, to, expression, join_type, join_strategy):
        self.to = to
        self.expression = expression
        self.join_type = join_type
        self.join_strategy = join_strategy

    def as_operator(self):
        return self.join_strategy.create(
            join_type=self.join_type,
            expression=self.expression
        )

    @classmethod
    def new(cls, to, expression, join_type):
        # TODO: Pick this dynamically?
        join_strategy = JoinStrategy.NestedLoop
        return cls(to, expression, join_type, join_strategy)


class SelectGroupBy(SelectClause):
    def __init__(self, group_by_list, projections):
        self.group_by_list = group_by_list
        self.projections = projections

    def as_operator(self):
        return AggregateOperator(
            group_by_list=self.group_by_list,
            projections=self.projections,
        )


class SelectOrder(SelectClause):
    def __init__(self, order_by_list):
        self.order_by_list = order_by_list

    def as_operator(self):
        order = [o.as_tuple() for o in self.order_by_list]

        return SortOperator(
            order=order
        )

    @classmethod
    def parse_tokens(cls, string, loc, tokens):
        order_by_list = []
        for token in tokens:
            order_by = SelectOrderBy.parse_tokens(token)
            order_by_list.append(order_by)

        return cls(order_by_list)


class SelectOrderBy(SelectClause):
    def __init__(self, ascending, expression):
        self.ascending = ascending
        self.expression = expression

    def as_tuple(self):
        return (self.ascending, self.expression)

    @classmethod
    def parse_tokens(cls, tokens):
        ascending = True
        if len(tokens) == 2:
            ascending = tokens[1].upper() == 'ASC'

        # TODO: Do we handle int literals here or elsewhere?
        return cls(
            ascending=ascending,
            expression=tokens[0]
        )


class SelectLimit(SelectClause):
    def __init__(self, limit):
        self.limit = limit

    def as_operator(self):
        return LimitOperator(limit=self.limit.val)
