from dbdb.operators.file_operator import TableScanOperator, TableGenOperator
from dbdb.operators.sorting import SortOperator
from dbdb.operators.limit import LimitOperator
from dbdb.operators.filter import FilterOperator
from dbdb.operators.project import ProjectOperator
from dbdb.operators.joins import (
    NestedLoopJoinOperator,
    HashJoinOperator,
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
        # TODO : Give this thing an as_operator() (and potentially make it so that
        # operators mutate the DAG directly...? so that we can compose this with
        # CTEs and subqueries and stuff. Like... a projection should be able to
        # be a subquery... lol but then i need to add support for correlated subqs
        # ok... maybe just add support in the FROM clause? That's fine & reasonable...
        """
        Order of operations:
        1. Table scan from FROM clause
        2. Apply JOINs
        3. Apply WHERE
        4. Apply GROUP and aggregates
        5. Apply projections
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
            pass

        # Projections
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


class SelectClause:
    def as_operator(self):
        raise NotImplementedError()


class SelectList(SelectClause):
    def __init__(self, projections):
        self.projections = projections

    def as_operator(self):
        # TODO : It's not super smart to mix projections and aliases here..
        # Do i need a new operator that just does the grouping part? That
        # Would feed forward tuples that contained the grouping set of tuples..
        # which could be kind of interesting! It should totally be its own node
        # in the execution graph.... but splitting up the projecting and agg
        # from the grouping part feels kind of annoying... hmm.....
        is_scalar = all(p.is_scalar() for p in self.projections)

        if is_scalar:
            return self.scalar_projection()
        else:
            return self.aggregate_projection()

    def scalar_projection(self):
        return ProjectOperator(
            project=[p.scalar_tuple() for p in self.projections]
        )

    def aggregate_projection(self):
        return AggregateOperator(
            fields=[p.agg_tuple() for p in self.projections]
        )


class SelectProjection(SelectClause):
    def __init__(self, expr, aggregate, alias):
        self.expr = expr
        self.aggregate = aggregate
        self.alias = alias

    def scalar_tuple(self):
        return (
            self.expr,
            self.alias
        )

    def agg_tuple(self):
        return (
            self.aggregate,
            self.expr,
            self.alias
        )

    def is_scalar(self):
        return self.aggregate == Aggregates.SCALAR


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
    def __init__(self, to, inner, expression, join_type):
        self.to = to
        self.inner = inner
        self.expression = expression
        self.join_type = join_type

    def as_operator(self):
        return self.join_type.create(
            inner=self.inner,
            expression=self.expression
        )


class SelectOrder(SelectClause):
    def __init__(self, order_by_list):
        self.order_by_list = order_by_list

    def as_operator(self):
        order = [o.as_tuple() for o in self.order_by_list]

        return SortOperator(
            order=order
        )


class SelectOrderBy(SelectClause):
    def __init__(self, ascending, expression):
        self.ascending = ascending
        self.expression = expression

    def as_tuple(self):
        return (self.ascending, self.expression)


class SelectLimit(SelectClause):
    def __init__(self, limit):
        self.limit = limit

    def as_operator(self):
        return LimitOperator(limit=self.limit)
