from dbdb.operators.file_operator import TableScanOperator, TableGenOperator
from dbdb.operators.sorting import SortOperator
from dbdb.operators.limit import LimitOperator
from dbdb.operators.filter import FilterOperator
from dbdb.operators.project import ProjectOperator
from dbdb.operators.union import UnionOperator
from dbdb.operators.joins import (
    NestedLoopJoinOperator,
    HashJoinOperator,
    JoinStrategy,
    JoinType
)

from dbdb.operators.rename import RenameScopeOperator
from dbdb.operators.aggregate import AggregateOperator, Aggregates
from dbdb.operators.create import CreateTableAsOperator
from dbdb.tuples.rows import Rows
from dbdb.tuples.identifiers import TableIdentifier, FieldIdentifier
from dbdb.expressions import Expression, Equality, EqualityTypes
from dbdb.lang import table_functions

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
        unions=None,
        ctes=None,

        scopes=None,
    ):
        self.projections = projections
        self.where = where
        self.source = source
        self.joins = joins or []
        self.group_by = group_by
        self.order_by = order_by
        self.limit = limit
        self.unions = []

        self.scopes = scopes or {}

        self._plan = None
        self._output_op = None

    def make_plan(self, plan=None):
        plan = plan or nx.DiGraph()

        def resolve_internal_reference(source, label):
            source_op = source.as_operator()
            plan.add_node(source_op, label=label)

            if source.name() in self.scopes:
                parent_node = self.scopes[source.name()]
                plan.add_edge(parent_node, source_op, input_arg="rows")

            return source_op

        # FROM
        source_op = None
        if self.source is None:
            source_op = SelectMemorySource(
                table_identifier="EmptyTable",
                rows=1
            ).as_operator()
        else:
            source_op = resolve_internal_reference(self.source, label="FROM")

        # JOIN
        output_op = source_op
        for join in self.joins:
            join_op = join.as_operator()
            plan.add_node(join_op, label="JOIN")
            plan.add_edge(output_op, join_op, input_arg="left_rows")

            join_to_op = resolve_internal_reference(join.to, label="FROM (join)")
            plan.add_edge(join_to_op, join_op, input_arg="right_rows")

            # Future operations are on the output of this operation
            output_op = join_op

        # WHERE
        if self.where:
            filter_op = self.where.as_operator()
            plan.add_node(filter_op, label="Filter")
            plan.add_edge(output_op, filter_op, input_arg="rows")
            output_op = filter_op

        # GROUP BY
        if self.group_by:
            aggregate_op = self.group_by.as_operator()
            plan.add_node(aggregate_op, label="Aggregate")
            plan.add_edge(output_op, aggregate_op, input_arg="rows")
            output_op = aggregate_op
            # We can do the actual grouping here.... should this
            # just be it's own operator? why not??????
            # I feel like i already tried this though...
        else:
            # Scalar projections
            project_op = self.projections.as_operator()
            plan.add_node(project_op, label="Project")
            plan.add_edge(output_op, project_op, input_arg="rows")
            output_op = project_op

        if self.unions:
            union_op = UnionOperator()
            plan.add_node(union_op, label="Union")
            plan.add_edge(output_op, union_op, input_arg="rows", list_args=True)
            for union in self.unions:
                _, output = union.make_plan(plan)
                plan.add_edge(output, union_op, input_arg="rows", list_args=True)

            output_op = union_op

        if self.order_by:
            order_by_op = self.order_by.as_operator()
            plan.add_node(order_by_op, label="Order")
            plan.add_edge(output_op, order_by_op, input_arg="rows")
            output_op = order_by_op

        if self.limit:
            limit_op = self.limit.as_operator()
            plan.add_node(limit_op, label="Limit")
            plan.add_edge(output_op, limit_op, input_arg="rows")
            output_op = limit_op

        return plan, output_op

    def save_plan(self, plan):
        plan, output_op = self.make_plan(plan)
        self._plan = plan
        self._output_op = output_op

    def __str__(self):
        return f"""
        Projections: {self.projections}
        Filters: {self.where}
        Source: {self.source}
        Joins: {self.joins}
        Order: {self.order_by}
        Limit: {self.limit}
        """


class CreateTableAs:
    def __init__(
        self,
        table,
        select,
    ):
        self.table = table
        self.select = select

        self._plan = None
        self._output_op = None

    def save_plan(self):
        plan, output_op = self.make_plan()
        self._plan = plan
        self._output_op = output_op

    def make_plan(self):
        plan = self.select._plan
        select_output_op = self.select._output_op

        create_op = CreateTableAsOperator(table=self.table)

        plan.add_node(create_op, label="Create")
        plan.add_edge(select_output_op, create_op, input_arg="rows")

        return plan, create_op


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


class SelectReferenceSource(SelectClause):
    def __init__(self, table_identifier):
        self.table = table_identifier

    def name(self):
        return self.table.name

    def as_operator(self):
        return RenameScopeOperator(
            scope_name=self.table.name
        )


class SelectFunctionSource(SelectClause):
    def __init__(self, function_name, function_args, table_identifier):
        self.function_name = function_name
        self.function_args = function_args
        self.table = table_identifier

    def name(self):
        return self.table.name

    def as_operator(self):
        return table_functions.as_operator(
            self.table,
            self.function_name,
            self.function_args
        )


class SelectFileSource(SelectClause):
    def __init__(self, table, columns):
        self.table = table
        self.columns = columns

    def name(self):
        return self.table.name

    def as_operator(self):
        return TableScanOperator(
            table_ref=self.table,
            columns=self.columns
        )


class SelectMemorySource(SelectClause):
    def __init__(self, table_identifier, rows):
        self.table_identifier = table_identifier
        self.rows = rows

    def name(self):
        return self.table_identifier.name

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
