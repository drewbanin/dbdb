from dbdb.operators.file_operator import TableScanOperator, TableGenOperator
from dbdb.operators.google_sheets import GoogleSheetsOperator
from dbdb.operators.generate_series import GenerateSeriesOperator
from dbdb.operators.sorting import SortOperator
from dbdb.operators.limit import LimitOperator
from dbdb.operators.filter import FilterOperator
from dbdb.operators.project import ProjectOperator
from dbdb.operators.music import PlayMusicOperator
from dbdb.operators.union import UnionOperator
from dbdb.operators.joins import (
    NestedLoopJoinOperator,
    HashJoinOperator,
    JoinStrategy,
    JoinType
)

from dbdb.operators.rename import RenameScopeOperator
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
        ctes=None,

        play_music=None,
    ):
        self.projections = projections
        self.where = where
        self.source = source
        self.joins = joins or []
        self.group_by = group_by
        self.order_by = order_by
        self.limit = limit
        self.ctes = ctes or {}

        self.play_music = play_music

    def make_plan(self, plan=None):
        """
        Order of operations:
        1. Table scan from FROM clause
        2. Apply JOINs
        3. Apply WHERE
        5. Apply projections (including aggregates)
        6. Apply ORDER clause
        7. Apply LIMIT
        """

        plan = plan or nx.DiGraph()
        scopes = {}

        for (cte_name, cte) in self.ctes.items():
            plan, output_op = cte.make_plan(plan)
            scopes[cte_name] = output_op

        def resolve_internal_reference(source, label):
            source_op = source.as_operator()
            plan.add_node(source_op, label=label)

            if source.name() in scopes:
                parent_node = scopes[source.name()]
                plan.add_edge(parent_node, source_op, input_arg="rows")

            return source_op

        # FROM
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

    def __str__(self):
        return f"""
        Projections: {self.projections}
        Filters: {self.where}
        Source: {self.source}
        Joins: {self.joins}
        Order: {self.order_by}
        Limit: {self.limit}
        """

class MusicPlayer:
    def __init__(self, sources, bpm, ctes):
        self.sources = sources
        self.bpm = bpm
        self.ctes = ctes

    def make_plan(self):
        plan = nx.DiGraph()
        scopes = {}

        for (cte_name, cte) in self.ctes.items():
            plan, output_op = cte.make_plan(plan)
            scopes[cte_name] = output_op

        union_op = UnionOperator()
        for source in self.sources:
            if source.name() not in scopes:
                raise RuntimeError(f"Unknown table: {source.name()}")

            parent_node = scopes[source.name()]
            plan.add_edge(parent_node, union_op, input_arg="rows", list_args=True)

        music_op = PlayMusicOperator(bpm=self.bpm)
        plan.add_edge(union_op, music_op, input_arg="rows")

        return plan, music_op


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

        # TODO : Move this into function / module!
        if self.function_name not in ('GOOGLE_SHEET', 'GENERATE_SERIES'):
            raise RuntimeError(f"Unsupported table function: {self.function_name}")

    def name(self):
        return self.table.name

    def as_operator(self):
        if self.function_name == "GOOGLE_SHEET":
            sheet_id = self.function_args[0]
            tab_id = self.function_args[1] if len(self.function_args) == 2 else None

            return GoogleSheetsOperator(
                table=self.table,
                sheet_id=sheet_id,
                tab_id=tab_id
            )
        elif self.function_name == "GENERATE_SERIES":
            count = self.function_args[0]

            return GenerateSeriesOperator(
                table=self.table,
                count=count,
            )



class SelectFileSource(SelectClause):
    def __init__(self, file_path, table_identifier, columns):
        self.file_path = file_path
        self.table_identifier = table_identifier
        self.columns = columns

    def name(self):
        return self.table_identifier.name

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
