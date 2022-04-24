
from dbdb.io.file_format import read_pages

from dbdb.operators.file_operator import TableScanOperator
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

import itertools


table_identifier = TableIdentifier.new("my_table")
column_names = [
    table_identifier.field('my_number'),
    table_identifier.field('is_odd'),
    table_identifier.field('my_string')
]

op_scan = TableScanOperator(
    table_ref='my_table.dumb',
    table=table_identifier,
    columns=column_names,
)


op_filter = FilterOperator(
    predicates=[
        Equality(
            lexpr=Expression(lambda r: r.field('is_odd')),
            rexpr=Expression(lambda r: True),
            equality=EqualityTypes.EQ

        )
    ]
)

op_sort = SortOperator(
    order=[
        (True, lambda row: row.index(1)),
        (False, lambda row: row.index(2)),
    ]
)

op_limit = LimitOperator(
    limit=4
)

rows = op_scan.run()
rows = op_filter.run(rows)
rows = op_limit.run(rows)
rows = op_sort.run(rows)
rows = rows.display(10)

"""
The way that I'm creating fields is really dumb. I think the general
organizing principles are kind of ad-hoc and silly and that's leading
to everything being kind of jank and unpleasant to work with. How do
we do that part better?

Every field should exist in terms of a table. You cannot have a field
that exists outside the scope of _some sort_ of relation, even if that
relation is virtual or tempoary (as in a join-product, or in a select
body that does not reference a table. This is less-common, but I think
that creating tables, virtual as they may be, is going to help us with
staying organized and managing field access paterns...
"""

aggregate_op = AggregateOperator(
    fields=[
        (
            Aggregates.SCALAR,
            lambda row: row.index(2),
            'my_string'
        ),
        (
            Aggregates.SUM,
            lambda row: row.index(0),
            'my_sum'
        ),
        (
            Aggregates.COUNTD,
            lambda row: row.index(0),
            'my_countd'
        ),
        (
            Aggregates.AVG,
            lambda row: row.index(0),
            'my_avg'
        ),
        (
            Aggregates.MAX,
            lambda row: row.index(0),
            'my_max'
        ),
    ]
)

# TODO: Can i get stats for rows in and rows out? 
# Maybe by doing stuff in the operator base class?
rows = aggregate_op.run(rows)

project_op = ProjectOperator(
    project=[
        (
            lambda row: row.field('my_string'),
            'my_string'
        ),
        (
            lambda row: row.field('my_sum'),
            'my_sum'
        )
    ],
)

rows = project_op.run(rows)


debug_identifier = TableIdentifier.new("debug")
debug_column_names = [
    debug_identifier.field('my_string'),
    debug_identifier.field('f2')
]

debug_iter = iter((
    ('abc', 'haha'),
    ('abc', 'cool'),
    ('def', 'neat'),
    ('xyz', 'nooooo'),
))
debug = Rows(debug_identifier, debug_column_names, debug_iter)

rows = rows.display()
debug = debug.display()

r1, r2 = itertools.tee(rows, 2)
r1_rows = Rows(
    table_identifier,
    [
        table_identifier.field('my_string'),
        table_identifier.field('my_number')
    ],
    r1
)
r2_rows = Rows(
    table_identifier,
    [
        table_identifier.field('my_string'),
        table_identifier.field('my_number')
    ],
    r2
)


join_op = NestedLoopJoinOperator(
    inner=False,
    expression=Equality(
        lexpr=Expression(lambda r: r.field('my_table.my_string')),
        rexpr=Expression(lambda r: r.field('debug.my_string')),
        equality=EqualityTypes.EQ
    )
)

rows = join_op.run(r1_rows, debug)

print()
rows.display()

join_op2 = HashJoinOperator(
    inner=True,
    expression=Equality(
        lexpr=Expression(lambda r: r.field('my_table.my_string')),
        rexpr=Expression(lambda r: r.field('debug.my_string')),
        equality=EqualityTypes.EQ
    )
)

rows = join_op2.run(r2_rows, debug)

op_sort = SortOperator(
    order=[
        (True, lambda row: row.index(1)),
        (True, lambda row: row.index(0)),
    ]
)

rows = op_sort.run(rows)

print()
rows.display()

print()
op_scan.print_cache()
