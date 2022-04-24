
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
column_names = FieldIdentifier.columns_from(
    table_identifier,
    column_names=['my_number', 'is_odd', 'my_string']
)

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
    limit=1000
)

rows = op_scan.run()
rows = op_filter.run(rows)
rows = op_limit.run(rows)
rows = op_sort.run(rows)
rows = rows.display(10)

def ff(name):
    return FieldIdentifier.field(name)

aggregate_op = AggregateOperator(
    fields=[
        (Aggregates.SCALAR, lambda row: row.index(2), ff('my_string')),
        (Aggregates.SUM,    lambda row: row.index(0), ff('my_sum')),
        (Aggregates.COUNTD, lambda row: row.index(0), ff('my_countd')),
        (Aggregates.AVG,    lambda row: row.index(0), ff('my_avg')),
        (Aggregates.MAX,    lambda row: row.index(0), ff('my_max')),
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


table_identifier = TableIdentifier.new("debug")
debug_column_names = FieldIdentifier.columns_from(
    table_identifier,
    column_names=['f1', 'f2']
)

debug_iter = iter((
    ('abc', 'haha'),
    ('abc', 'cool'),
    ('def', 'neat'),
    ('xyz', 'nooooo'),
))
debug = Rows(debug_column_names, debug_iter, table_identifier)

rows = rows.display()
debug = debug.display()

r1, r2 = itertools.tee(rows, 2)
r1_rows = Rows([ff('my_string'), ff('my_number')], r1, table_identifier)
r2_rows = Rows([ff('my_string'), ff('my_number')], r2, table_identifier)

join_op = NestedLoopJoinOperator(
    inner=False,
    expression=Equality(
        lexpr=Expression(lambda r: r.field('my_string')),
        rexpr=Expression(lambda r: r.field('f1')),
        equality=EqualityTypes.EQ
    )
)

rows = join_op.run(r1_rows, debug)

print()
rows.display()

join_op2 = HashJoinOperator(
    inner=False,
    expression=Equality(
        lexpr=Expression(lambda r: r.field('my_string')),
        rexpr=Expression(lambda r: r.field('f1')),
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
