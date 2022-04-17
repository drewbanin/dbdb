
from dbdb.files.file_format import read_pages

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
from dbdb.expressions import Expression, Equality, EqualityTypes

import itertools


op_scan = TableScanOperator(
    table_ref='my_table.dumb',
    # columns=['is_odd', 'my_time', 'my_number', 'my_string'],
    columns=['my_number', 'is_odd', 'my_string'],
)

op_filter = FilterOperator(
    predicates=[
        Equality(
            lexpr=Expression(lambda r: r.is_odd),
            rexpr=Expression(lambda r: True),
            equality=EqualityTypes.EQ

        )
    ]
)

op_sort = SortOperator(
    order=(0, True)
)

op_limit = LimitOperator(
    limit=1000
)

rows = op_scan.run()
rows = op_filter.run(rows)
rows = op_limit.run(rows)
rows = op_sort.run(rows)

# Need to tee the output if feeding forward one
# stream into multiple different things.. eg. for
# when we get to joining stuff later...

# TODO: I need to think about how expressions work...
# can i make an iterator that returns projected values..?


def pluck(iterator, index):
    yield from (row[index] for row in iterator)


def tap(iterator):
    for val in iterator:
        print(val)
        yield val

aggregate_op = AggregateOperator(
    group_by=[
        lambda row: row[2]
    ],
    aggregates=[
        (Aggregates.SUM,    lambda row: row[0]),
        (Aggregates.COUNTD, lambda row: row[0]),
        (Aggregates.AVG,    lambda row: row[0]),
        (Aggregates.MAX,    lambda row: row[0]),
    ],
    project=[
        'my_string',

        'the_sum',
        'the_countd',
        'the_avg',
        'the_max',
    ]
)

# TODO: Can i get stats for rows in and rows out? 
# Maybe by doing stuff in the operator base class?
rows = aggregate_op.run(rows)

project_op = ProjectOperator(
    columns=[
        (0, 'my_string'),
        (1, 'my_sum')
    ],
)

rows = project_op.run(rows)

debug_iter = iter([
    ('abc', 'haha'),
    ('abc', 'cool'),
    ('def', 'neat'),
    ('xyz', 'nooooo'),
])
debug = Rows(['f1', 'f2'], debug_iter)

rows = rows.display()
debug = debug.display()

r1, r2 = itertools.tee(rows, 2)
r1_rows = Rows(['my_string', 'my_sum'], r1)
r2_rows = Rows(['my_string', 'my_sum'], r2)


join_op = NestedLoopJoinOperator(
    inner=False,
    expression=Equality(
        lexpr=Expression(lambda r: r.my_string),
        rexpr=Expression(lambda r: r.f1),
        equality=EqualityTypes.EQ
    )
)

rows = join_op.run(r1_rows, debug)

print()
rows.display()

join_op2 = HashJoinOperator(
    inner=True,
    expression=Equality(
        lexpr=Expression(lambda r: r.my_string),
        rexpr=Expression(lambda r: r.f1),
        equality=EqualityTypes.EQ
    )
)

rows = join_op2.run(r2_rows, debug)

op_sort = SortOperator(
    order=(0, True)
)

rows = op_sort.run(rows)

print()
rows.display()

print()
op_scan.print_cache()
