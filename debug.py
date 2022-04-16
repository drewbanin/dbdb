
from dbdb.files.file_format import read_pages

from dbdb.operators.file_operator import TableScanOperator
from dbdb.operators.sorting import SortOperator
from dbdb.operators.limit import LimitOperator
from dbdb.operators.filter import FilterOperator
from dbdb.operators.project import ProjectOperator
from dbdb.operators.joins import (
    NestedLoopJoinOperator
)

from dbdb.operators.aggregate import AggregateOperator, Aggregates

import itertools


op_scan = TableScanOperator(
    table_ref='my_table.dumb',
    # columns=['is_odd', 'my_time', 'my_number', 'my_string'],
    columns=['my_number', 'is_odd', 'my_string'],
)

op_filter = FilterOperator(
    predicates=[
        lambda r: r[1] is True or r[1] is False
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
    aggregates=[
        (Aggregates.SUM,    lambda row: row[0]),
        (Aggregates.COUNTD, lambda row: row[0]),
        (Aggregates.AVG,    lambda row: row[0]),
        (Aggregates.MAX,    lambda row: row[0]),
    ],
    group_by=[
        lambda row: row[2]
    ]
)

# TODO: Can i get stats for rows in and rows out? 
# Maybe by doing stuff in the operator base class?
rows = aggregate_op.run(rows)

list_op = AggregateOperator(
    aggregates=[
        (Aggregates.LISTAGG, lambda row: row[0]),
    ],
    group_by=[]
)

project_op = ProjectOperator(
    columns=[0, 1]
)

rows = project_op.run(rows)
rows = tap(rows)

join_op = NestedLoopJoinOperator(
    inner=True,
    expression=lambda l, r: l[0] == r[0]
)

debug = [
    ('abc', 'haha'),
    ('abc', 'cool'),
    ('def', 'fuck yeah'),
    ('xyz', 'nooooo shit no'),

]
rows = join_op.run(rows, debug)

print(list(rows))
print(op_scan.cache)
