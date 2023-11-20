
from dbdb.io.file_format import read_pages

from dbdb.operators.file_operator import TableScanOperator
from dbdb.operators.sorting import SortOperator
from dbdb.operators.limit import LimitOperator
from dbdb.operators.filter import FilterOperator
from dbdb.operators.project import ProjectOperator
from dbdb.operators.joins import (
    NestedLoopJoinOperator,
    HashJoinOperator,
    JoinStrategy
)

from dbdb.operators.aggregate import AggregateOperator, Aggregates
from dbdb.tuples.rows import Rows
from dbdb.tuples.identifiers import TableIdentifier, FieldIdentifier, GlobIdentifier
from dbdb.expressions import Expression, Equality, EqualityTypes

import itertools
import networkx as nx

from dbdb.lang.select import (
    Select,
    SelectList,
    SelectProjection,
    SelectFilter,
    SelectFileSource,
    SelectMemorySource,
    SelectJoin,
    SelectOrder,
    SelectOrderBy,
    SelectLimit,
)


sql = """
select
  my_table.my_string as my_string,
  sum(my_table.is_odd + 10) as my_avg

from my_table
group by 1
order by 1
limit 10
"""


import dbdb.lang.lang
import asyncio


async def run():

    query = dbdb.lang.lang.parse_query(sql)
    print(query)

    plan = query.make_plan()
    print(plan)

    nodes = list(nx.topological_sort(plan))

    parents = {}
    for node in nodes:
        parent_nodes = plan.predecessors(node)
        parents[id(node)] = [id(n) for n in parent_nodes]

    row_iterators = {}
    for node in nodes:
        args = {}
        for parent, _, data in plan.in_edges(node, data=True):
            key = data['input_arg']
            args[key] = row_iterators[parent]

        print("Running operator", node, "with args", args)
        rows = await node.run(**args)
        row_iterators[node] = rows

    leaf_node = nodes[-1]
    print("Leaf:", leaf_node)


    preso = row_iterators[leaf_node]
    await preso.display()


asyncio.run(run())

"""
my_table = TableIdentifier.new("my_table")

debug_identifier = TableIdentifier.new("debug")
debug_rows = Rows.from_literals(
    table=debug_identifier,
    fields=[
        debug_identifier.field('my_string'),
        debug_identifier.field('my_number')
    ],
    data=(
        ('abc', 1),
        ('abc', 2),
        ('def', 3),
        ('def', 4),
        ('ghi', 5),
        ('ghi', 6),
    )
)


query = Select(
    projections=SelectList(
        projections=[
            SelectProjection(
                expr=lambda row: row.field('my_table.my_string'),
                aggregate=Aggregates.SCALAR,
                alias="my_field"
            ),
            SelectProjection(
                expr=lambda row: row.field('debug.my_number'),
                aggregate=Aggregates.AVG,
                alias="my_avg"
            )
        ],
    ),
    where=SelectFilter(
        expr=Equality(
            lexpr=Expression(lambda row: row.field('is_odd')),
            rexpr=Expression(lambda row: True),
            equality=EqualityTypes.EQ
        )
    ),
    source=SelectFileSource(
        file_path="my_table.dumb",
        table_identifier=my_table,
        columns=[
            my_table.field('my_string'),
            my_table.field('is_odd'),
        ]
    ),
    joins=[
        SelectJoin(
            to=SelectMemorySource(
                table_identifier=debug_identifier,
                rows=debug_rows
            ),
            inner=True,
            expression=Equality(
                lexpr=Expression(lambda r: r.field('my_table.my_string')),
                rexpr=Expression(lambda r: r.field('debug.my_string')),
                equality=EqualityTypes.EQ
            ),
            join_type=JoinTypes.HashJoin
        )
    ],
    order_by=SelectOrder(
        order_by_list=[
            SelectOrderBy(
                ascending=True,
                expression=lambda row: row.index(0)
            )
        ]
    ),
    limit=SelectLimit(
        limit=3
    )
)
"""



# import grandalf
# from grandalf.layouts import SugiyamaLayout
# import matplotlib.pyplot as plt
# 
# fig = plt.figure(figsize=(12,12))
# ax = plt.subplot(111)
# ax.set_title('Execution graph', fontsize=10)
# plt.title('Execution plan')
# 
# labels = {
#     n: data.get('label')
#     for n, data in plan.nodes(data=True)
# }
# 
# graph = grandalf.utils.convert_nextworkx_graph_to_grandalf(plan)
# 
# class defaultview(object):
#     w, h = 10, 10
# 
# for v in graph.V():
#     v.view = defaultview()
# 
# sug = SugiyamaLayout(graph.C[0])
# sug.init_all() # roots=[V[0]])
# sug.draw() # This is a bit of a misnomer, as grandalf doesn't actually come with any visualization methods. This method instead calculates positions
# pos = {v.data: (v.view.xy[0], v.view.xy[1]) for v in graph.C[0].sV} # Extracts the positions
# 
# 
# nx.draw(
#     plan,
#     pos,
#     labels=labels,
#     node_size=1800,
#     node_color="#ddd",
# )
# 
# plt.tight_layout()
# plt.savefig("Graph.png", format="PNG")


# OK... so now that we have a DAG.... go ahead and call run()
# on each operator with the inputs from its parent edges? How do
# we do that part...?




