import dbdb.lang.lang
from dbdb.operators.operator_stats import set_stats_callback

import networkx as nx
import time
import json
import random
from pydantic import BaseModel
from typing import Dict, List

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError

from sse_starlette.sse import EventSourceResponse
import asyncio


sql = """
with bass as (
    select
        sum(length) over (rows between unbounded preceding and current row) as start_time,
        start_time,
        play_tone(
            note,
            octave,
            length,
            amplitude,
            start_time
        )

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Bass')
)

select *
from bass
"""

parsed = dbdb.lang.lang.parse_query(sql)
plan = parsed.make_plan()
nodes = list(nx.topological_sort(plan))

parents = {}
for node in nodes:
    parent_nodes = plan.predecessors(node)
    parents[id(node)] = [id(n) for n in parent_nodes]


from server import _do_run_query

loop = asyncio.get_event_loop()
loop.run_until_complete(_do_run_query(plan, nodes))
