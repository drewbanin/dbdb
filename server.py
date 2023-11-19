import dbdb.lang.lang
from dbdb.operators.operator_stats import set_stats_callback

import networkx as nx
import time
import json
from pydantic import BaseModel
from typing import Dict, List

from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError

from sse_starlette.sse import EventSourceResponse
import asyncio


class Query(BaseModel):
    sql: str


class QueryId(BaseModel):
    queryId: str


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


EVENTS = []
STREAM_DELAY = 1  # second


@app.get('/stream')
async def message_stream(request: Request):
    async def event_generator():
        while True:
            # If client closes connection, stop sending events
            if await request.is_disconnected():
                break

            # Checks for new messages and return them to client if any
            if len(EVENTS) > 0:
                num_events = len(EVENTS)
                for i in range(num_events):
                    event = EVENTS.pop(0)
                    yield {
                        "event": "message",
                        "id": int(time.time()),
                        "data": json.dumps(event),
                    }

            await asyncio.sleep(STREAM_DELAY)

    return EventSourceResponse(event_generator())


async def do_run_query(plan, nodes):
    row_iterators: Dict[str, List] = {}

    def on_stat(stat):
        EVENTS.append({
            "event": "OperatorStats",
            "data": stat
        })

    set_stats_callback(on_stat)

    for node in nodes:
        args = {}
        for parent, _, data in plan.in_edges(node, data=True):
            key = data['input_arg']
            args[key] = row_iterators[parent]

        print("Running operator", node, "with args", args)
        rows = node.run(**args)
        row_iterators[node] = rows

    leaf_node = nodes[-1]

    preso = row_iterators[leaf_node]
    preso.display()

    columns = [f.name for f in preso.fields]
    data = preso.as_table()

    EVENTS.append({
        "event": "QueryComplete",
        "data": {
            "id": id(plan),
            "columns": columns,
            "rows": data
        }
    })


@app.post("/query")
async def run_query(query: Query, background_tasks: BackgroundTasks):
    sql = query.sql

    parsed = dbdb.lang.lang.parse_query(sql)
    plan = parsed.make_plan()
    nodes = list(nx.topological_sort(plan))

    parents = {}
    for node in nodes:
        parent_nodes = plan.predecessors(node)
        parents[id(node)] = [id(n) for n in parent_nodes]

    background_tasks.add_task(do_run_query, plan, nodes)

    return {
        "query_id": id(plan),
        "nodes": [node.to_dict() for node in nodes],
        "edges": parents,
    }


