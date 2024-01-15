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
STREAM_DELAY = 0.1  # second


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


async def _do_run_query(plan, nodes):
    start_time = time.time()
    row_iterators: Dict[str, List] = {}

    # Sample stats

    EVENTS.append({
        "event": "QueryStart",
        "data": {
            "id": id(plan),
        }
    })

    def on_stat(name, stat, event_name='OperatorStats'):
        if name == "processing":
            # sample it
            if random.random() > 0.05:
                return

        EVENTS.append({
            "event": event_name,
            "name": name,
            "data": stat
        })

    set_stats_callback(on_stat)

    for node in nodes:
        args = {}
        for parent, _, edge in plan.in_edges(node, data=True):
            key = edge['input_arg']
            row_iter = row_iterators[parent].consume()
            if edge.get("list_args"):
                if key not in args:
                    args[key] = []
                args[key].append(row_iter)
            else:
                args[key] = row_iter

        print("Running operator", node, "with args", args)
        rows = await node.run(**args)
        row_iterators[node] = rows

    leaf_node = nodes[-1]

    output = row_iterators[leaf_node]
    output_consumer = output.consume()

    columns = [f.name for f in output.fields]
    EVENTS.append({
        "event": "ResultSchema",
        "data": {
            "id": id(plan),
            "columns": columns,
        }
    })

    batch = []
    async for row in output_consumer:
        batch.append(row.as_tuple())
        if len(batch) == 1000:
            EVENTS.append({
                "event": "ResultRows",
                "data": {
                    "id": id(plan),
                    "rows": batch
                }
            })
            batch = []
    # flush the remaining items in the batch
    if len(batch) > 0:
        EVENTS.append({
            "event": "ResultRows",
            "data": {
                "id": id(plan),
                "rows": batch
            }
        })

    await output.display()

    data = await output.as_table()
    EVENTS.append({
        "event": "QueryComplete",
        "data": {
            "id": id(plan),
        }
    })

    total_bytes_read = 0
    for node in nodes:
        if node.name() == "Table Scan":
            total_bytes_read += node.stats.custom_stats['bytes_read']

    end_time = time.time()
    elapsed = end_time - start_time

    EVENTS.append({
        "event": "QueryStats",
        "data": {
            "id": id(plan),
            "elapsed": elapsed,
            "bytes_read": total_bytes_read,
        }
    })


async def do_run_query(plan, nodes):
    try:
        await _do_run_query(plan, nodes)
    except (RuntimeError, TypeError, ValueError) as e:
        print("GOT AN ERROR!", e)
        EVENTS.append({
            "event": "QueryError",
            "data": {
                "id": id(plan),
                "error": str(e)
            }
        })


@app.post("/query")
async def run_query(query: Query, background_tasks: BackgroundTasks):
    sql = query.sql
    try:
        select = dbdb.lang.lang.parse_query(sql)
        plan = select._plan
        nodes = list(nx.topological_sort(plan))
        parents = {}
        for node in nodes:
            parent_nodes = plan.predecessors(node)
            parents[id(node)] = [id(n) for n in parent_nodes]

    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=str(e))

    background_tasks.add_task(do_run_query, plan, nodes)

    return {
        "query_id": id(plan),
        "nodes": [node.to_dict() for node in nodes],
        "edges": parents,
    }


@app.post("/explain")
async def explain_query(query: Query):
    sql = query.sql

    try:
        select = dbdb.lang.lang.parse_query(sql)
        plan = select._plan
        nodes = list(nx.topological_sort(plan))
        parents = {}
        for node in nodes:
            parent_nodes = plan.predecessors(node)
            parents[id(node)] = [id(n) for n in parent_nodes]

    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "query_id": id(plan),
        "nodes": [node.to_dict() for node in nodes],
        "edges": parents,
    }


