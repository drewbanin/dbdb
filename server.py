import dbdb.lang.lang
from dbdb.operators.operator_stats import set_stats_callback

import networkx as nx
import time
import json
import random
import os
from pydantic import BaseModel
from typing import Dict, List

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

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
    expose_headers=["*"],
)


EVENTS = {}
STREAM_DELAY = 0.1  # second

# This is a very fly.io way of making sure that requests
# to stream responses get routed to the machine that
# processed the original query. I wish this was less
# coupled to fly.io specifically, but what can you do.
FLY_MACHINE_UNSET = "local"
FLY_MACHINE_ID = os.getenv('FLY_MACHINE_ID', FLY_MACHINE_UNSET)
QUERY_CACHE = {}


def push_cache(query_id, data):
    QUERY_CACHE[query_id] = data


def pop_cache(query_id):
    if query_id not in QUERY_CACHE:
        return

    return QUERY_CACHE.pop(query_id)


def add_event(query_id, payload):
    if query_id not in EVENTS:
        EVENTS[query_id] = [payload]
    else:
        EVENTS[query_id].append(payload)


def pop_events(query_id):
    event_list = EVENTS.get(query_id, [])

    # This looks dumb but is actually smart. We need to pop
    # from the left of the queue at the same time that another
    # task could be adding to the right of the queue. Instead
    # of looping over the list, snapshot the elements to pop
    # up-front and then loop that last. We'll get the other
    # elements on the next task.
    num_events = len(event_list)
    for i in range(num_events):
        event = event_list.pop(0)

        event_type = event['event']
        is_done = (event_type == 'QueryComplete')

        yield {
            "id": int(time.time()),
            "event": "message",
            "data": json.dumps(event),
        }, is_done


def unset_query(query_id):
    del QUERY_CACHE[query_id]
    del EVENTS[query_id]


@app.middleware("http")
async def check_machine_id(request: Request, call_next):
    machine_id = request.query_params.get('machine_id')
    if FLY_MACHINE_ID == FLY_MACHINE_UNSET:
        return await call_next(request)

    elif not machine_id:
        return await call_next(request)

    elif FLY_MACHINE_ID == machine_id:
        return await call_next(request)

    else:
        return JSONResponse(
            content={},
            headers={
                "fly-replay": f"instance={machine_id}"
            }
        )


@app.get('/stream')
async def message_stream(request: Request, query_id: str):
    print(f"Client connected (query={query_id})")
    async def event_generator():
        more_events = True
        while more_events:
            # If client closes connection, stop sending events
            if await request.is_disconnected():
                print(f"Client disconnected (query={query_id})")
                break

            for (event, is_done) in pop_events(query_id):
                yield event
                if is_done:
                    more_events = False
                    break

            await asyncio.sleep(STREAM_DELAY)

        unset_query(query_id)
        print(f"Client request completed (query={query_id})")

    return EventSourceResponse(event_generator())


async def _do_run_query(query_id, plan, nodes):
    start_time = time.time()
    row_iterators: Dict[str, List] = {}

    # Sample stats
    add_event(query_id, {
        "event": "QueryStart",
        "data": {
            "id": query_id,
        }
    })

    def on_stat(name, stat, event_name='OperatorStats'):
        if name == "processing":
            # sample it
            if random.random() > 0.05:
                return

        add_event(query_id, {
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
    add_event(query_id, {
        "event": "ResultSchema",
        "data": {
            "id": query_id,
            "columns": columns,
        }
    })

    batch = []
    async for row in output_consumer:
        batch.append(row.as_tuple())
        if len(batch) == 1000:
            add_event(query_id, {
                "event": "ResultRows",
                "data": {
                    "id": query_id,
                    "rows": batch
                }
            })
            batch = []
    # flush the remaining items in the batch
    if len(batch) > 0:
        add_event(query_id, {
            "event": "ResultRows",
            "data": {
                "id": query_id,
                "rows": batch
            }
        })

    await output.display()

    data = await output.as_table()
    push_cache(query_id, data)

    total_bytes_read = 0
    for node in nodes:
        if node.name() == "Table Scan":
            total_bytes_read += node.stats.custom_stats['bytes_read']

    end_time = time.time()
    elapsed = end_time - start_time

    add_event(query_id, {
        "event": "QueryStats",
        "data": {
            "id": query_id,
            "elapsed": elapsed,
            "bytes_read": total_bytes_read,
        }
    })

    add_event(query_id, {
        "event": "QueryComplete",
        "data": {
            "id": query_id,
        }
    })



async def do_run_query(query_id, plan, nodes):
    try:
        await _do_run_query(query_id, plan, nodes)
    except (RuntimeError, TypeError, ValueError) as e:
        print("GOT AN ERROR!", e)
        add_event(query_id, {
            "event": "QueryError",
            "data": {
                "id": query_id,
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

    query_id = str(id(plan))
    background_tasks.add_task(do_run_query, query_id, plan, nodes)

    return {
        "query_id": query_id,
        "machine_id": FLY_MACHINE_ID,
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

    query_id = id(plan)
    return {
        "query_id": query_id,
        "nodes": [node.to_dict() for node in nodes],
        "edges": parents,
    }

@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}
