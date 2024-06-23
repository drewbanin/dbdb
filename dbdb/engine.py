from dbdb.operators.operator_stats import set_stats_callback
from dbdb.logger import logger
import dbdb.lang.lang

import asyncio
import networkx as nx
import time
import json


# Store events and query results globally
EVENTS = {}
QUERY_CACHE = {}
RUNNING_QUERIES = {}


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


async def pop_events(query_id):
    event_list = EVENTS.get(query_id, [])

    is_done = False
    while not is_done and len(event_list) > 0:
        event = event_list.pop(0)

        event_type = event["event"]
        is_done = event_type == "QueryComplete"

        yield {
            "id": int(time.time()),
            "event": "message",
            "data": json.dumps(event),
        }, is_done

        await asyncio.sleep(0)


def unset_query(query_id):
    del QUERY_CACHE[query_id]
    del EVENTS[query_id]


async def run_query(query_id, plan, nodes):
    start_time = time.time()
    row_iterators: Dict[str, List] = {}

    # Sample stats
    add_event(
        query_id,
        {
            "event": "QueryStart",
            "data": {
                "id": query_id,
            },
        },
    )

    def on_stat(name, stat, event_name="OperatorStats"):
        add_event(query_id, {"event": event_name, "name": name, "data": stat})

    set_stats_callback(on_stat)

    for node in nodes:
        args = {}
        for parent, _, edge in plan.in_edges(node, data=True):
            key = edge["input_arg"]
            row_iter = row_iterators[parent].consume()
            if edge.get("list_args"):
                if key not in args:
                    args[key] = []
                args[key].append(row_iter)
            else:
                args[key] = row_iter

        # logger.info("Running operator", node, "with args", args)
        rows = await node.run(**args)
        row_iterators[node] = rows

    leaf_node = nodes[-1]

    output = row_iterators[leaf_node]
    output_consumer = output.consume()

    if not leaf_node.is_mutation():
        columns = [f.name for f in output.fields]
        add_event(
            query_id,
            {
                "event": "ResultSchema",
                "data": {
                    "id": query_id,
                    "columns": columns,
                },
            },
        )

    async for batch in output_consumer.iter_rows_batches(take=100):
        batched_rows = [r.as_tuple() for r in batch]
        if len(batched_rows) > 0:
            add_event(
                query_id,
                {
                    "event": "ResultRows",
                    "data": {"id": query_id, "rows": batched_rows},
                },
            )
        await asyncio.sleep(0.1)

    # await output.display()

    data = await output.as_table()
    push_cache(query_id, data)

    total_bytes_read = 0
    for node in nodes:
        if node.name() == "Table Scan":
            total_bytes_read += node.stats.custom_stats["bytes_read"]

    end_time = time.time()
    elapsed = end_time - start_time

    add_event(
        query_id,
        {
            "event": "QueryStats",
            "data": {
                "id": query_id,
                "elapsed": elapsed,
                "bytes_read": total_bytes_read,
            },
        },
    )

    if leaf_node.is_mutation():
        status = leaf_node.status_line()

        add_event(
            query_id,
            {
                "event": "QueryMutationStatus",
                "data": {"id": query_id, "status": f"{status} in {elapsed:0.2f}s"},
            },
        )

    add_event(
        query_id,
        {
            "event": "QueryComplete",
            "data": {
                "id": query_id,
            },
        },
    )

    return data


async def safe_dispatch_query(query_id, plan, nodes):
    try:
        await run_query(query_id, plan, nodes)
        logger.info(f"Query {query_id} completed successfully")

    except Exception as e:
        import traceback

        print(traceback.format_exc())

        logger.error(f"Error running query: {e}")
        add_event(
            query_id, {"event": "QueryError", "data": {"id": query_id, "error": str(e)}}
        )


def dispatch_query(loop, query_id, plan, nodes):
    # I don't think i should need to create this task manually, but
    # if I don't, then background tasks block new incoming requests.
    # Weird! And annoying!

    # Note that the loop is passed in from the request handler, so
    # this background task will run in the main server loop. Better
    # hope i didn't make any mistakes with asyncio in the db :)
    task = asyncio.run_coroutine_threadsafe(
        safe_dispatch_query(query_id, plan, nodes), loop
    )
    RUNNING_QUERIES[query_id] = plan, task


def plan_query(sql):
    statement = dbdb.lang.lang.parse_query(sql)

    plan = statement._plan
    nodes = list(nx.topological_sort(plan))
    edges = {}
    for node in nodes:
        parent_nodes = plan.predecessors(node)
        edges[id(node)] = [id(n) for n in parent_nodes]

    query_id = str(id(plan))
    return query_id, plan, nodes, edges


def terminate_query(query_id):
    if query_id not in RUNNING_QUERIES:
        logger.info(f"Query #{query_id} is not running")
        return

    plan, task = RUNNING_QUERIES[query_id]
    for node in list(plan):
        node.exit()

    task.cancel()

    logger.info(f"Cancelled query #{query_id}")
