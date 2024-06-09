from pydantic import BaseModel

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from sse_starlette.sse import EventSourceResponse

import asyncio
import traceback
import os

from dbdb.logger import logger
import dbdb.engine


# This is a very fly.io way of making sure that requests
# to stream responses get routed to the machine that
# processed the original query. I wish this was less
# coupled to fly.io specifically, but what can you do.
FLY_MACHINE_UNSET = "local"
FLY_MACHINE_ID = os.getenv('FLY_MACHINE_ID', FLY_MACHINE_UNSET)
STREAM_DELAY_IN_SECONDS = 0.1


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


@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}


@app.get('/stream')
async def message_stream(request: Request, query_id: str):
    logger.info(f"Client connected (query={query_id})")
    async def event_generator():
        more_events = True
        while more_events:
            # If client closes connection, stop sending events
            if await request.is_disconnected():
                logger.info(f"Client disconnected (query={query_id})")
                break

            async for (event, is_done) in dbdb.engine.pop_events(query_id):
                yield event
                await asyncio.sleep(0)
                if is_done:
                    more_events = False
                    break

            await asyncio.sleep(STREAM_DELAY_IN_SECONDS)

        logger.info(f"Client request completed (query={query_id})")
        dbdb.engine.unset_query(query_id)

    return EventSourceResponse(
        event_generator(),
        media_type="text/event-stream",
        ping=1
    )


@app.post("/query")
async def run_query(query: Query, background_tasks: BackgroundTasks):
    try:
        sql = query.sql
        query_id, plan, nodes, edges = dbdb.engine.plan_query(sql)

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    # Add background task to run query
    loop = asyncio.get_event_loop()
    background_tasks.add_task(
        dbdb.engine.dispatch_query,
        # Args
        loop,
        query_id,
        plan,
        nodes
    )

    return {
        "query_id": query_id,
        "machine_id": FLY_MACHINE_ID,
        "nodes": [node.to_dict() for node in nodes],
        "edges": edges,
    }


@app.post("/explain")
async def explain_query(query: Query):
    try:
        sql = query.sql
        query_id, plan, nodes, edges = dbdb.engine.plan_query(sql)

    except RuntimeError as e:
        logger.error(f"Error running query: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        logger.error(f"Error running query: {e}")
        logger.info(traceback.format_exc())

        raise HTTPException(status_code=500, detail="Internal server error")

    return {
        "query_id": query_id,
        "nodes": [node.to_dict() for node in nodes],
        "edges": edges,
    }
