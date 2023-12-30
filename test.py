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
        note,
        length,
        octave,
        amplitude,
        start_time

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Melody')
)

play bass at 130 bpm
"""

parsed = dbdb.lang.lang.parse_query(sql)
plan, output_node = parsed.make_plan()
nodes = list(nx.topological_sort(plan))

from server import _do_run_query

loop = asyncio.get_event_loop()
loop.run_until_complete(_do_run_query(plan, nodes))
