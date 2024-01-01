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
with gen as (
    select
        i as time,
        case
            when i < 44100 then sqr(i * 261.2 * 2 * pi / 44100)
            when i < (44100 * 2) then sqr(i * 261.2 * 3 * pi / 44100)
            when i < (44100 * 3) then sqr(i * 261.2 * 4 * pi / 44100)
            else sqr(i * 261.2 * 5 * pi / 44100)
        as freq

    from generate_series(44100)
)

play gen
"""

sql = """
with spine as (

    select i
    from generate_series(44100)

),

bass as (
    select
        note,
        length::float as length,
        octave::int as octave,
        amplitude::float as amplitude,
        start_time::float as start_time

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Melody')
)

select *
from spine
join bass on (bass.start_time * 44100) >= spine.i and (bass.start_time * 44100 + bass.length) < spine.i

limit 100
"""




# sin(i * 261.2 * 2 * pi / 44100) as freq

parsed = dbdb.lang.lang.parse_query(sql)
plan, output_node = parsed.make_plan()
nodes = list(nx.topological_sort(plan))

from server import _do_run_query

loop = asyncio.get_event_loop()
loop.run_until_complete(_do_run_query(plan, nodes))
