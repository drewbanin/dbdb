import dbdb.lang.lang
from dbdb.operators.operator_stats import set_stats_callback

import networkx as nx
import time
import json
import random
from pydantic import BaseModel
from typing import Dict, List
import tabulate

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

badguy = """
with notes as (
    select
        note,
        frequency::float as freq

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Notes')

),

bass as (
    select
        note,
        length::float as  length,
        octave::int as octave,
        amplitude::float as float,
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Bass')
),

melody as (
    select
        note,
        length::float as  length,
        octave::int as octave,
        amplitude::float as amplitude,
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Melody')
),

bass_freq as (

    select
        bass.start_time / 2 + 0.01 as time,
        bass.length / 2 - 0.02 as length,
        notes.freq * pow(2, bass.octave - 4) as freq

    from bass
    join notes on notes.note = bass.note

),

melody_freq as (

    select
        melody.start_time / 2 + 0.01 as time,
        melody.length / 2 - 0.02 as length,
        notes.freq * pow(2, melody.octave - 4) as freq

    from melody
    join notes on notes.note = melody.note

)

select 'sin' as func, * from bass_freq
union
select 'sin' as func, * from melody_freq
"""


scale = """
with gen as (
    select
        i as time,
        case
            when i = 0 then 'C'
            when i = 1 then 'D'
            when i = 2 then 'E'
            when i = 3 then 'F'
            when i = 4 then 'G'
            when i = 5 then 'A'
            when i = 6 then 'B'
            when i = 7 then 'C'
            else 'Rest'
        end as note,

        case
            when i < 5 then 4
            else 5
        end as octave

    from generate_series(10)
),

notes as (
    select
        note,
        frequency::float as frequency
    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Notes')

),

joined as (

    select
        gen.time,
        notes.frequency * pow(2, gen.octave - 4) as freq
    from gen
    join notes on gen.note = notes.note

)

select * from joined
"""

yoshi  = """
with notes as (
    select
        note,
        frequency::float as freq

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Notes')

),

bass as (
    select
        note,
        length::float as  length,
        octave::int as octave,
        amplitude::float as amplitude,
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'YoshiBass')
),

bass_freq as (

    select
        bass.start_time as time,
        bass.length,
        bass.amplitude,
        notes.freq * pow(2, bass.octave - 4) as freq

    from bass
    join notes on notes.note = bass.note

),

melody as (
    select
        note,
        length::float as  length,
        octave::int as octave,
        amplitude::float as amplitude,
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'YoshiMelody')
),

melody_freq as (

    select
        melody.start_time as time,
        melody.length,
        melody.amplitude,
        notes.freq * pow(2, melody.octave - 4) as freq

    from melody
    join notes on notes.note = melody.note

)

select 'square' as func, * from bass_freq
union
select 'sin' as func, * from melody_freq
"""

gerudo  = """
with notes as (
    select
        note,
        frequency::float as freq

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Notes')

),

bass as (
    select
        note,
        length::float as  length,
        octave::int as octave,
        amplitude::float as amplitude,
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Fairy')
),

bass_freq as (

    select
        bass.start_time as time,
        bass.length,
        bass.amplitude,
        notes.freq * pow(2, bass.octave - 5) as freq

    from bass
    join notes on notes.note = bass.note

)

select * from bass_freq
"""

avril  = """
select midi('avril_14.mid') at 60 bpm
"""

debug = """
select 'one' as id, i from generate_series(2)
union
select 'two' as id, i from generate_series(2)
union
select 'tre' as id, i from generate_series(2)

order by 1, 2 desc
"""

single_select = """
select 1 as id
"""

ctas = """
create table dbdb.my_schema.debug_tbl as (
    select * from mydb.my_schema.debug
)
"""

single_select = """
select * from mydb.my_schema.debug
"""

agg = """
select
    id,
    sum(id),
    count(id)
from debug
group by 1
"""

debug = """
select * from generate_series(10)
"""

sql = debug

select = dbdb.lang.lang.parse_query(sql)
nodes = list(nx.topological_sort(select._plan))

from server import _do_run_query

loop = asyncio.get_event_loop()
res = loop.run_until_complete(_do_run_query("abc123", select._plan, nodes))

tbl = tabulate.tabulate(
    res,
    headers="keys",
    tablefmt='presto'
)

print(tbl)
