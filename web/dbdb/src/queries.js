
const MATH = `
select
    10 as line,
    sin(i / 100) as sine,
    sin(2 * i / 100) as sine_two,
    sqr(i / 100) as square,
    sqr(2 * i / 100) as square_two

from generate_series(4410)
`.trim();

const DELAY = `
select * from generate_series(100, 0.1)
`.trim();

const SHEET = `
select
    note,
    frequency::float as freq
from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Notes')
`.trim();


const SWEEP = `
select
    i / 2 as time,
    50 + 25 * i as freq,
    0.5 as length,
    'sin' as func
from generate_series(15)
`.trim();


const SCALE = `
with scale as (
    select
        note,
        freq::float as frequency,
        start_time::float as time,
        length::float as length,
        'sin' as func

    from google_sheet('1n9NnBdqvDhDaLz7txU3QQ0NOA4mia9sUiIX6n5MD9WU', 'Scale')
)

select *, frequency as freq from scale
union
select *, frequency * 2 as freq from scale
`.trim();


const YOSHI = `
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

select 'sqr' as func, * from bass_freq
union
select 'sin' as func, * from melody_freq
`.trim();


const FAIRY = `
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
`.trim();

const APHEX = `
select * from midi('avril_14.mid')
`.trim();

const QUERIES = {

    DELAY: DELAY,
    MATH: MATH,
    SHEET: SHEET,
    SWEEP: SWEEP,
    SCALE: SCALE,
    FAIRY: FAIRY,
    YOSHI: YOSHI,
    APHEX: APHEX,
}


const QUERY_OPTS = Object.keys(QUERIES).map(key => {
    return {label: key, value: QUERIES[key]}
});

export default QUERY_OPTS;
