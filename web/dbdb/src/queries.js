
const MATH = `
select
    i / 20 as x,
    sin(i / 20) + sin (1.1 * i / 20) as y,
    '#000000' as color

from generate_series(2000, 0.001)
`.trim();

const DELAY = `
select * from generate_series(100, 0.1)
`.trim();

const SHEET = `
select *
from google_sheet('1FdRv_eVVo5GxtXthiScIyA3L7Z7h21eXWMMDcaFfGMI', 'Example')
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

    from google_sheet('1Jb9K3yDyNVPIAP_i7AELDLBzm5bmQR3f3RuUwGAzWuc', 'Scale')
)

select note, time, length, func, frequency as freq from scale
union
select note, time, length, func, frequency * 2 as freq from scale
order by time
`.trim();


const YOSHI = `
with notes as (
    select
        note,
        frequency::float as freq

    from google_sheet('1Jb9K3yDyNVPIAP_i7AELDLBzm5bmQR3f3RuUwGAzWuc', 'Notes')

),

bass as (
    select
        note,
        length::float as  length,
        octave::int as octave,
        amplitude::float as amplitude,
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1E0Zp6G_2URi3HiFRUk1PlMlAHeWopiJ7LZy6kugGG1Y', 'Bass')
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

    from google_sheet('1E0Zp6G_2URi3HiFRUk1PlMlAHeWopiJ7LZy6kugGG1Y', 'Melody')
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
order by time
`.trim();


const FAIRY = `
with notes as (
    select
        note,
        frequency::float as freq

    from google_sheet('1Jb9K3yDyNVPIAP_i7AELDLBzm5bmQR3f3RuUwGAzWuc', 'Notes')

),

melody as (
    select
        note,
        length::float as  length,
        octave::int as octave,
        amplitude::float as amplitude,
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1Q_9SttYWqcIfJ2r2fLUY01yPXrKn300aA32h58YB7lY', 'Zelda')
),

melody_freq as (

    select
        melody.start_time as time,
        melody.length,
        melody.amplitude,
        notes.freq * pow(2, melody.octave - 5) as freq

    from melody
    join notes on notes.note = melody.note

)

select * from melody_freq
order by time
`.trim();

const APHEX = `
select
    freq::float as freq,
    time::float as time,
    length::float as length,
    'sin' as func

from google_sheet('1BohUT5DscWO8JLV-o0uzPLWzaBC2NwJkGNelwGVZjEQ')
order by time
`.trim();

const SATIE = `
select
    freq::float as freq,
    time::float as time,
    length::float as length,
    'sin' as func

from google_sheet('1_I2qc7jBJhrQBpgtoPjvEQ_nyLob1CruBLJ1UFbmueY')
order by time
`;

const ALTMAN = `
with music as (
    select * from ask_gpt('Play the song "Happy Birthday"')
)

select
    time::float as time,
    length::float as length,
    freq::float as freq,
    func
from music
order by time
`.trim();

const QUERIES = {

    DELAY: DELAY,
    MATH: MATH,
    SHEET: SHEET,
    SWEEP: SWEEP,
    SCALE: SCALE,
    YOSHI: YOSHI,
    FAIRY: FAIRY,
    "AVRIL 14": APHEX,
    ALTMAN: ALTMAN,
    SATIE: SATIE,
}


const QUERY_OPTS = Object.keys(QUERIES).map(key => {
    return {label: key, value: QUERIES[key]}
});

export default QUERY_OPTS;
