
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
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1E0Zp6G_2URi3HiFRUk1PlMlAHeWopiJ7LZy6kugGG1Y', 'Bass')
),

bass_freq as (

    select
        bass.start_time as time,
        bass.length,
        notes.freq * pow(2, bass.octave - 4) as freq

    from bass
    join notes on notes.note = bass.note

),

melody as (
    select
        note,
        length::float as  length,
        octave::int as octave,
        start_time::float as start_time,
        start_time::float + length::float as end_time

    from google_sheet('1E0Zp6G_2URi3HiFRUk1PlMlAHeWopiJ7LZy6kugGG1Y', 'Melody')
),

melody_freq as (

    select
        melody.start_time as time,
        melody.length,
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
        notes.freq * pow(2, melody.octave - 5) as freq,
        0.9 as velocity

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
    'sin' as func,
    0.95 as velocity

from google_sheet('1BohUT5DscWO8JLV-o0uzPLWzaBC2NwJkGNelwGVZjEQ')
order by time
`.trim();

const SATIE = `
with notes as (
    select * from google_sheet('1Jb9K3yDyNVPIAP_i7AELDLBzm5bmQR3f3RuUwGAzWuc', 'C-Major')
)

select
    music.note,
    octave,
    notes.frequency::float * pow(2, music.octave::float - 5) as freq,
    time::float as time,
    length::float as length,
    'sin' as func,
    case
        when length::float < 1 then 0.95
        else velocity::float * 2
    end as velocity

from google_sheet('1_I2qc7jBJhrQBpgtoPjvEQ_nyLob1CruBLJ1UFbmueY') as music
join notes on music.note = notes.note
order by time
`;

const PUNK = `
with notes as (
    select * from google_sheet('1Jb9K3yDyNVPIAP_i7AELDLBzm5bmQR3f3RuUwGAzWuc', 'C-Major')
)


select
    music.note,
    octave,
    notes.frequency::float * pow(2, music.octave::float - 5) as freq,
    time::float as time,
    length::float as length,
    'sin' as func,
    velocity::float as velocity

from google_sheet('1EKocOJqU0tR0YK2uqUIJk3NwNuywMnWMIFMjx3Hrbek') music
join notes on music.note = notes.note
order by time
`.trim();

const HOWL = `
with notes as (
    select * from google_sheet('1Jb9K3yDyNVPIAP_i7AELDLBzm5bmQR3f3RuUwGAzWuc', 'C-Major')
)

select
    music.note,
    octave,
    notes.frequency::float * pow(2, music.octave::float - 5) as freq,
    time::float as time,
    length::float as length,
    'sin' as func,
    case
        when length::float < 1 then 0.95
        else velocity::float * 2
    end as velocity

from google_sheet('18dxRrnZfyVnILz_y2cjf1FHxc6jiUp_T7SxOROMWUUw') as music
join notes on music.note = notes.note
order by time
`;

const LA_DA_DEE = `
with notes as (
    select * from google_sheet('1Jb9K3yDyNVPIAP_i7AELDLBzm5bmQR3f3RuUwGAzWuc', 'C-Major')
)

select
    music.note,
    octave,
    notes.frequency::float * pow(2, music.octave::float - 5) as freq,
    time::float as time,
    length::float as length,
    'sin' as func,
    case
        when length::float < 1 then 0.95
        else velocity::float * 2
    end as velocity

from google_sheet('1ZZyhmM4Pk91s8jh17ErhQ_s_FRfvneGJtKQCJCKeZ9E') as music
join notes on music.note = notes.note
order by time
`;

const WII = `
with notes as (
    select * from google_sheet('1Jb9K3yDyNVPIAP_i7AELDLBzm5bmQR3f3RuUwGAzWuc', 'C-Major')
)

select
    music.note,
    octave,
    notes.frequency::float * pow(2, music.octave::float - 5) as freq,
    time::float as time,
    length::float as length,
    'sin' as func,
    0.9 as velocity

from google_sheet('1kmbkXsrSiqH11bH7uOpo1tbbJRHqwFxNxYDxEf5sJ9g') as music
join notes on music.note = notes.note
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
    PUNK: PUNK,
    SATIE: SATIE,
    HOWL: HOWL,
    "LA DA DEE": LA_DA_DEE,
    "WHO ME?": WII,
    ALTMAN: ALTMAN,
}


const QUERY_OPTS = Object.keys(QUERIES).map(key => {
    return {label: key, value: QUERIES[key]}
});

export default QUERY_OPTS;
