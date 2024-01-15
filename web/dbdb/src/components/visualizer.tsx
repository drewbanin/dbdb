import React, { useContext, useMemo, useState, useEffect, useRef } from 'react';
import { useSub } from '../Hooks.js';

import { formatBytes, formatNumber } from '../Helpers.js';
import { QueryContext } from '../Store.js';

import { ResponsiveChartContainer, BarPlot, LinePlot, ChartsXAxis, ChartsYAxis, ChartsTooltip } from '@mui/x-charts';
import { BarChart } from '@mui/x-charts/BarChart';

import { useAnimationFrame } from '../animate.js';

import Stack from '@mui/material/Stack';
import Box from '@mui/material/Box';
import { SparkLineChart } from '@mui/x-charts/SparkLineChart';

const sin = (t, f, a) => {
    return a * Math.sin(2 * Math.PI * t * f);
}

const sqr = (t, f, a) => {
    const val = sin(t, f, a);
    if (val > 0) {
        return a;
    } else {
        return -a
    }
}

const SAMPLE_RATE = 44100;

const createBuffer = (rows) => {
    const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    audioCtx.suspend();

    const numChannels = 1;

    // this is in seconds
    const totalTime = Math.max(...rows.map(r => r.time + (r.length || 1)));

    const audioBuffer = audioCtx.createBuffer(
      numChannels,
      totalTime * SAMPLE_RATE,
      SAMPLE_RATE,
    );

    const freqBuf = new Float32Array(totalTime * SAMPLE_RATE);
    const countBuf = new Float32Array(totalTime * SAMPLE_RATE);

    rows.forEach(row => {
        const startTime = row.time;
        const freq = row.freq;
        const length = row.length || 1;
        const amplitude = row.amp || 0.5;
        const funcName = row.func || 'square';

        // const beatOffset = SAMPLE_RATE * 0.01;
        const beatOffset = 0;

        const startIndex = Math.floor(startTime * SAMPLE_RATE);
        const endIndex = Math.floor(startIndex + length * SAMPLE_RATE);

        const offsetStartIndex = startIndex + beatOffset;
        const offsetEndIndex = endIndex - beatOffset;

        if (offsetEndIndex > freqBuf.length || offsetStartIndex > freqBuf.length) {
            console.log("End index=", endIndex, "is out of bounds", freqBuf.length);
            return
        } else if (offsetStartIndex > offsetEndIndex) {
            console.log("End time is before start time? how?");
            return;
        }

        for (let i=offsetStartIndex; i < offsetEndIndex; i++) {
            const time = i / SAMPLE_RATE;

            const waveFunc = (funcName == 'sqr') ? sqr : sin;
            const value = waveFunc(time, freq, amplitude)

            freqBuf[i] += value;
            countBuf[i] += 1;
        }
    })

    const buffer = audioBuffer.getChannelData(0);
    for (let i=0; i < freqBuf.length; i++) {
        const count = countBuf[i];
        const freq = freqBuf[i];

        let normed;
        if (count === 0) {
            normed = 0;
        } else {
            normed = freq / count;
        }

        let clipped;
        if (normed > 1) {
            clipped = 1;
        } else if (normed < -1) {
            clipped = -1;
        } else {
            clipped = normed;
        }

        buffer[i] = clipped;
    }

    const source = audioCtx.createBufferSource();
    source.buffer = audioBuffer;

    const gain = audioCtx.createGain();

    source.connect(gain);
    gain.connect(audioCtx.destination)

    return [source, audioCtx, gain, totalTime];
}

function FrequencyDomainViz({ playing, rows, offset }) {
  if (!playing) {
      return null
  }

  // find tones that are playing at t=offset
  const beatOffset = 0.00;
  const relevantRows = rows.filter(row => {
      const startT = row.time + beatOffset;
      const endT = row.time + (row.length || 1) - beatOffset;
      return startT <= offset && endT > offset
  })

  const maxFreq = rows.reduce((acc, val) => {
      if (val.freq > acc) {
          return val.freq;
      } else {
          return acc;
      }
  }, 0)

  const xDomain = maxFreq + 1;
  let stepSize = Math.floor(xDomain / 50);

  const xVals = [];
  const yVals = [];

  for (let i=0; i < (xDomain / stepSize); i += 1) {
      const xStart = Math.floor(i * stepSize);
      const xEnd = Math.ceil(xStart + stepSize);

      const activeNotes = relevantRows.filter(row => {
          return row.freq >= xStart && row.freq < xEnd;
      });
      // todo : account for amplitude?
      const numActiveNotes = activeNotes.length;
      const yVal = numActiveNotes;

      xVals.push(xStart);
      yVals.push(yVal);
  }

  if (xVals.length == 0 || yVals.length == 0) {
      xVals.push(0);
      yVals.push(0);
  }

  const formattedYVals = {
    data: yVals,
    label: 'Amplitude',
    type: 'bar',
    color: '#000000',
  }

  return (
    <ResponsiveChartContainer
      series={[ formattedYVals ]}
      xAxis={[{ scaleType: 'band', id: "x", data: xVals, min: 0, max: xDomain }]}
      yAxis={[{ id: "y", min: 0, max: 1 }]}
      margin={{
        left: 15,
        right: 35,
        top: 20,
        bottom: 35,
      }}
    >
        <BarPlot />
        <ChartsTooltip trigger={"axis"} />
        <ChartsXAxis axisId="x" position="bottom" />
    </ResponsiveChartContainer>
  );
}

function TimeDomainViz({ playing, rows, offset }) {
  const yBuffer = useRef();
  const maxY = useRef(1);

  if (!playing) {
      return null
  }

  const beatOffset = 0.01;
  const relevantRows = rows.filter(row => {
      const startT = row.time + beatOffset;
      const endT = row.time + (row.length || 1) - beatOffset;
      return startT <= offset && endT >= offset
  })

  const xDomain = 10000;
  const xVals = [];
  const yVals = [];

  for (let i=0; i < xDomain; i++) {
      xVals.push(i);

      const scaleFactor = SAMPLE_RATE * 10;

      const freqs = relevantRows.map(row => {
          const amp = row.amp || 1;
          const func = row.func === 'sqr' ? sqr : sin;
          return func(i / scaleFactor, row.freq, amp)
      })

      const yTotal = freqs.reduce((acc, val) => acc + val, 0);
      // const yVal = freqs.length === 0 ? 0 : yTotal / freqs.length;
      const yVal = yTotal;
      yVals.push(yVal);
  }

  let vizY;
  if (!yBuffer.current) {
      // initialize the buffer
      // Each element has a starting point, ending point, and step

      const newYBuf = yVals.map(val => {
        return {to: val, current: val, step: 0}
      })

      yBuffer.current = newYBuf;
      vizY = yVals;

  } else {
      // grab yBuffer and add `from` to step value to get towards `to`
      // ideally we would spline (?) this, but can go linear for now...

      const maxSteps = 5;
      const animatedVals = yBuffer.current.map((existing, i) => {
          const targetYVal = yVals[i];
          const increment = (targetYVal - existing.current) / maxSteps;
          let newVal = existing.current + increment;

          if (Math.abs(newVal) > maxY.current) {
              maxY.current = Math.abs(newVal);
          }

          return {to: targetYVal, current: newVal, step: existing.step + 1, increment: increment}
      })

      vizY = animatedVals.map(val => val.current);
      yBuffer.current = animatedVals;
  }

  return (
    <ResponsiveChartContainer
      series={[{ data: vizY, label: 'v', type: 'line', color: '#000000' }]}
      xAxis={[{ scaleType: 'linear', data: xVals, min: 0, max: xDomain, tickNumber: 5 }]}
      yAxis={[{ min: -maxY.current - 0.1, max: maxY.current + 0.1, tickNumber: 2 }]}
      margin={{
        left: 15,
        right: 35,
        top: 20,
        bottom: 35,
      }}
    >
        <LinePlot />
        <ChartsTooltip trigger={"axis"} />
        <ChartsXAxis position="bottom" />
    </ResponsiveChartContainer>
  );
}

export function Visualizer() {
    const { result, schema } = useContext(QueryContext);
    const [ rows, setRows ] = result;
    const [ dataSchema, setSchema ] = schema;

    const [ playTime, setPlayTime ] = useState(null);

    const [ audioState, setAudioState ] = useState('waiting');
    const [ muted, setMuted ] = useState(false);

    const source = useRef(null);
    const audioCtx = useRef(null);
    const gain = useRef(null);
    const endTime = useRef(null);
    const state = {}

    useSub('QUERY_COMPLETE', (queryId) => {
        console.log("Query is complete: ", queryId);
        setAudioState('waiting');
    });

    const mappedRows = rows.map(row => {
        const mapped = {};
        dataSchema.forEach((col, i) => {
            mapped[col] = row[i];
        })
        return mapped;
    })

    const stopSound = () => {
        source.current.stop();

        setPlayTime(0);
        setAudioState('done');
    }

    const playPauseSound = () => {
        const currentTime = playTime;

        if (audioState == 'waiting') {
            source.current.start();
            setAudioState('playing');
            audioCtx.current.resume();
        } else if (audioState == 'playing') {
            setAudioState('paused');
            audioCtx.current.suspend();
        } else if (audioState == 'paused') {
            setAudioState('playing');
            audioCtx.current.resume();
        }
    }

    const muteUnmuteSound = () => {
        if (muted) {
            gain.current.gain.value = 1;
        } else {
            gain.current.gain.value = 0;
        }

        setMuted(!muted);
    }

    useEffect(() => {
        console.log("CHECKING USE EFFECT?", mappedRows.length);
        if (mappedRows.length === 0) {
            return
        }

        console.log("Playing for query:", mappedRows.length, "rows");
        const [ newSource, ctx, newGain, totalTime ] = createBuffer(mappedRows);

        endTime.current = totalTime;

        audioCtx.current = ctx;
        source.current = newSource;
        gain.current = newGain;

        audioCtx.current.suspend();
        // newSource.start();
        setAudioState('waiting');
    }, [rows, source, audioCtx, gain])

    useEffect(() => {
      console.log("Running effect?", audioState);

      if (audioState === 'done') {
          console.log("Audio state is done - exiting");
          return
      }

      const interval = setInterval(() => {
        if (!audioCtx.current || audioCtx.current.state === 'suspended') {
            return
        }

        setPlayTime(audioCtx.current.currentTime);

        if (audioCtx.current.currentTime > endTime.current) {
          console.log("done playing", audioCtx.current.currentTime, endTime.current)
          source.current.stop()
          setAudioState('done');
        }
      }, 10);

      return () => { console.log("Cancelling interval"); clearInterval(interval); }
    }, [setPlayTime, setAudioState, audioState, audioCtx, endTime]);

    const [ vizType, setVizType ] = useState('freq');

    const showTime = vizType === 'time';
    const showFreq = vizType === 'freq';

    const isPlaying = audioState == 'playing' || audioState == 'paused';

    let playPauseLabel;
    let showMediaControls;
    let isReady = mappedRows.length > 0;
    if (audioState == 'waiting' || audioState == 'paused') {
        playPauseLabel = 'PLAY';
        showMediaControls = isReady;
    } else if (audioState == 'playing') {
        playPauseLabel = 'PAUSE';
        showMediaControls = isReady;
    } else if (audioState == 'done') {
        playPauseLabel = audioState
        showMediaControls = false;
    }

    console.log("Rendering at t=", playTime);
    return (
        <>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">
                            <>
                                <button
                                    style={{ margin: 0, marginRight: 5, verticalAlign: 'top' }}
                                    onClick={ e => setVizType('freq') }
                                    className="light title">FREQ</button>
                                <button
                                    style={{ margin: 0, verticalAlign: 'top' }}
                                    onClick={ e => setVizType('time') }
                                    className="light title">TIME</button>

                                { showMediaControls && <div style={{ margin: 0, float: 'right' }}>
                                    <button
                                        style={{ margin: 0, marginRight: 5, verticalAlign: 'top' }}
                                        onClick={ e => muteUnmuteSound() }
                                        className="light title">
                                        { muted ? 'UNMUTE' : 'MUTE' }
                                    </button>

                                    <button
                                        style={{ margin: 0, marginRight: 5, verticalAlign: 'top' }}
                                        onClick={ e => playPauseSound() }
                                        className="light title"> {playPauseLabel}
                                    </button>

                                    <button
                                        style={{ margin: 0, verticalAlign: 'top' }}
                                        onClick={ e => stopSound() }
                                        className="light title">STOP</button>
                                </div>}
                            </>
                    </div>
                </div>
            </div>
            <div className="configBox fixedHeight">
                {showFreq && <FrequencyDomainViz playing={isPlaying} rows={mappedRows} offset={playTime} />}
                {showTime && <TimeDomainViz playing={isPlaying} rows={mappedRows} offset={playTime} />}
            </div>
        </>
    )
}

export function VizPlaceHolder() {
    const { schema, result } = useContext(QueryContext);
    const [ dataSchema, setSchema ] = schema;
    const [ rows, setRows ] = result;

    const viz = (dataSchema || []).map((colName, index) => {
        if (rows.length == 0) {
            return null;
        }

        const exampleRow = rows[0][index];
        if (typeof exampleRow !== 'number') {
            return null;
        }

        const data = rows.map(r => r[index]);
        return (
            <Box sx={{ flexGrow: 1 }} key={"viz" + index}>
                <span style={{ fontSize: 12, borderBottom: '1px solid #000000' }}>
                    Column: {colName.toUpperCase()}
                </span>
                <SparkLineChart data={data} height={50} colors={["#000000"]} />
            </Box>

        )
    })

    const vizData = viz.filter(v => v !== null);

    return (<>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">VIZ
                    </div>
                </div>
            </div>
            <div className="configBox fixedHeight"
                 style={{ overflowY: 'scroll', overflowX: 'clip' }}>
                <Stack direction="column" sx={{ width: '95%' }} >
                    {vizData}
                </Stack>
            </div>
        </>);

}
