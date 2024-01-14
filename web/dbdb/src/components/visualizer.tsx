import React, { useContext, useMemo, useState, useEffect, useRef } from 'react';
import { useSub } from '../Hooks.js';

import { formatBytes, formatNumber } from '../Helpers.js';
import { QueryContext } from '../Store.js';

import { ResponsiveChartContainer, BarPlot, LinePlot, ChartsXAxis, ChartsYAxis, ChartsTooltip } from '@mui/x-charts';
import { BarChart } from '@mui/x-charts/BarChart';

import { useAnimationFrame } from '../animate.js';

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

            const waveFunc = (funcName == 'sin') ? sin : sqr;
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

    source.connect(audioCtx.destination);

    return [source, audioCtx, totalTime];
}

function FrequencyDomainViz({ playing, rows, offset }) {
  if (!playing) {
      return null
  }

  // find tones that are playing at t=offset
  const beatOffset = 0.00;
  const relevantRows = rows.filter(row => {
      const startT = row.time + beatOffset;
      const endT = row.time + row.length - beatOffset;
      return startT <= offset && endT > offset
  })

  const maxFreq = rows.reduce((acc, val) => {
      if (val.freq > acc) {
          return val.freq;
      } else {
          return acc;
      }
  }, 0)

  const xDomain = maxFreq + 50;
  // want to create 100 steps...
  const stepSize = Math.floor(maxFreq / 50);
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
      xAxis={[{ scaleType: 'band', id: "x", data: xVals, min: 0, max: xDomain, tickNumber: 4 }]}
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
      const endT = row.time + row.length - beatOffset;
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
          const func = row.func === 'sin' ? sin : sqr;
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
      yAxis={[{ min: -maxY.current, max: maxY.current, tickNumber: 2 }]}
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

function Visualizer() {
    const { result, schema } = useContext(QueryContext);
    const [ rows, setRows ] = result;
    const [ dataSchema, setSchema ] = schema;

    const [ playing, setPlaying ] = useState(null);
    const [ playTime, setPlayTime ] = useState(null);
    const [ pausedAt, setPausedAt ] = useState(null);
    const [ muted, setMuted ] = useState(false);

    const source = useRef(null);
    const audioCtx = useRef(null);
    const state = {}

    useSub('QUERY_COMPLETE', (queryId) => {
        setPlaying(queryId);
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
        setPlaying(false);
        state.source = null;
        state.context = null;
    }

    const playPauseSound = () => {
        const currentTime = playTime;

        if (pausedAt !== null) {
            audioCtx.current.resume();
            setPausedAt(null);
        } else {
            audioCtx.current.suspend();
            setPausedAt(currentTime);
        }
    }

    const muteUnmuteSound = () => {
    }

    useEffect(() => {
        if (!playing || mappedRows.length === 0) {
            return
        }

        console.log("Playing for query:", playing, mappedRows);
        const [ newSource, ctx, totalTime ] = createBuffer(mappedRows);

        state.endTime = totalTime;
        state.source = newSource;
        state.context = ctx;

        audioCtx.current = ctx;
        source.current = newSource;

        newSource.start();
    }, [playing, source, audioCtx])

    useEffect(() => {
      const interval = setInterval(() => {
        if (!state.context) {
            return
        }

        setPlayTime(state.context.currentTime);

        if (state.context.currentTime > state.endTime) {
          console.log("done playing", state.context.currentTime, state.endTime)
          state.source.stop()
          setPlaying(false);
        }
      }, 10);

      return () => { console.log("Cancelling interval"); clearInterval(interval); }
    }, [playing, setPlaying, setPlayTime]);

    const [ vizType, setVizType ] = useState('time');

    const showTime = vizType === 'time';
    const showFreq = vizType === 'freq';

    // const samples = rows ? rows.map((val) => val[1]) : [];
    // const startIndex = Math.floor(playTime * 44100);
    // const endIndex = startIndex + 441;
    // const samples = rows.slice(startIndex, endIndex).map(val => val[1]);
    // console.log("???", startIndex, endIndex, samples.length)
    
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

                                <div style={{ margin: 0, float: 'right' }}>
                                    <button
                                        style={{ margin: 0, marginRight: 5, verticalAlign: 'top' }}
                                        onClick={ e => muteUnmuteSound() }
                                        className="light title">
                                        { muted ? 'UNMUTE' : 'MUTE' }
                                    </button>

                                    <button
                                        style={{ margin: 0, marginRight: 5, verticalAlign: 'top' }}
                                        onClick={ e => playPauseSound() }
                                        className="light title">
                                        { pausedAt === null ? 'PAUSE' : 'PLAY' }
                                    </button>

                                    <button
                                        style={{ margin: 0, verticalAlign: 'top' }}
                                        onClick={ e => stopSound() }
                                        className="light title">STOP</button>
                                </div>
                            </>
                    </div>
                </div>
            </div>
            <div className="configBox fixedHeight">
                {showFreq && <FrequencyDomainViz playing={playing} rows={mappedRows} offset={playTime} />}
                {showTime && <TimeDomainViz playing={playing} rows={mappedRows} offset={playTime} />}
            </div>
        </>
    )
}

export default Visualizer;
