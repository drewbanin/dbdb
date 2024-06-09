import React, { useContext, useState, useEffect, useRef, useMemo } from 'react';
import { useSub } from '../Hooks.js';

import { QueryContext } from '../Store.js';
import VolumePicker from './volumePicker.tsx';
import FullScreenIcon from './fullScreen.js';

import {
    BarPlot,
    ChartsTooltip,
    ChartsXAxis,
    ChartsYAxis,
    LinePlot,
    PiePlot,
    ResponsiveChartContainer,
    ScatterPlot,
    pieArcLabelClasses,
} from '@mui/x-charts';

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

            const waveFunc = (funcName === 'sqr') ? sqr : sin;
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
  const steps = 50;
  let stepSize = Math.max(Math.floor(xDomain / steps), 1);

  if (stepSize < 1) {
      stepSize = 1;
  }

  const xVals = [];
  const yVals = [];

  for (let i=0; i < steps ; i += 1) {
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

  if (xVals.length === 0 || yVals.length === 0) {
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

function PieViz({ playing, rows, offset }) {
  if (!playing) {
      return null
  }

  const maxFreq = Math.max(...rows.map(row => row.freq))

  // find tones that are playing at t=offset
  const beatOffset = 0.00;
  const relevantRows = rows.filter(row => {
      const startT = row.time + beatOffset;
      const endT = row.time + (row.length || 1) - beatOffset;
      return startT <= offset && endT > offset
  })

  const series = relevantRows.map((row, i) => {
      const grayness = Math.floor((row.freq / maxFreq) * 240)
      const color = `rgb(${grayness}, ${grayness}, ${grayness})`;

      return {
          id: i,
          value: row.amp || 1,
          label: Math.floor(row.freq) + "hz",
          color: color,
      }
  })

  return (
    <ResponsiveChartContainer
      series={[ { data: series, type: 'pie', arcLabel: 'label' } ]}
      sx={{
        [`& .${pieArcLabelClasses.root}`]: {
          fill: 'white',
          fontSize: 12
        },
      }}
      margin={{
        left: 15,
        right: 35,
        top: 20,
        bottom: 35,
      }}
    >
        <PiePlot />
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
    const { result, schema, volume, fullscreen } = useContext(QueryContext);
    const [ rows ] = result;
    const [ dataSchema ] = schema;
    const [ musicVolume ] = volume;
    const [ isFullscreen, setFullscreen ] = fullscreen;

    const [ playTime, setPlayTime ] = useState(null);
    const [ audioState, setAudioState ] = useState('waiting');

    const source = useRef(null);
    const audioCtx = useRef(null);
    const gain = useRef(null);
    const endTime = useRef(null);

    useSub('QUERY_COMPLETE', (queryId) => {
        console.log("Query is complete: ", queryId);
        setAudioState('waiting');
    });

    const mappedRows = useMemo(() => {
        return rows.map(row => {
            const mapped = {};
            dataSchema.forEach((col, i) => {
                mapped[col] = row[i];
            })
            return mapped;
        })

    }, [rows, dataSchema]);


    const volumeRef = useRef(musicVolume);

    const stopSound = () => {
        if (source.current && source.current.context.state === 'running') {
            source.current.stop();
        }
        setPlayTime(0);
        source.current = null;
        setAudioState('done');
    }

    const playPauseSound = () => {
        if (audioState === 'waiting') {
            source.current.start();
            setAudioState('playing');
            console.log("Audio: playing")
            audioCtx.current.resume();
        } else if (audioState === 'playing') {
            setAudioState('paused');
            console.log("Audio: paused")
            audioCtx.current.suspend();
        } else if (audioState === 'paused') {
            setAudioState('playing');
            console.log("Audio: playing")
            audioCtx.current.resume();
        }
    }

    useEffect(() => {
        if (mappedRows.length === 0) {
            stopSound();
            return
        }

        const [ newSource, ctx, newGain, totalTime ] = createBuffer(mappedRows);

        endTime.current = totalTime;

        audioCtx.current = ctx;
        source.current = newSource;

        // initialized w/ saved volume level
        gain.current = newGain;
        console.log("initialized volume to", volumeRef.current);
        gain.current.gain.value = volumeRef.current;

        audioCtx.current.suspend();
        // newSource.start();
        setAudioState('waiting');
    }, [mappedRows, source, audioCtx, gain, setAudioState])

    useEffect(() => {
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

      return () => { clearInterval(interval); }
    }, [setPlayTime, setAudioState, audioState, audioCtx, endTime]);

    const [ vizType, setVizType ] = useState('freq');

    const showTime = vizType === 'time';
    const showFreq = vizType === 'freq';
    const showPie = vizType === 'pie';

    const isPlaying = audioState === 'playing' || audioState === 'paused';

    let playPauseLabel;
    let showMediaControls;
    let isReady = mappedRows.length > 0;
    if (audioState === 'waiting' || audioState === 'paused') {
        playPauseLabel = 'PLAY';
        showMediaControls = isReady;
    } else if (audioState === 'playing') {
        playPauseLabel = 'PAUSE';
        showMediaControls = isReady;
    } else if (audioState === 'done') {
        playPauseLabel = audioState
        showMediaControls = false;
    }

    function updateVolume(level) {
        console.log("Setting gain to", level);
        gain.current.gain.value = level;
        volumeRef.current = level;
    }

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
                                    style={{ margin: 0, marginRight: 5, verticalAlign: 'top' }}
                                    onClick={ e => setVizType('time') }
                                    className="light title">TIME</button>
                                <button
                                    style={{ margin: 0, verticalAlign: 'top' }}
                                    onClick={ e => setVizType('pie') }
                                    className="light title">BEN</button>

                                <div style={{ margin: 0, float: 'right' }}>
                                    { showMediaControls && 
                                        <>
                                            <VolumePicker onVolumeChange={updateVolume} />

                                            <button
                                                style={{ margin: 0, marginRight: 5, verticalAlign: 'top' }}
                                                onClick={ e => playPauseSound() }
                                                className="light title"> {playPauseLabel}
                                            </button>

                                            <button
                                                style={{ margin: 0, verticalAlign: 'top', marginRight: 5 }}
                                                onClick={ e => stopSound() }
                                                className="light title">STOP</button>
                                        </>
                                    }

                                    <button
                                        onClick={e => setFullscreen(!isFullscreen)}
                                        style={{ margin: 0, verticalAlign: 'top' }}
                                        className="light title">
                                        <FullScreenIcon isFullscreen={isFullscreen} />
                                    </button>
                                </div>
                            </>
                    </div>
                </div>
            </div>
            <div className="configBox minFixedHeight">
                {showFreq && <FrequencyDomainViz playing={isPlaying} rows={mappedRows} offset={playTime} />}
                {showTime && <TimeDomainViz playing={isPlaying} rows={mappedRows} offset={playTime} />}
                {showPie && <PieViz playing={isPlaying} rows={mappedRows} offset={playTime} />}
            </div>
        </>
    )
}

export function XYViz() {
    const { schema, result } = useContext(QueryContext);
    const [ dataSchema ] = schema;
    const [ rows ] = result;

    // collects series into list of {name, x, y}
    
    const series = {};
    (dataSchema || []).forEach((colName, index) => {
        let col = null;
        let axis = null;
        let color = null;

        if (colName.endsWith("_x")) {
            axis = 'x';
            col = colName.replace('_x', '');
        } else if (colName.endsWith("_y")) {
            axis = 'y';
            col = colName.replace('_y', '');
        } else if (colName.endsWith('_color')) {
            col = colName.replace('_color', '');
            color = true
        }

        if (axis && col) {
            if(!series[col]) {
                series[col] = {name: col}
            }

            const data = rows.map(r => r[index]);
            series[col][axis] = data;
        } else if (color && col) {
            if(!series[col]) {
                series[col] = {name: col}
            }

            series[col]['color'] = rows.map(r => r[index]);
        }
    })

    const seriesNames = Object.keys(series);

                // <SparkLineChart data={data} height={50} colors={["#000000"]} />
    const viz = seriesNames.map(name => {
        const ser = series[name];
        const color = (ser.color && ser.color.length) ? ser.color[0] : '#000000';
        return {
            label: name.toUpperCase(),
            type: 'scatter',
            color: color,
            data: ser.x.map((x, i) => {
                return {
                    x: ser.x[i],
                    y: ser.y[i],
                    id: 'x-' + ser.x[i] + '-y-' + ser.y[i],
                }
            })
        }
    })

    return (<>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">VIZ
                    </div>
                </div>
            </div>
            <div className="configBox minFixedHeight">
                <ResponsiveChartContainer
                  series={viz}
                  margin={{
                    left: 40,
                    right: 35,
                    top: 20,
                    bottom: 35,
                  }}
                >
                    <ChartsXAxis position="bottom" />
                    <ChartsYAxis position="left" />
                    <ScatterPlot />
                </ResponsiveChartContainer>
            </div>
        </>);

}

export function PlaceholderViz({ text }) {
    const showText = text || 'RUN A QUERY';
    return (<>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">VIZ
                    </div>
                </div>
            </div>
            <div className="configBox minFixedHeight">
                <div>{showText}</div>
            </div>
        </>);

}
