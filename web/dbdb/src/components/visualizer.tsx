import React, { useContext, useState, useEffect, useRef } from 'react';
import { useSub } from '../Hooks.js';

import { formatBytes, formatNumber } from '../Helpers.js';
import { QueryContext } from '../Store.js';

import { ResponsiveChartContainer, BarPlot, LinePlot } from '@mui/x-charts';
import { BarChart } from '@mui/x-charts/BarChart';

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

const createBuffer = (rows) => {
    const audioCtx = new (window.AudioContext || window.webkitAudioContext)();

    const numChannels = 1;
    const sampleRate = 44100;

    // this is in seconds
    const totalTime = Math.max(...rows.map(r => r.time + r.length));

    const audioBuffer = audioCtx.createBuffer(
      numChannels,
      totalTime * sampleRate,
      sampleRate,
    );

    const buffer = audioBuffer.getChannelData(0);

    rows.forEach(row => {
        const startTime = row.time;
        const freq = row.freq;
        const length = row.length || 1;
        const amplitude = row.amp || 0.5;
        const funcName = row.func || 'square';

        const beatOffset = sampleRate * 0.01;

        const startIndex = Math.floor(startTime * sampleRate) + beatOffset;
        const endIndex = Math.floor(startIndex + length * sampleRate) - beatOffset;

        if (endIndex > buffer.length) {
            return
        }

        for (let i=startIndex; i < endIndex; i++) {
            const time = i / sampleRate;

            const waveFunc = (funcName == 'sin') ? sin : sqr;
            const value = waveFunc(time, freq, amplitude)

            buffer[i] += value;
        }
    })

    const source = audioCtx.createBufferSource();
    source.buffer = audioBuffer;

    const analyser = audioCtx.createAnalyser();

    source.connect(analyser);
    analyser.connect(audioCtx.destination);

    return [source, analyser, audioCtx, totalTime];
}

function Wave({ samples }) {
  if (!samples || samples.length == 0) {
      return;
  }
  // const xLabels = [0,1,2,3,4,5,6,7,8,9];
  const x = samples.map((v, i) => i);
  const y = samples;

  return (
    <ResponsiveChartContainer
      series={[{ data: y, label: 'v', type: 'bar', color: '#000000' }]}
      xAxis={[{ scaleType: 'band', data: x }]}
      margin={{
        left: 5,
        right: 5,
        top: 20,
        bottom: 20,
      }}
    >
        <BarPlot />
    </ResponsiveChartContainer>
  );
}

function Visualizer() {
    const { result, schema } = useContext(QueryContext);
    const [ rows, setRows ] = result;
    const [ dataSchema, setSchema ] = schema;

    const [ playing, setPlaying ] = useState(null);
    const [ playTime, setPlayTime ] = useState(0);
    const [ FFT, setFFT ] = useState(null);
    const state = {}

    useSub('QUERY_COMPLETE', (queryId) => {
        setPlaying(queryId);
    });

    useEffect(() => {
        if (!playing) {
            return
        }

        const mappedRows = rows.map(row => {
            const mapped = {};
            dataSchema.forEach((col, i) => {
                mapped[col] = row[i];
            })
            return mapped;
        })

        console.log("Playing for query:", playing);
        const [ newSource, analyser, ctx, totalTime ] = createBuffer(mappedRows);

        // TODO : Use actual max, not last value
        state.endTime = totalTime;
        console.log("Setting end time to", state.endTime);
        state.source = newSource;
        state.context = ctx;
        state.analyser = analyser;

        newSource.start();
    }, [playing])

    useEffect(() => {
      const interval = setInterval(() => {
        if (!state.context) {
            return
        }

        setPlayTime(state.context.currentTime);
        console.log("Play at t=", state.context.currentTime);

        state.analyser.fftSize = 256;
        const bufferLength = state.analyser.frequencyBinCount;
        const dataArray = new Uint8Array(bufferLength);
        state.analyser.getByteFrequencyData(dataArray);
        setFFT(dataArray)

        if (state.context.currentTime > state.endTime) {
          console.log("done playing", state.context.currentTime, state.endTime)
          state.source.stop()
          setPlaying(false);
        }
      }, 10);

      return () => { console.log("Cancelling interval"); clearInterval(interval); }
    }, [playing, setPlaying, setPlayTime, setFFT]);

    // const samples = rows ? rows.map((val) => val[1]) : [];
    // const startIndex = Math.floor(playTime * 44100);
    // const endIndex = startIndex + 441;
    // const samples = rows.slice(startIndex, endIndex).map(val => val[1]);
    // console.log("???", startIndex, endIndex, samples.length)
    const samples = playing ? Array.from(FFT || []) : [];
    
    return (
        <>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">
                        <span className="light title">VIZ</span>
                    </div>
                </div>
            </div>
            <div className="configBox fixedHeight">
                <Wave samples={samples} offset={playTime} />
            </div>
        </>
    )
}

export default Visualizer;
