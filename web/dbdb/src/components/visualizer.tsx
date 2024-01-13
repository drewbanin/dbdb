import React, { useContext, useState, useEffect, useRef } from 'react';
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
    const totalTime = Math.max(...rows.map(r => r.time + r.length));

    const audioBuffer = audioCtx.createBuffer(
      numChannels,
      totalTime * SAMPLE_RATE,
      SAMPLE_RATE,
    );

    const buffer = audioBuffer.getChannelData(0);

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

        if (offsetEndIndex > buffer.length || offsetStartIndex > buffer.length) {
            console.log("End index=", endIndex, "is out of bounds", buffer.length);
            return
        } else if (offsetStartIndex > offsetEndIndex) {
            console.log("End time is before start time? how?");
            return;
        }

        for (let i=offsetStartIndex; i < offsetEndIndex; i++) {
            const time = i / SAMPLE_RATE;

            const waveFunc = (funcName == 'sin') ? sin : sqr;
            const value = waveFunc(time, freq, amplitude)

            buffer[i] += value;
        }
    })

    const source = audioCtx.createBufferSource();
    source.buffer = audioBuffer;

    const analyser = audioCtx.createAnalyser();
    analyser.fftSize = 256;

    source.connect(analyser);
    analyser.connect(audioCtx.destination);

    return [source, analyser, audioCtx, totalTime];
}

function FrequencyDomainViz({ analyser, offset }) {
  const [ time, setTime ] = useState(0);
  const [ xVals, setXVals ] = useState([]);
  const [ yVals, setYVals ] = useState([]);

  useAnimationFrame(deltaTime => {
      if (!analyser.current) {
          return
      }

      const bufferLength = analyser.current.frequencyBinCount;
      const dataArray = new Uint8Array(bufferLength);
      analyser.current.getByteFrequencyData(dataArray);

      const domain = SAMPLE_RATE / 2;
      const stepLength = domain / dataArray.length;

      const x = [];
      dataArray.forEach((v, i) => {
          const val = Math.floor(stepLength * i);
          x.push(val);
      });

      const y = dataArray;

      const xArray = x;
      const yArray = Array.from(y || []);

      setXVals(xArray);
      setYVals(yArray);

      setTime(t => t + deltaTime);
  })

  if (!analyser.current || xVals.length === 0 || yVals.length === 0) {
      return null;
  }

  return (
    <ResponsiveChartContainer
      series={[{ data: yVals, label: 'v', type: 'bar', color: '#000000' }]}
      xAxis={[{ scaleType: 'band', data: xVals }]}
      yAxis={[{ min: 0, max: 255 }]}
      margin={{
        left: 35,
        right: 35,
        top: 20,
        bottom: 35,
      }}
    >
        <BarPlot />
        <ChartsTooltip />
        <ChartsXAxis position="bottom" />
        <ChartsYAxis position="left" />
    </ResponsiveChartContainer>
  );
}

function TimeDomainViz({ analyser, offset }) {
  const [ time, setTime ] = useState(0);
  const [ xVals, setXVals ] = useState([]);
  const [ yVals, setYVals ] = useState([]);

  useAnimationFrame(deltaTime => {
      if (!analyser.current) {
          return
      }

      const bufferLength = analyser.current.frequencyBinCount;
      const dataArray = new Uint8Array(bufferLength);
      // state.analyser.getByteFrequencyData(dataArray);
      analyser.current.getByteTimeDomainData(dataArray);

      const y = [];
      dataArray.forEach(val => {
          const amp = (val / 128) - 1;
          y.push(amp);
      });
      const yArray = Array.from(y || []);

      setXVals(xData => {
          // pick up x-index where we left off?
          const x = [];
          const lastT = xData.length == 0 ? 0 : xData[xData.length - 1];

          dataArray.forEach((v, i) => {
              const val =  i + lastT;
              x.push(val);
          });

          return xData.concat(x)
      });

      setYVals(yData => yData.concat(yArray));

      setTime(t => t + deltaTime);
  })

  if (!analyser.current || xVals.length === 0 || yVals.length === 0) {
      return null;
  }

  const maxWindow = 512;
  const minX = xVals.length > maxWindow ? xVals[xVals.length - 1 - maxWindow] : 0;;
  const maxX = xVals.length > maxWindow ? xVals[xVals.length - 1] : maxWindow;

  return (
    <ResponsiveChartContainer
      series={[{ data: yVals, label: 'v', type: 'line', color: '#000000' }]}
      xAxis={[{ scaleType: 'linear', data: xVals, min: minX, max: maxX }]}
      yAxis={[{ min: -1, max: 1 }]}
      margin={{
        left: 35,
        right: 35,
        top: 20,
        bottom: 35,
      }}
    >
        <LinePlot />
        <ChartsXAxis position="bottom" />
        <ChartsYAxis position="left" />
    </ResponsiveChartContainer>
  );
}

function Visualizer() {
    const { result, schema } = useContext(QueryContext);
    const [ rows, setRows ] = result;
    const [ dataSchema, setSchema ] = schema;

    const [ playing, setPlaying ] = useState(null);
    const [ playTime, setPlayTime ] = useState(null);
    const [ FFT, setFFT ] = useState(null);

    const analyser = useRef(null);

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
        const [ newSource, newAnalyser, ctx, totalTime ] = createBuffer(mappedRows);

        analyser.current = newAnalyser;

        state.endTime = totalTime;
        state.source = newSource;
        state.context = ctx;
        // state.analyser = analyser;

        newSource.start();
    }, [playing, analyser])

    useEffect(() => {
      const interval = setInterval(() => {
        if (!state.context) {
            return
        }

        setPlayTime(state.context.currentTime);

        // state.analyser.fftSize = 64;
        // const bufferLength = state.analyser.frequencyBinCount;
        // const dataArray = new Uint8Array(bufferLength);
        // state.analyser.getByteFrequencyData(dataArray);
        // state.analyser.getByteTimeDomainData(dataArray);
        // setFFT(dataArray)

        if (state.context.currentTime > state.endTime) {
          console.log("done playing", state.context.currentTime, state.endTime)
          state.source.stop()
          setPlaying(false);
        }
      }, 10);

      return () => { console.log("Cancelling interval"); clearInterval(interval); }
    }, [playing, setPlaying, setPlayTime, setFFT]);

    const [ vizType, setVizType ] = useState('time');

    const showTime = vizType === 'time' && !!playing;
    const showFreq = vizType === 'freq' && !!playing;

    // const samples = rows ? rows.map((val) => val[1]) : [];
    // const startIndex = Math.floor(playTime * 44100);
    // const endIndex = startIndex + 441;
    // const samples = rows.slice(startIndex, endIndex).map(val => val[1]);
    // console.log("???", startIndex, endIndex, samples.length)
    
    // {!!playing && <TimeDomainViz analyser={analyser} offset={playTime} />}
    return (
        <>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">
                        <button
                            style={{ margin: 0, marginRight: 5, verticalAlign: 'top' }}
                            onClick={ e => setVizType('freq') }
                            className="light title">BAR</button>
                        <button
                            style={{ margin: 0, verticalAlign: 'top' }}
                            onClick={ e => setVizType('time') }
                            className="light title">LINE</button>
                    </div>
                </div>
            </div>
            <div className="configBox fixedHeight">
                {showFreq && <FrequencyDomainViz analyser={analyser} offset={playTime} />}
                {showTime && <TimeDomainViz analyser={analyser} offset={playTime} />}
            </div>
        </>
    )
}

export default Visualizer;
