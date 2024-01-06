import React, { useContext, useState, useEffect, useRef } from 'react';
import { useSub } from '../Hooks.js';

import { LineDetail } from './LineDetail.js';
import { formatBytes, formatNumber } from '../Helpers.js';
import { QueryContext } from '../Store.js';

const createBuffer = (rows) => {
    const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const numChannels = 1;
    const sampleRate = 44100;
    const buf = audioCtx.createBuffer(
      numChannels,
      rows.length,
      sampleRate,
    );

    const nowBuffering = buf.getChannelData(0);
    for (let i = 0; i < rows.length; i++) {
      nowBuffering[i] = rows[i][1]
    }

    const source = audioCtx.createBufferSource();
    source.buffer = buf;
    source.connect(audioCtx.destination);

    return source;
}


function Visualizer() {
    const { result, schema } = useContext(QueryContext);

    const [ playing, setPlaying ] = useState(null);
    const [ source, setSource ] = useState(null);

    const [ rows, setRows ] = result;
    const [ dataSchema, setSchema ] = schema;

    useSub('QUERY_COMPLETE', (queryId) => {
        setPlaying(queryId);

        // const validSchema = validateSchema(dataSchema);

    });

    useEffect(() => {
        if (!playing) {
            return
        }

        console.log("Playing for query:", playing);
        const newSource = createBuffer(rows);
        setSource(newSource);
        newSource.start();
    }, [playing])


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
                {playing ? 'playing!' : '...'}
            </div>
        </>
    )
}

export default Visualizer;
