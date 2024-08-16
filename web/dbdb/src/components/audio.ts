
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

const synth = (t, f, a) => {
    const sinVal = sin(t, f, 1);
    return Math.abs(sinVal) * a;
}

const tri = (t, f, a) => {
    const y = Math.abs((t % f) - f / 2) / (f / 2);
    return y * a;
}

const getWaveFunc = (funcName) => {
    const funcMap = {
        'sin': sin,
        'sqr': sqr,
        'synth': synth,
        'tri': tri,
    }

    return funcMap[funcName] || sin;
}


const SAMPLE_RATE = 44100;

const updateBuffer = (freqBuffer, countBuffer, notesBuffer, row) => {
    const startTime = row.time;
    const freq = row.freq;
    const length = row.length || 1;
    const amplitude = row.amp || 0.5;
    const funcName = row.func || 'sin';

    const startIndex = Math.floor(startTime * SAMPLE_RATE);
    const endIndex = Math.ceil(startIndex + length * SAMPLE_RATE) + 1;

    if (freqBuffer.length !== countBuffer.length) {
        console.log("Freq and count buffers have different sizes");
        return;
    } else if (endIndex > freqBuffer.length || startIndex > freqBuffer.length) {
        console.log("End index=", endIndex, "is out of bounds", freqBuffer.length);
        return
    } else if (startIndex > endIndex) {
        console.log("End time is before start time? how?");
        return;
    }

    const doFade = row.velocity !== undefined;
    const fadeDelayPct = (1 - row.velocity) / 1.0;
    const fadeDelay = fadeDelayPct * length / 2;

    const SAMPLE_SIZE = 1 / SAMPLE_RATE;
    let tick = 0;
    for (let i=startIndex; i < endIndex; i++) {
        const time = i / SAMPLE_RATE;

        /*
         * Increment by `tick` (which starts at zero) so that
         * the sin wave starts and ends at 0 amplitude. This
         * helps avoid clicking sounds....
         */
        const waveFunc = getWaveFunc(funcName);
        let value = waveFunc(tick, freq, amplitude)

        const endTime = startTime + length;
        const offsetFromStart = time - startTime;
        const offsetUntilEnd = endTime - time;

        // Fade in only if velocity is set...
        if (doFade && offsetFromStart < fadeDelay) {
            const amp = offsetFromStart / fadeDelay;
            value = value * amp;
        }

        // always fade out to avoid clipping
        const fadeOutDelay = Math.max(length / 10, 0.05);
        if (offsetUntilEnd < fadeOutDelay) {
            const fadeOutAmp = offsetUntilEnd / fadeOutDelay;
            // const fadeOutAmp = Math.pow(2, (offsetUntilEnd - fadeOutDelay));
            value = value * fadeOutAmp;
        }

        if (i > 99020 && i < 99430 && Math.abs(value) > 0.1) {
            //if (i > 99120 && i < 99320 && row.note === 'C') {
            //console.log(row.row, "tick=", i, "note=", row.note, "time=", time.toFixed(3), "value=", value.toFixed(3));
        }

        freqBuffer[i] += value;
        countBuffer[i] += 1;
        if (!notesBuffer[i]) {
            notesBuffer[i] = [value]
        } else {
            notesBuffer[i].push(value)
        }

        //if (row.row === '22') {
        //    console.log("time=", time, "tick=", tick, "value=", value);
        //}

        tick += SAMPLE_SIZE;
    }
}

const createBuffer = (rows) => {
    const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    audioCtx.suspend();

    // this is in seconds
    const totalTime = Math.max(...rows.map(r => r.time + (r.length || 1)));
    const bufSize = totalTime * SAMPLE_RATE;

    const audioBuffer = audioCtx.createBuffer(1, bufSize, SAMPLE_RATE);
    const buffer = audioBuffer.getChannelData(0);


    const freqBuffer = new Float32Array(bufSize);
    const countBuffer = new Float32Array(bufSize);
    const notesBuffer = {}

    rows.forEach(row => {
        updateBuffer(freqBuffer, countBuffer, notesBuffer, row);
    })


    let maxNotesPlayed = 0;
    for (let i=0; i < freqBuffer.length; i++) {
        if (countBuffer[i] > maxNotesPlayed) {
            maxNotesPlayed = countBuffer[i];
        }
    }

    for (let i=0; i < freqBuffer.length; i++) {
        const value = freqBuffer[i] / (maxNotesPlayed || 1);
        buffer[i] =  Math.max(Math.min(value, 1), -1);

        if (i > 429964 && i < 429984) {
            console.log("tick=", i, "value=", buffer[i], "freqTotal=", freqBuffer[i], "count=", countBuffer[i]);
            console.log(notesBuffer[i]);
        }
    }

    const source = audioCtx.createBufferSource();
    source.buffer = audioBuffer;

    const gain = audioCtx.createGain();
    source.connect(gain);

    gain.connect(audioCtx.destination)

    return {
        source,
        gain,
        totalTime,
        context: audioCtx,
    }
}

export {createBuffer, SAMPLE_RATE, getWaveFunc}
