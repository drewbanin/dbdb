
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

const updateBuffer = (freqBuffer, countBuffer, row) => {
    const startTime = row.time;
    const freq = row.freq;
    const length = row.length || 1;
    const amplitude = row.amp || 0.5;
    const funcName = row.func || 'square';

    const startIndex = Math.floor(startTime * SAMPLE_RATE);
    const endIndex = Math.floor(startIndex + length * SAMPLE_RATE);

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

    for (let i=startIndex; i < endIndex; i++) {
        const time = i / SAMPLE_RATE;

        const waveFunc = (funcName === 'sqr') ? sqr : sin;
        let value = waveFunc(time, freq, amplitude)

        /*
        // longer notes get longer attacks
        const endTime = startTime + length;
        const offsetFromStart = time - startTime;
        const offsetUntilEnd = endTime - time;

        let fadeDelay = 0.25;
        const doFade = false;
        if (doFade && offsetFromStart < fadeDelay) {
            const amp = offsetFromStart / fadeDelay;
            value = value * amp;
        } else if (doFade && offsetUntilEnd < fadeDelay) {
            const amp = offsetUntilEnd / fadeDelay;
            value = value * amp;
        }
        */

        freqBuffer[i] += value;
        countBuffer[i] += 1;
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

    rows.forEach(row => {
        updateBuffer(freqBuffer, countBuffer, row);
    })

    for (let i=0; i < freqBuffer.length; i++) {
        buffer[i] = freqBuffer[i] / (countBuffer[i] || 1);
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


export {createBuffer, SAMPLE_RATE, sqr, sin}
