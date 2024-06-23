
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

const kick = (t, f, a) => {
    const kickLength = 0.2;
    const sustainLength = 0.8;

    const attackPct = Math.min(t, kickLength) / kickLength;
    const sustainPct = Math.min(t, sustainLength) / sustainLength;

    const kickFreq = sin(t, f, 1 - attackPct);
    const kickSustain = sin(t, f / 2, 1 - sustainPct);

    return kickFreq + kickSustain;
}

const getWaveFunc = (funcName) => {
    const funcMap = {
        'sin': sin,
        'sqr': sqr,
        'kick': kick,
    }

    return funcMap[funcName] || sin;
}


const SAMPLE_RATE = 44100;

const updateBuffer = (freqBuffer, countBuffer, row) => {
    const startTime = row.time;
    const freq = row.freq;
    const length = row.length || 1;
    const amplitude = row.amp || 0.5;
    const funcName = row.func || 'sin';

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

    const doFade = row.velocity !== undefined;
    const fadeDelayPct = (1 - row.velocity) / 1.0;
    const fadeDelay = fadeDelayPct * length / 2;

    const SAMPLE_LENGTH = 1.0 / SAMPLE_RATE;
    for (let i=startIndex; i < endIndex; i++) {
        const time = i / SAMPLE_RATE;

        /*
         * If we start calculating the value of the wave at t=time, then sin waves
         * could start playing from some value other than zero. Instead, we want to
         * use our own x domain that starts at zero while tracking the samples in
         * our actual note.
         */
        const waveFunc = getWaveFunc(funcName);
        let value = waveFunc(time, freq, amplitude)

        const endTime = startTime + length;
        const offsetFromStart = time - startTime;
        const offsetUntilEnd = endTime - time;

        // Fade in only if velocity is set...
        if (doFade && offsetFromStart < fadeDelay) {
            const amp = offsetFromStart / fadeDelay;
            value = value * amp;
        }

        // always fade out to avoid clipping
        const fadeOutDelay = length / 2;
        if (offsetUntilEnd < fadeOutDelay) {
            const fadeOutAmp = offsetUntilEnd / fadeOutDelay;
            value = value * fadeOutAmp;
        }

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
        const value = freqBuffer[i] / (countBuffer[i] || 1);
        buffer[i] =  Math.max(Math.min(value, 1), -1);
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
