import React, { useState, useEffect } from 'react';

const FRAMES = [
    ".",
    "..",
    "...",
]


function Spinner({loading}) {
    const [index, setIndex] = useState(0);

    const frameIndex = index % FRAMES.length;

   useEffect(() => {
        const interval = setInterval(() => {
            setIndex(index + 1);
        }, 200);

        return () => clearInterval(interval);
    }, [index]);

    if (!loading) {
        return <span />
    }



    return <span>{FRAMES[frameIndex]}</span>
}

export default Spinner;
