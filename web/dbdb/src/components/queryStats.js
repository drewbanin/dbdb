import React, { useState } from 'react';
import { useSub } from '../Hooks.js';

import { LineDetail } from './LineDetail.js';
import { formatBytes, formatNumber } from '../Helpers.js';

function QueryStats() {
    const [queryStats, setQueryStats] = useState(null)

    useSub('QueryStats', (stats) => {
        console.log(stats);
        setQueryStats(stats);
    });

    const formatElapsed = (elapsed) => {
        if (!elapsed) {
            return '?';
        }

        return elapsed.toFixed(2) + " seconds";
    }

    const timeElapsed = queryStats ? formatElapsed(queryStats.elapsed) : "?";
    const bytesRead = queryStats ? formatBytes(queryStats.bytes_read) : "?";

    return (
        <>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">
                        <span className="light title">QUERY STATS</span>
                    </div>
                </div>
            </div>
            <div className="configBox">
                <LineDetail label={"TIME ELAPSED"} value={timeElapsed} />
                <LineDetail label={"BYTES READ"} value={bytesRead} />
            </div>
        </>
    )
}

export default QueryStats;
