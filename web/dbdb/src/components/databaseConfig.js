import React from 'react';

import {LineDetail} from "./LineDetail.js";

function DatabaseConfig() {
    return (
        <>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">
                        <span className="light title">DATABASE CONFIG</span>
                    </div>
                </div>
            </div>
            <div className="configBox">
                <LineDetail label={"ENCODE COLUMNS"} value={"YES"} />
                <LineDetail label={"PRUNE PAGES"} value={"NO"} />
                <LineDetail label={"I/O SPEED"} value={"100 kbps"} />
            </div>
        </>
    )
}

export default DatabaseConfig;
