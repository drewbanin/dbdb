import React from 'react';

function QueryStats() {
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
                <div>
                    <span className="light">Rows / sec</span>
                    <span className="light">.........</span>
                    <span className="heavy">487</span>
                </div>
            </div>
        </>
    )
}

export default QueryStats;
