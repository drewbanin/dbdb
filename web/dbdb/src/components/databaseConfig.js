import React from 'react';

function DatabaseConfig() {
    return (
        <>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">
                        <span className="light title">DATABASE</span>
                    </div>
                </div>
            </div>
            <div className="configBox">
                <div>
                    <span className="light">Enable page pruning</span>
                    <span className="light">.........</span>
                    <span className="heavy">NO</span>
                </div>
                <div>
                    <span className="light">Set I/O speed</span>
                    <span className="light">.........</span>
                    <span className="heavy">100kbps</span>
                </div>
                <div>
                    <span className="light">Enable column encodings</span>
                    <span className="light">.........</span>
                    <span className="heavy">NO</span>
                </div>
            </div>
        </>
    )
}

export default DatabaseConfig;
