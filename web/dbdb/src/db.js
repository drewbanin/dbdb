import React, {useState, useContext} from 'react';
import Button from 'react-bootstrap/Button';

import './App.css';
import 'reactflow/dist/style.css';

import QueryComponent from './components/queryComponent.js';
import OperatorViz from './components/operatorViz.js';
import { Visualizer, XYViz, PlaceholderViz } from './components/visualizer.tsx';
import ResultTable from './components/resultTable.js';

import { QueryContext } from './Store.js';

function Database() {
    const [activeTab, setActiveTab] = useState('table');

    const { schema, result, running, fullscreen, isMobileSized } = useContext(QueryContext);
    const [ dataSchema ] = schema;
    const [ resultData ] = result;
    const [ queryRunning ] = running;

    const [ isFullscreen ] = fullscreen;

    const fields = dataSchema || [];

    const isMusic = fields.indexOf('time') >= 0 && fields.indexOf('freq') >= 0;
    const isXY = fields.filter(f => f.endsWith('x')).length > 0;

    function formatRowCount(count) {
        if (!count) {
            return ""
        }

        return count.toLocaleString() + " ROWS"
    }

    const rowCountString = formatRowCount(resultData.length);

    return (
        <div className="flexColumn">
            <div className="flexColumnBox">
                <div className="flexRow">
                    <div className="flexRowBox" style={{ flexGrow: 1 }}>
                        <div className="boxWrapper">
                            <QueryComponent />
                        </div>
                    </div>
                    { !isMobileSized && (<div className="statsPanel" style={{ marginLeft: 40, flexGrow: 0, minWidth: 500, maxWith: 500, width: 500}}>
                        <div className="boxWrapper">
                            {isFullscreen && <div className="flexColumn regularViz">
                                <PlaceholderViz text="RUNNING IN FULL SCREEN" />
                            </div>}
                            <div className={"flexColumn " + (isFullscreen ? 'fullScreenViz' : "regularViz")}>
                                {isMusic && <Visualizer />}
                                {isXY && <XYViz />}
                                {(!isXY && !isMusic) &&  <PlaceholderViz />}
                            </div>
                        </div>
                    </div>)}
                </div>
            </div>
            <div className="flexColumnBox" style={{ marginTop: 90 }}>
                <div className="flexRow">
                    <div className="flexRowBox boxWrapper" style={{ flexGrow: 1 }}>
                        <div className="tabPicker light">
                            <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%' }}>
                                <div>
                                    <Button className={activeTab === "table" ? "selected" : ""} onClick={ e => setActiveTab("table") }>RESULTS</Button>
                                    <Button className={activeTab === "plan" ? "selected" : ""}onClick={ e => setActiveTab("plan") }>QUERY PLAN</Button>
                                </div>
                                {(queryRunning || resultData.length > 0) && <div style={{ textAlign: 'right', position: 'relative', bottom: -20, fontSize: 12 }}>
                                    <span>{rowCountString}</span>
                                </div>}
                            </div>
                        </div>
                        <div style={{ borderTop: '1px solid black' }}>
                            { activeTab === "plan" && <OperatorViz />}
                            { activeTab === "table" && <ResultTable />}
                        </div>
                    </div>
                </div>
            </div>
        </div>
  );
}

export default Database;
