import React, {useState, useContext} from 'react';
import Button from 'react-bootstrap/Button';

import './App.css';
import 'reactflow/dist/style.css';

import QueryComponent from './components/queryComponent.js';
import OperatorViz from './components/operatorViz.js';
import { Visualizer, XYViz, PlaceholderViz } from './components/visualizer.tsx';
import ResultTable from './components/resultTable.js';

import { QueryContext, QueryContextProvider } from './Store.js';

function Database() {
    const [activeTab, setActiveTab] = useState('plan');

    const { schema } = useContext(QueryContext);
    const [ dataSchema, setSchema ] = schema;

    const fields = dataSchema || [];

    const isMusic = fields.indexOf('time') >= 0 && fields.indexOf('freq') >= 0;
    const isXY = fields.filter(f => f.endsWith('_x')).length > 0;

    return (
        <div className="flexColumn">
            <div className="flexColumnBox">
                <div className="flexRow">
                    <div className="flexRowBox" style={{ flexGrow: 1 }}>
                        <div className="boxWrapper">
                            <QueryComponent />
                        </div>
                    </div>
                    <div className="statsPanel" style={{ marginLeft: 40, flexGrow: 0, minWidth: 500, maxWith: 500, width: 500}}>
                        <div className="boxWrapper">
                            <div className="flexColumn">
                                {isMusic && <Visualizer />}
                                {isXY && <XYViz />}
                                {(!isXY && !isMusic) &&  <PlaceholderViz />}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div className="flexColumnBox" style={{ marginTop: 90 }}>
                <div className="flexRow">
                    <div className="flexRowBox boxWrapper" style={{ flexGrow: 1 }}>
                        <div className="tabPicker light">
                            <Button className={activeTab === "plan" ? "selected" : ""}onClick={ e => setActiveTab("plan") }>QUERY PLAN</Button>
                            <Button className={activeTab === "table" ? "selected" : ""} onClick={ e => setActiveTab("table") }>RESULTS</Button>
                        </div>
                        { activeTab === "plan" && <OperatorViz />}
                        { activeTab === "table" && <ResultTable />}
                    </div>
                </div>
            </div>
        </div>
  );
}

export default Database;
