import React, {useState} from 'react';
import Button from 'react-bootstrap/Button';

import './App.css';
import 'reactflow/dist/style.css';

import QueryComponent from './components/queryComponent.js';
import OperatorViz from './components/operatorViz.js';
import Visualizer from './components/visualizer.tsx';
import ResultTable from './components/resultTable.js';

import { QueryContextProvider } from './Store.js';


function App() {
    const [activeTab, setActiveTab] = useState('plan');

    return (
        <div className="App">
            <QueryContextProvider>
                <div className="flexColumn">
                    <div className="flexColumnBox">
                        <div className="flexRow">
                            <div className="flexRowBox" style={{ flexGrow: 1, marginRight: 40 }}>
                                <div className="boxWrapper">
                                    <QueryComponent />
                                </div>
                            </div>
                            <div className="statsPanel" style={{ flexGrow: 0, minWidth: 500 }}>
                                <div className="boxWrapper">
                                    <div className="flexColumn">
                                        <Visualizer />
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
            </QueryContextProvider>
        </div>
  );
}

export default App;
