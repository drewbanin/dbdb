import React, { useContext, useState, useCallback, useEffect } from 'react';
import Button from 'react-bootstrap/Button';

import { postRequest } from '../Client.js';
import { QueryContext } from '../Store.js';
import { usePollForQuery } from '../Hooks.js';

import CodeEditor from '@uiw/react-textarea-code-editor';
import Spinner from '../spinner.gif';

function QueryComponent() {

    const { query, result, nodes } = useContext(QueryContext);


    const [ queryText, setQueryText ] = query;
    const [ _, setResult ] = result;
    const [ nodeData, setNodeData ] = nodes;

    const [ queryRunning, setQueryRunning ] = useState(false);
    const runQuery = useCallback(async () => {
        if (queryRunning) return;

        setQueryRunning(true);
        postRequest("query", {sql: queryText}, (res) => {
            setNodeData(res);
        })
    });

    useEffect(() => {
      const sse = new EventSource('http://localhost:8000/stream', { withCredentials: false });
      sse.onmessage = e => {
        const payload = JSON.parse(e.data);

        const event = payload.event;
        const data = payload.data;

        if (event === "QueryComplete") {
            setResult(data);
            setQueryRunning(false);
        } else if (event === "OperatorStats") {
            console.log(data);
        }
      }
      sse.onerror = (e) => {
        // error log here 
        console.log("ERROR:", e);
        sse.close();
      }
      return () => {
        sse.close();
      };
    }, []);

    return (
        <>
            <div className="panelHeader">
                <div style={{ padding: 5 }}>
                    <div className="helpText">
                        {queryRunning && <span>
                            <img className="queryLoading" src={Spinner} />
                        </span>}
                        <span className="light title">QUERY</span>
                    </div>
                </div>
            </div>
            <CodeEditor
              value={queryText}
              language="sql"
              onChange={(e) => setQueryText(e.target.value)}
              padding={15}
              data-color-mode="light"
              style={{
                fontSize: 14,
                backgroundColor: "white",
                fontFamily: 'MonaspaceNeon',
                border: '1px solid black',
              }}
            />
            <Button disabled={queryRunning} onClick={ runQuery } className="primaryButton">EXECUTE</Button>
            <Button disabled={queryRunning}>EXPLAIN</Button>
        </>
    )
}

export default QueryComponent;
