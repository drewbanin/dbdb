import React, { useContext, useState, useEffect, useRef } from 'react';
import Button from 'react-bootstrap/Button';

import { postRequest } from '../Client.js';
import { QueryContext } from '../Store.js';
import { usePub } from '../Hooks.js';
import { cleanDag } from '../DAG.js';
import { makeRoute } from '../routes.js';

import Editor, { loader } from '@monaco-editor/react';
import CodeTheme from './theme.json';

import Dropdown from 'react-dropdown';
import 'react-dropdown/style.css';

import QUERIES from '../queries.js';


function QueryComponent() {

    const { query, result, nodes, error, schema, nodeStats, running } = useContext(QueryContext);

    const [ queryText, setQueryText ] = query;
    const [ , setRows ] = result;
    const [ nodeData, setNodeData ] = nodes;
    const [ , setSchema ] = schema;
    const [ , setNodeStatData ] = nodeStats;
    const [ queryRunning, setQueryRunning ] = running;

    const [ errorData, setError ] = error;

    const [ queryStatus, setQueryStatus ] = useState(null);

    loader.init().then((monaco) => {
        monaco.editor.defineTheme('dbdb', CodeTheme);
    });

    const runQuery = () => {
        if (queryRunning) return;

        setError(null);
        setQueryStatus(null);
        setNodeData(null);
        setRows([]);

        setQueryRunning(true);
        postRequest("query", {sql: queryText}, (res) => {
            if (res.detail) {
                setQueryRunning(false)
                setError({error: res.detail});
                setNodeData(null)
            } else {
                const cleaned = cleanDag(res);
                setNodeData(cleaned);
            }
        })
    };

    const explainQuery = () => {
        if (queryRunning) return;

        setError(null);
        setNodeData(null);
        setRows([]);

        setQueryRunning(true);
        postRequest("explain", {sql: queryText}, (res) => {
            if (res.detail) {
                setError({error: res.detail});
                setQueryRunning(false)
                setNodeData(null)
            } else {
                const cleaned = cleanDag(res);
                setNodeData(cleaned);
                setQueryRunning(false);
            }
        })
    };

    const publish = useRef(usePub());

    useEffect(() => {
      if (!nodeData || !nodeData.query_id) {
          console.log("No query id set - not streaming", nodeData)
          return;
      }

      const queryId = nodeData.query_id;
      const machineId = nodeData.machine_id;
      console.log("Creating SSE stream for query:", queryId, "and machine:", machineId)
      const routePrefix = makeRoute('stream');
      const route = routePrefix + "?query_id=" + queryId + "&machine_id=" + machineId;

      const sse = new EventSource(route, { withCredentials: false });
      sse.onmessage = e => {
        const payload = JSON.parse(e.data);

        const event = payload.event;
        const data = payload.data;

        if (event === "QueryStart") {
            setRows((rows) => {
                return []
            });
        } else if (event === "ResultRows") {
            setRows((rows) => {
                return [...rows, ...payload.data.rows]
            });
        } else if (event === "ResultSchema") {
            setSchema(payload.data.columns)
        } else if (event === "QueryComplete") {
            publish.current("QUERY_COMPLETE", data.id);
            setQueryRunning(false);
        } else if (event === "QueryMutationStatus") {
             setQueryStatus(data.status);
        } else if (event === "QueryError") {
            // setResult(null);
            setQueryRunning(false);
            setError(data);
        } else if (event === "OperatorStats") {
            const operatorId = data.operator_id;
            setNodeStatData(d => {
                const newData = {...d}
                newData[operatorId] = data;
                return newData
            })
        } else {
            publish.current(event, data, payload);
        }
      }
      sse.onerror = (e, data) => {
        // error log here
        console.log("closing SSE")
        sse.close();
      }
      return () => {
        console.log("closing SSE")
        sse.close();
      };
    }, [nodeData, publish, setError, setNodeStatData, setRows, setSchema, setQueryRunning]);

    const queryOptions = QUERIES;

    const setFromExample = (opt) => {
        setQueryText(opt.value);
    }

    return (
        <>
            <div className="panelHeader">
                <div style={{ padding: 5, height: '24px' }}>
                    <div className="helpText">
                        <div style={{ display: 'inline-block', marginTop: 5 }}>
                            <span className="light title">QUERY</span>
                        </div>

                        <div style={{ display: 'inline-block', float: 'right'  }}>
                            <Dropdown
                                className="styled-dropdown"
                                controlClassName="styled-dropdown-control"
                                placeholder="EXAMPLES"
                                onChange={setFromExample}
                                options={queryOptions} />
                        </div>

                        <div style={{ clear: 'left' }}></div>
                    </div>
                </div>
            </div>
            <div className="fixedHeight" style={{ border: '1px solid black' }}>
                <Editor
                    height="100%"
                    defaultLanguage="sql"
                    defaultValue={queryText}
                    value={queryText}
                    theme='dbdb'
                    onChange={setQueryText}
                    options={{
                        scrollbar: {
                            horizontal: 'hidden',
                            vertical: 'hidden',
                        },
                        minimap: {
                            enabled: false
                        },
                        padding: {
                            top: 5,
                            bottom: 5,
                        },
                        overviewRulerBorder: false,
                        overviewRulerLanes: 0,
                        renderLineHighlight: 'none',
                        fontFamily: 'MonaspaceNeon',
                        fontSize: 14,
                        lineNumbersMinChars: 3,
                        renderIndentGuides: false,
                        scrollBeyondLastLine: false,
                    }}
                />
                <Button disabled={queryRunning} onClick={ runQuery } className="primaryButton">EXECUTE</Button>
                <Button disabled={queryRunning} onClick={ explainQuery }>EXPLAIN</Button>
                {errorData && <div className="queryError">
                    <strong>Query Error:</strong> {errorData.error}
                </div>}
                {!errorData && queryStatus && <div className="queryStatus">
                    <strong>Status:</strong> {queryStatus}
                </div>}
            </div>
        </>
    )
}

export default QueryComponent;
