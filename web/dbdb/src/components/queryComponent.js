import React, { useContext, useState, useCallback, useEffect } from 'react';
import Button from 'react-bootstrap/Button';

import { postRequest } from '../Client.js';
import { QueryContext } from '../Store.js';
import { usePub } from '../Hooks.js';

import Editor, { useMonaco, loader } from '@monaco-editor/react';
import CodeTheme from './theme.json';

import Spinner from '../spinner.gif';


function QueryComponent() {

    const { query, result, nodes, error, schema } = useContext(QueryContext);

    const [ queryText, setQueryText ] = query;
    const [ rows, setRows ] = result;
    const [ rowSchema, setSchema ] = schema;
    const [ nodeData, setNodeData ] = nodes;
    const [ errorData, setError ] = error;

    const [ queryRunning, setQueryRunning ] = useState(false);

    loader.init().then((monaco) => {
        monaco.editor.defineTheme('dbdb', CodeTheme);
    });

    const runQuery = () => {
        if (queryRunning) return;

        setError(null);
        setNodeData(null);
        setRows([]);

        setQueryRunning(true);
        postRequest("query", {sql: queryText}, (res) => {
            if (res.detail) {
                setQueryRunning(false)
                setError({error: res.detail});
                setNodeData(null)
            } else {
                setNodeData(res);
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
                setNodeData(res);
                setQueryRunning(false);
            }
        })
    };

    const cancelQuery = () => {
        if (!queryRunning) return;

    };

    const publish = usePub();

    useEffect(() => {
      const sse = new EventSource('http://localhost:8000/stream', { withCredentials: false });
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
            publish("QUERY_COMPLETE", data.id);
            setQueryRunning(false);
        } else if (event === "QueryError") {
            // setResult(null);
            setQueryRunning(false);
            setError(data);
        } else {
            publish(event, data, payload);
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
                        { !!rows.length && <span style={{marginLeft: 10, fontSize: 12}}>{rows.length} rows</span>}
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
                {!!queryRunning && <Button
                    style={{ backgroundColor: '#ff7575', float: 'right', marginRight: 0 }}
                    onClick={ cancelQuery }>CANCEL</Button>}
                {errorData && <div className="queryError">
                    <strong>Query Error:</strong> {errorData.error}
                </div>}
            </div>
        </>


        /*
         *
                <MonacoEditor
                  value={queryText}
                  defaultValue={queryText}
                  language="sql"
                />
         *
                  padding={15}
                  style={{
                    fontSize: 14,
                    backgroundColor: "white",
                    fontFamily: 'MonaspaceNeon',
                    height: '100%',
                  }}
        */
    )
}

export default QueryComponent;
