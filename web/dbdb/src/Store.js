import { React, useState, createContext } from "react";

import QUERIES  from './queries.js';


const QueryContext = createContext();

const QueryContextProvider = ({ children }) => {
    const [query, setQuery] = useState(QUERIES[0].value);
    const [result, setResult] = useState([]);
    const [nodes, setNodes] = useState(null);
    const [error, setError] = useState(null);
    const [schema, setSchema] = useState(null);
    const [nodeStats, setNodeStats] = useState({});
    const [volume, setVolume] = useState(1);
    const [queryRunning, setQueryRunning] = useState(false);
    const [isFullscreen, setFullscreen] = useState(false);

    return (
        <QueryContext.Provider value={{
            query: [query, setQuery],
            schema: [schema, setSchema],
            result: [result, setResult],
            nodes: [nodes, setNodes],
            error: [error, setError],
            nodeStats: [nodeStats, setNodeStats],
            volume: [volume, setVolume],
            running: [queryRunning, setQueryRunning],
            fullscreen: [isFullscreen, setFullscreen],
        }}>
            {children}
        </QueryContext.Provider>
    );
};

export { QueryContextProvider, QueryContext };
