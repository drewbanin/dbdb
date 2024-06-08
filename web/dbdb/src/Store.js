import { React, useState, createContext } from "react";

const DEFAULT_QUERY =`
`.trim();


const QueryContext = createContext();

const QueryContextProvider = ({ children }) => {
    const [query, setQuery] = useState(DEFAULT_QUERY);
    const [result, setResult] = useState([]);
    const [nodes, setNodes] = useState(null);
    const [error, setError] = useState(null);
    const [schema, setSchema] = useState(null);
    const [nodeStats, setNodeStats] = useState({});
    const [volume, setVolume] = useState(1);
    const [queryRunning, setQueryRunning] = useState(false);

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
        }}>
            {children}
        </QueryContext.Provider>
    );
};

export { QueryContextProvider, QueryContext };
