import { React, useState, createContext } from "react";

const DEFAULT_QUERY =`
/*
 *  Hi! I'm Drew, and you're looking at a database I built called dbdb.
 *
 *  Click "EXECUTE" below to check out some of my other projects. And,
 *  if you're feeling adventurous, try running an example query from
 *  the list above. Happy querying :)
 *
 *  -Drew
 */

select * from google_sheet('1yYnpJEv1IvndVvRJ2crLc68TeF0TB_4M7LX5Ys62BHQ')

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
