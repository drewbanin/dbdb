import { React, useState, createContext } from "react";

const DEFAULT_QUERY =`
select
  i / 10 as time,
  1 as length,
  440 as freq
from generate_series(10)
`.trim();


const QueryContext = createContext();

const QueryContextProvider = ({ children }) => {
    const [query, setQuery] = useState(DEFAULT_QUERY);
    const [result, setResult] = useState([]);
    const [nodes, setNodes] = useState(null);
    const [error, setError] = useState(null);
    const [schema, setSchema] = useState(null);

    return (
        <QueryContext.Provider value={{
            query: [query, setQuery],
            schema: [schema, setSchema],
            result: [result, setResult],
            nodes: [nodes, setNodes],
            error: [error, setError],
        }}>
            {children}
        </QueryContext.Provider>
    );
};

export { QueryContextProvider, QueryContext };
