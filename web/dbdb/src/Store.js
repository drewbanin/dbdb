import { React, useState, createContext } from "react";

const DEFAULT_QUERY =`select
  user_id as user_id,
  first_name as first_name,
  last_name as last_name
from people
limit 10
`

 
const QueryContext = createContext();
 
const QueryContextProvider = ({ children }) => {
    const [query, setQuery] = useState(DEFAULT_QUERY);
    const [result, setResult] = useState(null);
    const [nodes, setNodes] = useState(null);
    const [error, setError] = useState(null);
 
    return (
        <QueryContext.Provider value={{
            query: [query, setQuery],
            result: [result, setResult],
            nodes: [nodes, setNodes],
            error: [error, setError],
        }}>
            {children}
        </QueryContext.Provider>
    );
};
 
export { QueryContextProvider, QueryContext };
