import { React, useState, createContext } from "react";

const DEFAULT_QUERY =`select
  my_table.my_string as id,
  sum(1) as total

from my_table
group by 1
order by 1
`

 
const QueryContext = createContext();
 
const QueryContextProvider = ({ children }) => {
    const [query, setQuery] = useState(DEFAULT_QUERY);
    const [result, setResult] = useState(null);
    const [nodes, setNodes] = useState(null);
 
    return (
        <QueryContext.Provider value={{
            query: [query, setQuery],
            result: [result, setResult],
            nodes: [nodes, setNodes],
        }}>
            {children}
        </QueryContext.Provider>
    );
};
 
export { QueryContextProvider, QueryContext };
