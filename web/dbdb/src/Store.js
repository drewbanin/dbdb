import { React, useState, createContext } from "react";

const DEFAULT_QUERY =`select
  my_table.my_string as my_string,
  sum(my_table.is_odd + 10) as my_avg

from my_table
inner join my_table as debug on debug.my_string = my_table.my_string
where debug.is_odd = true
  and debug.is_odd is not false
group by 1
order by 1
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
