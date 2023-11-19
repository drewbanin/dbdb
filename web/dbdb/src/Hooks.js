import React, { useContext, useState, useEffect } from 'react';
import { postRequest } from './Client.js';
import { QueryContext } from './Store.js';

const usePollForQuery = (queryText) => {
    const { query, result, nodes } = useContext(QueryContext);

    const [ intervalId, setIntervalId ] = useState(null);
    const [ response, setResponse ] = useState(null);

    postRequest("query", {sql: queryText}, (resp) => {
        console.log("Setting node data", resp);
        //setNodeData(resp);
    });


    useEffect(() => {
      const isComplete = false;

      const interval = setInterval(() => {
          debugger

            postRequest("query-results", {queryId: response.id}, (resp) => {
                console.log("GOT RESULTS", resp);
                clearInterval(intervalId);
            });

        console.log('This will run every second!');
      }, 2000);

      setIntervalId(interval)



      return () => clearInterval(interval);

    }, []);

    return [false, null];
}

export { usePollForQuery,  };
