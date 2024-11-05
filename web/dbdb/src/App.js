import { React, useState, useEffect } from 'react';

import './App.css';
import 'reactflow/dist/style.css';

import { QueryContextProvider } from './Store.js';
import Database from './db.js';

const mediaQuery = "(min-width: 900px)";

function App() {
    const [ matches, setMatches ] = useState(
        window.matchMedia(mediaQuery).matches
    );

     useEffect(() => {
        window
        .matchMedia(mediaQuery)
        .addEventListener('change', e => setMatches( e.matches ));
      }, []);

    if (matches) {
        return (
            <div className="App">
                <QueryContextProvider>
                    <Database />
                </QueryContextProvider>
            </div>
        )
    } else {
        return <div className="App">
            <h1>hi i'm drew</h1>
            <p>i work at dbt Labs</p>
            <p>i live in philadelphia</p>
            <p>try coming back on your laptop, ok?</p>
        </div>
    }
}

export default App;
