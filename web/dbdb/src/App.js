import React from 'react';

import './App.css';
import 'reactflow/dist/style.css';

import { QueryContextProvider } from './Store.js';
import Database from './db.js';

function App() {

    return (
        <div className="App">
            <QueryContextProvider>
                <Database />
            </QueryContextProvider>
        </div>
  );
}

export default App;
