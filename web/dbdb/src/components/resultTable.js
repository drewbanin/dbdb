import React, { useContext } from 'react';
import DataTable from 'react-data-table-component';

import { QueryContext } from '../Store.js';

const customStyles = {
    rows: {
        style: {
            minHeight: '32px',
            fontWeight: 300,

        }
    },
    headRow: {
        style: {
            minHeight: '32px',
            fontWeight: 700,
            fontSize: '14px',
        },
    },
}

function ResultTable() {
    const { result } = useContext(QueryContext);
    const [ resultData ] = result;

    if (!resultData)
        return (<div style={{ marginTop: 10 }}>NO DATA</div>)

    const columns = resultData.columns.map( (col) => {
        return {
            name: col,
            selector: row => row[col]
        }
    })

    return (
        <DataTable columns={columns} data={resultData.rows} customStyles={customStyles}/>
    )
}

export default ResultTable;
