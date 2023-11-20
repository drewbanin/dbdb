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

    const formatRow = (row) => {
        Object.keys(row).forEach((key) => {
            const value = row[key];

            if (value === true) {
                row[key] = "TRUE";
            } else if (value === false) {
                row[key] = "FALSE";
            } else if (value === null) {
                row[key] = "NULL"
            }
        });

        return row;
    }

    const columns = resultData.columns.map( (col) => {
        return {
            name: col,
            selector: row => row[col],
        }
    })

    const rows = resultData.rows.forEach((row) => {
        // this mutates
        formatRow(row);
    })


    return (
        <DataTable columns={columns} data={resultData.rows} customStyles={customStyles}/>
    )
}

export default ResultTable;
