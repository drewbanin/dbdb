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
    const { result, schema } = useContext(QueryContext);
    const [ resultData ] = result;
    const [ resultSchema ] = schema;

    if (!resultData || !resultSchema) {
        return (<div style={{ marginTop: 10 }}>NO DATA</div>)
    }

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

    const columns = resultSchema.map( (col, i) => {
        return {
            name: col,
            selector: row => row[i],
        }
    })

    // const displayRows = resultData.slice(0, 1000);
    const rows = resultData.map((row) => {
        // this mutates
        return formatRow(row);
    })

    return (
        <DataTable
            columns={columns}
            data={rows}
            customStyles={customStyles}
            pagination
        />
    )
}

export default ResultTable;
