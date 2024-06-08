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
            borderBottom: '1px solid black',
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

    /*
     * instead of scrolling into view as a row is returned, instead, scroll
     * into view as a note is played by the music player. Active rows should be
     * highlighted in a default color, or the color can be set by returning
     * a value called _color (or w/e) from the SQL query
     */

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
            name: col.toUpperCase(),
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
            fixedHeader
            fixedHeaderScrollHeight={'500px'}

        />
    )
}

export default ResultTable;
