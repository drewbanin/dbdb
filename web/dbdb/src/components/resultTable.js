import React, { useContext, useMemo } from 'react';
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

    const columns = useMemo(() => {
        return (resultSchema || []).map((col, i) => {
            return {
                name: col.toUpperCase(),
                selector: row => row[i],
            }
        });

    }, [resultSchema]);

    // const displayRows = resultData.slice(0, 1000);
    const rows = useMemo(() => {
        return (resultData || []).map((row) => {
            // this mutates
            return formatRow(row);
        })
    }, [resultData]);


    // paginationRowsPerPageOptions={[15, 50, 100]}
    // paginationPerPage={50}
    return (
        <DataTable
            columns={columns}
            data={rows}
            customStyles={customStyles}
            fixedHeader
            fixedHeaderScrollHeight={'500px'}
            pagination
        />
    )
}

export default ResultTable;
