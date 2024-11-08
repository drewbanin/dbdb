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
        const renderedColumns = (resultSchema || []).map((col, i) => {
            const linkField = (col + ":link");
            const linkFieldIndex = resultSchema.findIndex(r => r === linkField);

            if (col.endsWith(':link')) {
                return null;
            }

            return {
                name: col.toUpperCase(),
                selector: row => row[i],
                cell: (d) => {
                    if (linkFieldIndex >= 0) {
                      return (<a href={d[linkFieldIndex]}>
                        {d[i]}
                      </a>)
                    } else {
                        return d[i];
                    }
                },
            }
        });

        return renderedColumns.filter(r => r !== null);

    }, [resultSchema]);

    // const displayRows = resultData.slice(0, 1000);
    const rows = useMemo(() => {
        return (resultData || []).map((row) => {
            // this mutates
            return formatRow(row);
        })
    }, [resultData]);


    return (
        <DataTable
            columns={columns}
            data={rows}
            customStyles={customStyles}
            fixedHeader
            fixedHeaderScrollHeight={'515px'}
            paginationRowsPerPageOptions={[15, 50, 100]}
            paginationPerPage={15}
            pagination
        />
    )
}

export default ResultTable;
