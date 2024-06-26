import { useContext } from 'react';
import { Handle, Position } from 'reactflow';

import { QueryContext } from '../Store.js';

import { formatNumber } from '../Helpers.js';

export const OperatorNode = ({ data }) => {
  const { nodeStats } = useContext(QueryContext);
  const [ nodeStatData ] = nodeStats;

  const statData = nodeStatData[data.id] || {};

  // const operatorType = statData.operator_type;
  // const isTableScan = operatorType === "Table Scan";

  const state = statData.state || 'pending';
  // const rows_processed = formatNumber(statData.rows_processed, 0);
  const rows_emitted = formatNumber(statData.rows_emitted, 0);
  const elapsed = formatNumber(statData.elapsed_time);

  const table_name = (data.details || {}).qualified_table_name;
  const tab_name = (data.details || {}).sheet_tab_id;

  // const getCustomStat = (statData, statName, formatFunc) => {
  //     if (!statData || !statData.custom) {
  //         return null;
  //     }

  //     const value = statData.custom[statName];
  //     if (formatFunc) {
  //         return formatFunc(value);
  //     } else {
  //       return value;
  //     }
  // }

  // const bytesRead = getCustomStat(statData, 'bytes_read', formatBytes);
  // const bytesTotal = getCustomStat(statData, 'bytes_total', formatBytes);
  // const pagesRead = getCustomStat(statData, 'reads');

  return (
    <>
      <Handle type="target" position={Position.Left} />
      <div>
        <div className="title operator-title">
            {data.label}
        </div>
        <div className="operator-stats">
            <ul>
                {!!table_name && (
                    <li>NAME: {table_name}</li>
                )}
                {!!tab_name && (
                    <li>NAME: {tab_name}</li>
                )}
                <li>PROG: {state.toUpperCase()}</li>
                <li>ROWS: {rows_emitted}</li>
                <li>TIME: {elapsed || 0}</li>

            </ul>
        </div>
      </div>
      <Handle type="source" position={Position.Right} />
    </>
  );
}
