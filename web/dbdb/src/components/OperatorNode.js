import { useState, useContext } from 'react';
import { Handle, Position } from 'reactflow';

import { QueryContext } from '../Store.js';
import { useSub } from '../Hooks.js';
import Spinner from '../spinner.gif';

import { formatBytes, formatNumber } from '../Helpers.js';

export const OperatorNode = ({ data }) => {
  const { nodeStats } = useContext(QueryContext);
  const [ nodeStatData, setNodeStatData ] = nodeStats;

  const statData = nodeStatData[data.id] || {};

  const operatorType = statData.operator_type;
  const isTableScan = operatorType === "Table Scan";


  const state = statData.state || 'pending';
  const rows_processed = formatNumber(statData.rows_processed, 0);
  const rows_emitted = formatNumber(statData.rows_emitted, 0);
  const elapsed = formatNumber(statData.elapsed_time);
  const table_name = (data.details || {}).qualified_table_name;

  const getCustomStat = (statData, statName, formatFunc) => {
      if (!statData || !statData.custom) {
          return null;
      }

      const value = statData.custom[statName];
      if (formatFunc) {
          return formatFunc(value);
      } else {
        return value;
      }
  }

  const bytesRead = getCustomStat(statData, 'bytes_read', formatBytes);
  const bytesTotal = getCustomStat(statData, 'bytes_total', formatBytes);
  const pagesRead = getCustomStat(statData, 'reads');

  return (
    <>
      <Handle type="target" position={Position.Left} />
      <div>
        <div className="title operator-title">
        {(state === "running") && <span>
            <img className="queryLoading" src={Spinner} />
        </span>}
            {data.label}
        </div>
        <div className="operator-stats">
            <ul>
                {!!table_name && (
                    <li>FILE: {table_name}</li>
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
