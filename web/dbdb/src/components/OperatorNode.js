import { useState } from 'react';
import { Handle, Position } from 'reactflow';

import { useSub } from '../Hooks.js';
import Spinner from '../spinner.gif';

import { formatBytes, formatNumber } from '../Helpers.js';
 
export const OperatorNode = ({ data }) => {
  const [ statData, setStatData ] = useState({});

  useSub('OperatorStats', (stats) => {
      if (stats.operator_id === data.id) {
          setStatData(stats);
      }
  });

  const operatorType = statData.operator_type;
  const isTableScan = operatorType === "Table Scan";


  const state = statData.state || 'pending';
  const rows_processed = formatNumber(statData.rows_processed, 0);
  const rows_emitted = formatNumber(statData.rows_emitted, 0);

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
  const fileRef = getCustomStat(statData, 'file_ref');

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
                <li>STATE: {state.toUpperCase()}</li>
                <li>PROCESSED: {rows_processed}</li>
                <li>EMITTED: {rows_emitted}</li>

                {isTableScan && (
                    <>
                        <li>&nbsp;</li>
                        <li>BYTES READ: {bytesRead}</li>
                        <li>BYTES TOTAL: {bytesTotal}</li>
                        <li>PAGES READ: {pagesRead}</li>
                        <li>FILE: {fileRef}</li>
                    </>
                )}
            </ul>
        </div>
      </div>
      <Handle type="source" position={Position.Right} />
    </>
  );
}
