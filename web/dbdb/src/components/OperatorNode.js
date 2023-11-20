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

  const bytesRead = isTableScan && formatBytes(statData.custom.bytes_read);
  const bytesTotal = isTableScan && formatBytes(statData.custom.bytes_total);
  const pagesRead = isTableScan && statData.custom.reads;

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
                <li>State: {state}</li>
                <li>Rows Processed: {rows_processed}</li>
                <li>Rows Emitted: {rows_emitted}</li>

                {isTableScan && (
                    <>
                        <li>&nbsp;</li>
                        <li>Bytes read: {bytesRead}</li>
                        <li>Bytes total: {bytesTotal}</li>
                        <li>Pages read: {pagesRead}</li>
                    </>
                )}
            </ul>
        </div>
      </div>
      <Handle type="source" position={Position.Right} />
    </>
  );
}
