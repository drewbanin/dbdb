from dataclasses import dataclass
from typing import Optional

from dbdb.tuples.rows import Rows, RowTuple


@dataclass
class ExecutionContext:
    row: RowTuple

    row_index: Optional[int] = None
    rows: Optional[Rows] = None
