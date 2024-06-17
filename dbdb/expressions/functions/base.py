from dbdb.tuples.context import ExecutionContext

import enum
from collections import defaultdict


AGG_FUNCTION_INCOMPLETE = object()


class FunctionTypes(enum.Enum):
    SCALAR = enum.auto()
    AGGREGATE = enum.auto()
    TABLE = enum.auto()
    WINDOW = enum.auto()


class BaseFunction:
    NAMES = []
    TYPE = None

    def get_name(self):
        raise NotImplementedError()

    def eval(self, context: ExecutionContext):
        raise NotImplementedError()


class ScalarFunction(BaseFunction):
    NAMES = []
    TYPE = FunctionTypes.SCALAR

    def __init__(self, expr):
        self.expr = expr


class AggregateFunction:
    NAMES = []
    TYPE = FunctionTypes.AGGREGATE

    def __init__(self, expr, modifiers=None):
        self.accum = None
        self.expr = expr
        self.modifiers = modifiers or dict()

    def start(self):
        self.accum = None


class WindowFunction:
    NAMES = []
    TYPE = FunctionTypes.WINDOW

    def __init__(self, expr, partition_cols, order_cols, frame_start, frame_end):
        self.expr = expr
        self.partition_cols = partition_cols
        self.order_cols = order_cols
        self.frame_start = frame_start
        self.frame_end = frame_end

        self._partitions = None

    def get_partition(self, key):
        if self._partitions is None:
            raise RuntimeError("Window partitions are not initialized")

        return self._partitions.get(key, [])

    def get_frame_range(self, index, frame_size):
        start_index = None
        end_index = None

        if not self.frame_start or not self.frame_end:
            return 0, frame_size - 1

        if self.frame_start[0] == "UNBOUNDED":
            start_index = 0
        elif self.frame_start[0] == "CURRENT":
            start_index = index
        else:
            start_index = max(0, index - self.frame_start[0])

        if self.frame_end[0] == "UNBOUNDED":
            end_index = frame_size - 1
        elif self.frame_end[0] == "CURRENT":
            end_index = index
        else:
            end_index = min(frame_size - 1, index + self.frame_end[0])

        if start_index is None or end_index is None:
            raise RuntimeError("Invalid range found for window function")

        return start_index, end_index

    def make_sorted_partitions(self, rows):
        partitions = defaultdict(list)

        for row in rows:
            key = tuple([row.field(c) for c in self.partition_cols])
            partitions[key].append(row)

        if not self.order_cols:
            return partitions

        # sort partitions if needed
        sorted_partitions = {}
        for key, rows in partitions.items():
            sorted_partitions[key] = sorted(rows, key=self.order_cols.as_comparator)

        return sorted_partitions

    def partition_key(self, row):
        return tuple([row.field(c) for c in self.partition_cols])

    def _eval(self, context: ExecutionContext):
        raise NotImplementedError()

    def eval(self, context: ExecutionContext):
        if self._partitions is None and context.rows:
            self._partitions = self.make_sorted_partitions(context.rows)
        elif not context.rows:
            raise RuntimeError(
                "Rows were not materialized before window function execution"
            )

        key = self.partition_key(context.row)
        row_id = id(context.row)

        partition_rows = self.get_partition(key)
        rows_by_id = [id(row) for row in partition_rows]
        index = rows_by_id.index(row_id)

        start_idx, end_idx = self.get_frame_range(index, len(partition_rows))
        frame_rows = partition_rows[start_idx : end_idx + 1]

        context = ExecutionContext(row=context.row, rows=frame_rows, row_index=index)

        return self._eval(context)


class TableFunction:
    NAMES = []
    TYPE = FunctionTypes.TABLE

    def generate(self):
        raise NotImplementedError()

    def details(self):
        return {}
