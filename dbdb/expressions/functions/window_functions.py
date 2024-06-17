from dbdb.expressions.functions.base import WindowFunction
from dbdb.tuples.context import ExecutionContext


class WindowCount(WindowFunction):
    NAMES = ["COUNT"]

    def _eval(self, context: ExecutionContext):
        return len(context.rows)


class WindowRowNumber(WindowFunction):
    NAMES = ["ROW_NUMBER"]

    def _eval(self, context: ExecutionContext):
        index = context.rows.index(context.row)
        return index + 1


class WindowSum(WindowFunction):
    NAMES = ["SUM"]

    def _eval(self, context: ExecutionContext):
        total = 0
        for row in context.rows:
            ctx = ExecutionContext(row=row)
            value = self.expr[0].eval(ctx)
            total += value

        return total


class WindowMin(WindowFunction):
    NAMES = ["MIN"]

    def _eval(self, context: ExecutionContext):
        min_val = None

        for row in context.rows:
            ctx = ExecutionContext(row=row)
            value = self.expr[0].eval(ctx)
            if min_val is None or value < min_val:
                min_val = value

        return min_val


class WindowMax(WindowFunction):
    NAMES = ["MAX"]

    def _eval(self, context: ExecutionContext):
        max_val = None
        for row in context.rows:
            ctx = ExecutionContext(row=row)
            value = self.expr[0].eval(ctx)
            if max_val is None or value > max_val:
                max_val = value

        return max_val


class WindowAverage(WindowFunction):
    NAMES = ["AVG", "MEAN"]

    def _eval(self, context: ExecutionContext):
        total = 0
        seen = 0
        for row in context.rows:
            ctx = ExecutionContext(row=row)
            value = self.expr[0].eval(ctx)
            total += value
            seen += 1

        if seen == 0:
            return None

        return total / seen


class WindowLag(WindowFunction):
    NAMES = ["LAG"]

    def _eval(self, context: ExecutionContext):
        if len(self.expr) == 2:
            offset_expr = self.expr[1]
            offset = offset_expr.eval(context)
        else:
            offset = 1

        row_idx = context.row_index - offset
        if row_idx < 0 or row_idx >= len(context.rows):
            return None

        row = context.rows[row_idx]
        ctx = ExecutionContext(row=row)
        value = self.expr[0].eval(ctx)
        return value


class WindowLead(WindowFunction):
    NAMES = ["LEAD"]

    def _eval(self, context: ExecutionContext):
        if len(self.expr) == 2:
            offset_expr = self.expr[1]
            offset = offset_expr.eval(context)
        else:
            offset = 1

        row_idx = context.row_index + offset
        if row_idx < 0 or row_idx >= len(context.rows):
            return None

        row = context.rows[row_idx]
        ctx = ExecutionContext(row=row)
        value = self.expr[0].eval(ctx)
        return value
