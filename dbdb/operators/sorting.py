from dbdb.operators.base import Operator, OperatorConfig
from dbdb.expressions.expressions import Literal
from dbdb.tuples.context import ExecutionContext


class ReverseSort:
    def __init__(self, row):
        self.row = row

    def __eq__(self, other):
        return self.row == other.row

    # Note: this is intentionally backwards!
    def __lt__(self, other):
        return self.row > other.row


class SortingConfig(OperatorConfig):
    def __init__(
        self,
        order,
    ):
        self.order = order


class SortOperator(Operator):
    Config = SortingConfig

    def name(self):
        return "Sort"

    def sort_func(self, row):
        # function that returns a tuple of sort orders...
        # this will sort "ascending", so make sure that
        # we account for sort order in here... somehow...

        self.stats.update_row_processed(row)
        sort_keys = []
        context = ExecutionContext(row=row)
        for ascending, projection in self.config.order:
            if isinstance(projection, Literal):
                key = row.data[projection.value() - 1]
            else:
                key = projection.eval(context)

            if not ascending:
                key = ReverseSort(key)

            sort_keys.append(key)

        return sort_keys

    async def make_iterator(self, rows):
        data = await rows.materialize()
        for row in sorted(
            data,
            key=self.sort_func,
        ):
            self.stats.update_row_emitted(row)
            yield row

        self.stats.update_done_running()

    async def run(self, rows):
        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        iterator = self.add_exit_check(iterator)
        return rows.new(iterator)
