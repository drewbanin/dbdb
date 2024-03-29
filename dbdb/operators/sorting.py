from dbdb.operators.base import Operator, OperatorConfig
import itertools


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
        for ascending, projection in self.config.order:
            if projection.is_int():
                key = row.data[projection.value() - 1]
            else:
                key = projection.eval(row)

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
        return rows.new(iterator)
