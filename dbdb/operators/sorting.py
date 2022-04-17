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

    def sort_func(self, row):
        # function that returns a tuple of sort orders...
        # this will sort "ascending", so make sure that
        # we account for sort order in here... somehow...

        sort_keys = []
        for ascending, key_f in self.config.order:
            if ascending:
                key = key_f(row)
            else:
                key = ReverseSort(key_f(row))

            sort_keys.append(key)

        return sort_keys

    def make_iterator(self, tuples):
        yield from sorted(
            tuples,
            key=self.sort_func,
        )

    def run(self, rows):
        iterator = self.make_iterator(rows)
        return rows.new(iterator)
