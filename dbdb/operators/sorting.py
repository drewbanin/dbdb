from dbdb.operators.base import Operator, OperatorConfig
import itertools


class SortingConfig(OperatorConfig):
    def __init__(
        self,
        order,
    ):
        self.order = order


class SortOperator(Operator):
    Config = SortingConfig

    def make_iterator(self, tuples):
        order_index, ascending = self.config.order
        yield from sorted(
            tuples,
            key=lambda t: t[order_index],
            reverse=not ascending,
        )

    def run(self, rows):
        iterator = self.make_iterator(rows)
        return rows.new(iterator)
