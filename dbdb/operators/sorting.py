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

    def run(self, tuples):
        order_index, reverse = self.config.order
        yield from sorted(
            tuples,
            key=lambda t: t[order_index],
            reverse=reverse,
        )
