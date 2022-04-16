from dbdb.operators.base import Operator, OperatorConfig
import itertools


class LimitConfig(OperatorConfig):
    def __init__(
        self,
        limit,
    ):
        self.limit = limit


class LimitOperator(Operator):
    Config = LimitConfig

    def run(self, tuples):
        limit = self.config.limit

        if limit == 0:
            return

        for i, val in enumerate(tuples):
            yield val

            if i >= limit - 1:
                break
