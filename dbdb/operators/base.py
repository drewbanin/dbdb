from dbdb.operators.operator_stats import OperatorStats
from dbdb.logger import logger

import tabulate
import uuid


def pipeline(*iterables):
    stream = iterables[0]
    # Feed forward each iterator into the next iterator

    for fn in iterables[1:]:
        stream = fn(stream)

    yield from stream


class OperatorConfig:
    def __init__(self):
        pass


class Operator:
    Config = OperatorConfig

    def __init__(self, **config):
        self.operator_id = uuid.uuid4()
        self.cache = {}
        self.config = self.Config(**config)
        self.stats = OperatorStats(
            operator_id=id(self),
            operator_type=self.name()
        )

        self.iterator = None

    async def run(self):
        raise NotImplementedError()

    def close(self):
        if self.iterator:
            self.iterator.aclose()

    def name(self):
        raise NotImplementedError()

    def is_mutation(self):
        return False

    def details(self):
        return {}

    def to_dict(self):
        return {
            "id": id(self),
            "name": self.name(),
            "details": self.details()
        }

    def print_cache(self):
        rows = []
        keys = sorted(self.cache.keys())
        for key in keys:
            rows.append((key, self.cache[key]))

        tbl = tabulate.tabulate(
            rows,
            headers=['key', 'value'],
            tablefmt='presto',
            floatfmt="0.2f"
        )
        logger.info(tbl)

    def __hash__(self):
        return hash(str(self.operator_id))

    def __eq__(self, other):
        return False
