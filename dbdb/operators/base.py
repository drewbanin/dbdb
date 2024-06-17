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


class SafeIterator:
    def __init__(self, iterator):
        self.iterator = iterator
        self.exit_next_tick = False

    def should_exit(self):
        self.exit_next_tick = True

    def check_raise(self):
        if self.exit_next_tick:
            raise GeneratorExit()

    def __iter__(self):
        return self.iterator

    def __aiter__(self):
        return self.iterator

    def __next__(self):
        self.check_raise()
        return self.iterator.__next__()

    async def __anext__(self):
        self.check_raise()
        return await self.iterator.__anext__()


class Operator:
    Config = OperatorConfig

    def __init__(self, **config):
        self.operator_id = uuid.uuid4()
        self.cache = {}
        self.config = self.Config(**config)
        self.stats = OperatorStats(operator_id=id(self), operator_type=self.name())

        self.iterator = None
        self.exit_next_tick = False
        self.safe_iterator = None

    def exit(self):
        self.safe_iterator.should_exit()

    def add_exit_check(self, iterator):
        self.safe_iterator = SafeIterator(iterator)
        return self.safe_iterator

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
        return {"id": id(self), "name": self.name(), "details": self.details()}

    def print_cache(self):
        rows = []
        keys = sorted(self.cache.keys())
        for key in keys:
            rows.append((key, self.cache[key]))

        tbl = tabulate.tabulate(
            rows, headers=["key", "value"], tablefmt="presto", floatfmt="0.2f"
        )
        logger.info(tbl)

    def __hash__(self):
        return hash(str(self.operator_id))

    def __eq__(self, other):
        return False
