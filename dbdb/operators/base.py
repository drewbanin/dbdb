
import tabulate

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
        self.cache = {}
        self.config = self.Config(**config)

    def run(self):
        raise NotImplementedError()

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
        print(tbl)

