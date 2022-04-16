
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
