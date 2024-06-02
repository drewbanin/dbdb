
class Aggregation:
    ...


class AggregationMin(Aggregation):
    def __init__(self):
        self.min = None

    def process(self, values):
        value = values[0]
        if self.min is None:
            self.min = value

        elif value < self.min:
            self.min = value

    def result(self):
        return self.min


class AggregationMax(Aggregation):
    def __init__(self):
        self.max = None

    def process(self, values):
        value = values[0]
        if self.max is None:
            self.max = value

        elif value > self.max:
            self.max = value

    def result(self):
        return self.max


class AggregationSum(Aggregation):
    def __init__(self):
        self.sum = None

    def process(self, values):
        value = values[0]
        if self.sum is None:
            self.sum = value

        else:
            self.sum += value

    def result(self):
        return self.sum


class AggregationAverage(Aggregation):
    def __init__(self):
        self.sum = 0
        self.seen = 0

    def process(self, values):
        value = values[0]
        self.sum += value
        self.seen += 1

    def result(self):
        if self.seen == 0:
            return None

        return self.sum / self.seen


class AggregationCount(Aggregation):
    def __init__(self):
        self.seen = 0

    def process(self, values):
        # TODO : I think this should not count nulls...
        self.seen += 1

    def result(self):
        return self.seen


class AggregationCountDistinct(Aggregation):
    def __init__(self):
        self.seen = set()

    def process(self, values):
        value = values[0]
        self.seen.add(value)

    def result(self):
        return len(self.seen)


class AggregationListAgg(Aggregation):
    def __init__(self):
        self.seen = []
        self.delim = ','

    def process(self, values):
        if len(values) == 1:
            value = values[0]
        else:
            value, self.delim = values

        self.seen.append(value)

    def result(self):
        self.result = self.delim.join([str(s) for s in self.seen])
        return self.result
