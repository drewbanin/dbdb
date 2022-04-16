
from dbdb.files import file_format
from dbdb.files.file_wrapper import FileReader

import itertools

# Add some shit in here
class OperatorConfig:
    def __init__(self):
        pass


class Operator:
    def __init__(self, config):
        self.cache = {}
        self.config = config


class TableScanConfig:
    def __init__(
        self,
        table_ref,
        columns,
        limit=None,
        order=None,
    ):
        self.table_ref = table_ref
        self.columns = columns
        self.limit = limit
        self.order = order


# TODO : We need to do some more book-keeping here to keep track of page
# traversal for stats. That's kind of annoying... how do we do it in a
# way that doesn't suck?
class TableScan(Operator):
    def update_stats(self, tuples):
        for t in tuples:
            self.cache['rows_seen'] += 1
            yield t

    def reorder(self, tuples):
        # LOL.... can this know about my sort key? Need to make
        # it easier to interrogate the table from these operators..
        if self.config.order:
            order_index, order_dir = self.config.order
            collect = sorted(
                tuples,
                key=lambda t: t[order_index] * order_dir
            )
            yield from collect
        else:
            yield from tuples

    def apply_limit(self, tuples):
        for i, t in enumerate(tuples):
            if self.config.limit is not None and i >= self.config.limit:
                break
            yield t

    def pipeline(self, tuples, *fns):
        stream = tuples
        # Feed forward each iterator into the next iterator
        for fn in fns:
            stream = fn(stream)

        yield from stream

    def run(self):
        self.cache['rows_seen'] = 0

        reader = FileReader(self.config.table_ref)

        iterator = file_format.read_pages(
            reader=reader,
            columns=self.config.columns
        )

        yield from self.pipeline(
            iterator,

            self.reorder,
            self.apply_limit,
            self.update_stats,
        )

        self.cache.update(reader.stats())
