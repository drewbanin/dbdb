
from dbdb.files import file_format

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


# TODO : We need to do some more book-keeping here to keep track of page
# traversal for stats. That's kind of annoying... how do we do it in a
# way that doesn't suck?
class TableScan(Operator):
    def run(self):
        self.cache['rows_seen'] = 0
        self.cache['pages_seen'] = 0

        for page in file_format.read_pages(
            table_ref=self.config.table_ref,
            columns=self.config.columns
        ):
            # TODO: We need to rework this if we're going to support
            # ordering... ie. we can't limit until we've sorted. Is that
            # done by this operator, or another operator in front of it?
            # or like... maybe limiting is its own thing?

            # We can cache this part, but we need some sort
            # of stats module and some sort of wrapper around
            # file handles... ok... that's fine....
            self.cache['rows_seen'] += 1
            # self.cache['pages_seen'] += 1

            yield page

            limit = self.config.limit
            order = self.config.order
            # TODO : Do something with order?

            if limit is not None and self.cache['rows_seen'] >= limit:
                break
