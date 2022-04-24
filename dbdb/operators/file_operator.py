
from dbdb.io import file_format
from dbdb.io.file_wrapper import FileReader

from dbdb.operators.base import Operator, OperatorConfig, pipeline
from dbdb.tuples.rows import Rows

import itertools


class TableScanConfig(OperatorConfig):
    def __init__(
        self,
        table_ref,
        columns,

        limit=None,
        order=None,
        table=None,
    ):
        self.table_ref = table_ref
        self.limit = limit
        self.order = order

        self.table = table
        self.columns = columns


class TableScanOperator(Operator):
    Config = TableScanConfig

    def update_stats(self, tuples):
        self.cache['rows_seen'] += 1
        self.cache.update(self.reader.stats())

    def make_iterator(self, tuples):
        for record in tuples:
            yield record
            self.update_stats(record)

    def run(self):
        self.cache['rows_seen'] = 0

        self.reader = FileReader(self.config.table_ref)
        column_names = [c.name for c in self.config.columns]

        tuples = file_format.read_pages(
            reader=self.reader,
            columns=column_names
        )

        iterator = self.make_iterator(tuples)
        return Rows(
            self.config.table,
            self.config.columns,
            iterator,
        )
