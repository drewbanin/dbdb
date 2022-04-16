
from dbdb.files import file_format
from dbdb.files.file_wrapper import FileReader

from dbdb.operators.base import Operator, OperatorConfig, pipeline

import itertools


class TableScanConfig(OperatorConfig):
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


class TableScanOperator(Operator):
    Config = TableScanConfig

    def update_stats(self, tuples):
        for t in tuples:
            self.cache['rows_seen'] += 1
            self.cache.update(self.reader.stats())
            yield t

    def run(self):
        self.cache['rows_seen'] = 0

        self.reader = FileReader(self.config.table_ref)

        iterator = file_format.read_pages(
            reader=self.reader,
            columns=self.config.columns
        )

        yield from pipeline(
            iterator,
            self.update_stats,
        )
