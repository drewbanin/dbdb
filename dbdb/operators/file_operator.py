
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

    def name(self):
        return "Table Scan"

    def details(self):
        return {
            "table": self.config.table_ref,
            "columns": self.config.columns,
        }

    def make_iterator(self, tuples):
        for i, record in enumerate(tuples):
            self.stats.update_row_processed(record)

            yield record
            self.stats.update_custom_stats(self.reader.stats())
            self.stats.update_row_emitted(record)

        self.stats.update_done_running()

    def run(self):
        self.stats.update_start_running()

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


class TableGenOperator(OperatorConfig):
    def __init__(
        self,
        table,
        rows,
    ):
        self.table = table
        self.rows = rows


class TableGenOperator(Operator):
    Config = TableGenOperator

    def run(self):
        return self.config.rows
