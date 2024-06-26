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
    ):
        self.table_ref = table_ref
        self.limit = limit
        self.order = order
        self.columns = columns


class TableScanOperator(Operator):
    Config = TableScanConfig

    def name(self):
        return "Table Scan"

    def details(self):
        return {
            "table": self.config.table_ref,
            "qualified_table_name": str(self.config.table_ref),
            "columns": self.config.columns,
        }

    async def make_iterator(self, tuples):
        for record in tuples:
            self.stats.update_row_processed(record)

            yield record
            self.stats.update_custom_stats(self.reader.stats())
            self.stats.update_row_emitted(record)

        self.stats.update_done_running()

    async def run(self):
        self.stats.update_start_running()

        self.reader = FileReader(self.config.table_ref)

        column_data = file_format.read_header(self.reader)
        scanned_columns = [c.to_dict() for c in column_data]
        column_names = [c.column_name for c in column_data]

        self.stats.update_custom_stats(
            {
                "scanned_columns": scanned_columns,
                "file_ref": self.reader.table_ref,
            }
        )

        tuples = file_format.read_pages(reader=self.reader, columns=column_names)

        iterator = self.make_iterator(tuples)
        self.iterator = iterator

        return Rows(
            self.config.table_ref,
            self.config.columns,
            iterator,
        )


class TableGenOperatorConfig(OperatorConfig):
    def __init__(
        self,
        table,
        rows,
    ):
        self.table = table
        self.rows = rows


class TableGenOperator(Operator):
    Config = TableGenOperatorConfig

    def name(self):
        return "Row Generator"

    async def make_iterator(self):
        record = []
        self.stats.update_row_processed(record)
        yield record
        self.stats.update_row_emitted(record)
        self.stats.update_done_running()

    async def run(self):
        self.stats.update_start_running()

        columns = []
        iterator = self.make_iterator()
        iterator = self.add_exit_check(iterator)

        return Rows(
            self.config.table,
            columns,
            iterator,
        )
