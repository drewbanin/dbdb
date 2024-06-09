from dbdb.io import file_format
from dbdb.io.file_format import ColumnInfo, ColumnData, Column, Table
from dbdb.io.file_wrapper import FileReader

from dbdb.operators.base import Operator, OperatorConfig, pipeline
from dbdb.tuples.rows import Rows

from dbdb.logger import logger

import itertools

class CreateTableAsConfig(OperatorConfig):
    def __init__(
        self,
        table,
    ):
        self.table = table


class CreateTableAsOperator(Operator):
    Config = CreateTableAsConfig

    def name(self):
        return "Create Table"

    def details(self):
        return {}

    def is_mutation(self):
        return True

    def status_line(self):
        return f"CREATE {self.rows_written}"

    def make_columns_from_data(self, records):
        row = records[0]
        columns = []
        for i, field in enumerate(row.fields):
            field_name = field.name

            field_value = row.data[i]
            field_type = file_format.infer_type(field_value)

            column_info = file_format.ColumnInfo(
                column_type=field_type,
                column_name=field_name,
            )

            column_data = ColumnData([row.data[i] for row in records])

            column = Column(
                column_info=column_info,
                column_data=column_data,
            )

            columns.append(column)

        return columns

    async def make_iterator(self, tuples):
        records = []
        async for record in tuples:
            self.stats.update_row_processed(record)
            records.append(record)

        # This is dumb, but if there are no rows, then we
        # cannot infer column types. Need to go back and add
        # typing to operators to propagate types through the graph
        if len(records) == 0:
            raise RuntimeError("Cannot create empty table")

        columns = self.make_columns_from_data(records)
        table = Table(columns=columns)
        table_data = table.serialize()

        table_size = len(table_data)
        one_mb = 1_000_000
        if table_size > one_mb:
            size_human = table_size / one_mb
            raise RuntimeError(
                f"Tried to write {size_human:0.2f}mb, but the maximum allowed "
                "table size is about a megabyte - sorry!"
            )

        reader = FileReader(self.config.table)
        with reader.open('wb') as fh:
            fh.write(table_data)

        logger.info(f"Done writing table {self.config.table}")

        self.rows_written = len(records)

        self.stats.update_row_emitted([])
        yield []
        self.stats.update_done_running()

    async def run(self, rows):
        self.rows_written = 0
        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        iterator = self.add_exit_check(iterator)
        return rows.new(iterator)
