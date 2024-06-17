from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows
from dbdb.tuples.identifiers import FieldIdentifier
from dbdb.tuples.context import ExecutionContext

from collections import defaultdict
import enum
import itertools


class WindowConfig(OperatorConfig):
    def __init__(
        self,
        projections,
    ):
        self.projections = projections


class WindowOperator(Operator):
    Config = WindowConfig

    def name(self):
        return "Window"

    async def make_iterator(self, rows):
        projections = self.config.projections.projections

        rows = rows.materialize()

        index = 0
        async for row in rows:
            context = ExecutionContext(row=row, row_index=index, rows=rows)

            self.stats.update_row_processed(row)
            # self.stats.update_row_processed(row)
            projected = []
            for projection in projections:
                if projection.is_star():
                    for value in row.data:
                        projected.append(value)
                else:
                    value = projection.expr.eval(context)
                    projected.append(value)

            yield projected
            self.stats.update_row_emitted(projected)
            index += 1

        self.stats.update_done_running()

    def list_fields(self, rows):
        # TODO : This is a dupe of the impl in project.py...!
        fields = []
        projections = self.config.project
        for i, projection in enumerate(projections):
            if projection.is_star():
                for field in rows.fields:
                    fields.append(field)
            else:
                if projection.alias:
                    col_name = projection.alias
                elif projection.can_derive_name():
                    col_name = projection.make_name()
                else:
                    col_name = f"col_{i + 1}"

                field = FieldIdentifier(col_name, rows.table)
                fields.append(field)

        return fields

    async def run(self, rows):
        self.stats.update_start_running()
        iterator = self.make_iterator(rows)
        iterator = self.add_exit_check(iterator)

        fields = self.list_fields(rows)
        return Rows(rows.table, fields, iterator)
