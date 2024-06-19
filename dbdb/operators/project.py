from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows
from dbdb.tuples.identifiers import FieldIdentifier
from dbdb.tuples.context import ExecutionContext

import asyncio


class ProjectConfig(OperatorConfig):
    def __init__(self, project=None):
        self.project = project


class WindowIterator:
    "Fake an async iterator over a tuple - asyncio is bad"

    def __init__(self, rows):
        self.rows = rows
        self.iterator = self.make_iter(rows)

    def make_iter(self, rows):
        for row in rows:
            yield row

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0)
        try:
            return next(self.iterator)
        except StopIteration:
            raise StopAsyncIteration()


class ProjectOperator(Operator):
    Config = ProjectConfig

    def name(self):
        return "Projection"

    async def make_iterator(self, tuples):
        projections = self.config.project

        has_window = False
        for projection in projections:
            is_window = projection.is_window()
            is_agg = projection.is_aggregate()

            if is_agg:
                raise RuntimeError(
                    "Cannot use projection operator with an aggregate expression..."
                )
            elif is_window:
                has_window = True

        if has_window:
            rows = await tuples.materialize()
            tuples = WindowIterator(rows)
        else:
            rows = tuples

        async for row in tuples:
            context = ExecutionContext(row=row, rows=rows)
            self.stats.update_row_processed(row)
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
            # self.stats.update_row_emitted(row)
        self.stats.update_done_running()

    def list_fields(self, rows):
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

        fields = self.list_fields(rows)

        iterator = self.make_iterator(rows)
        iterator = self.add_exit_check(iterator)
        return Rows(rows.table, fields, iterator)
