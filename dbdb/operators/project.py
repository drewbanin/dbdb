from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows
from dbdb.tuples.identifiers import FieldIdentifier
from dbdb.tuples.context import ExecutionContext


class ProjectConfig(OperatorConfig):
    def __init__(self, project=None):
        self.project = project


class ProjectOperator(Operator):
    Config = ProjectConfig

    def name(self):
        return "Projection"

    async def make_iterator(self, tuples):
        projections = self.config.project

        # TODO : Do not do this unless there are window functions!!
        rows = await tuples.materialize()

        for row in rows:
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
