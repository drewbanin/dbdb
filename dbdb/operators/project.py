from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows, RowTuple
from dbdb.tuples.identifiers import FieldIdentifier


class ProjectConfig(OperatorConfig):
    def __init__(
        self,
        project=None
    ):
        self.project = project


class ProjectOperator(Operator):
    Config = ProjectConfig

    def name(self):
        return "Projection"

    async def make_iterator(self, tuples):
        projections = self.config.project
        async for row in tuples:
            self.stats.update_row_processed(row)
            # self.stats.update_row_processed(row)
            projected = []
            for projection in projections:
                if projection.is_star():
                    for value in row.data:
                        projected.append(value)
                else:
                    value = projection.expr.eval(row)
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
        self.iterator = iterator
        return Rows(rows.table, fields, iterator)
