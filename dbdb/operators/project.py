from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows, RowTuple
from dbdb.tuples.identifiers import FieldIdentifier

from dbdb.operators.functions import find_func


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
                if projection.expr == "*":
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
        from dbdb.lang.lang import ColumnIdentifier, FunctionCall

        fields = []
        projections = self.config.project
        unnamed_col_counter = 0
        for projection in projections:
            if projection.expr == "*":
                for field in rows.fields:
                    fields.append(field)
            else:
                if projection.alias:
                    col_name = projection.alias
                elif isinstance(projection.expr, ColumnIdentifier):
                    col_name = projection.expr.column
                elif isinstance(projection.expr, FunctionCall):
                    col_name = projection.expr.func_name.lower()
                else:
                    col_name = f"col_{unnamed_col_counter}"
                    unnamed_col_counter += 1

                field = FieldIdentifier(col_name, rows.table)
                fields.append(field)

        return fields

    async def run(self, rows):
        self.stats.update_start_running()

        fields = self.list_fields(rows)

        iterator = self.make_iterator(rows)
        self.iterator = iterator
        return Rows(rows.table, fields, iterator)
