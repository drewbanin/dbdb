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

    def make_iterator(self, tuples):
        projections = self.config.project
        for row in tuples:
            projected = []
            for projection in projections:
                value = projection.expr.eval(row)
                projected.append(value)

            yield projected

    async def run(self, rows):
        fields = []
        projections = self.config.project
        for projection in projections:
            alias = projection.alias
            field = FieldIdentifier(alias, rows.table)
            fields.append(field)

        iterator = self.make_iterator(rows)
        return Rows(rows.table, fields, iterator)
