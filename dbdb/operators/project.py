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

    def make_iterator(self, tuples):
        projections = self.config.project
        for row in tuples:
            projected = []
            for project_f, _ in projections:
                value = project_f(row)
                projected.append(value)

            yield projected

    def run(self, rows):
        fields = []
        projections = self.config.project
        for _, alias in projections:
            field = FieldIdentifier(alias, rows.table)
            fields.append(field)

        iterator = self.make_iterator(rows)
        return Rows(rows.table, fields, iterator)
