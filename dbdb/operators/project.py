from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.rows import Rows


class ProjectConfig(OperatorConfig):
    def __init__(
        self,
        columns=None
    ):
        self.columns = columns


class ProjectOperator(Operator):
    Config = ProjectConfig

    def make_iterator(self, tuples):
        for row in tuples:
            projected = tuple(row[i] for (i, _) in self.config.columns)
            yield projected

    def run(self, rows):
        fields = [name for (_, name) in self.config.columns]
        iterator = self.make_iterator(rows)
        return Rows(fields, iterator)
