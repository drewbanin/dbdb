from dbdb.operators.base import Operator, OperatorConfig


class ProjectConfig(OperatorConfig):
    def __init__(
        self,
        columns=None
    ):
        self.columns = columns


class ProjectOperator(Operator):
    Config = ProjectConfig

    def run(self, tuples):
        for row in tuples:
            projected = tuple(row[i] for i in self.config.columns)
            yield projected
