

class Expression:
    def __init__(self, expression_func):
        self.expression_func = expression_func

    def evaluate(self, row):
        return self.expression_func(row)
