from dbdb.operators.google_sheets import GoogleSheetsOperator
from dbdb.operators.midi import MIDIOperator
from dbdb.operators.generate_series import GenerateSeriesOperator


FUNCS = [
    GoogleSheetsOperator,
    MIDIOperator,
    GenerateSeriesOperator,
]

FUNC_MAP = {f.function_name(): f for f in FUNCS}


def as_operator(table, function_name, function_args):
    func_op = FUNC_MAP.get(function_name)
    if not func_op:
        raise RuntimeError(f"Function {function_name} does not exist")

    return func_op(table=table, function_args=function_args)
