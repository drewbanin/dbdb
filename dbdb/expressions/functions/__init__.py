from dbdb.expressions.functions.base import (
    ScalarFunction,
    AggregateFunction,
    WindowFunction,
    TableFunction,
    FunctionTypes,
)

import functools
import inspect


def is_subclass_of(value, base_class):
    return inspect.isclass(value) and issubclass(value, base_class)


@functools.cache
def get_functions_by_type(module, base_class):
    func_map = {}
    for classname in dir(module):
        klass = getattr(module, classname)
        if is_subclass_of(klass, base_class):
            for name in klass.NAMES:
                func_map[name.upper()] = klass

    return func_map


def find_function_by_type(func_name: str, module, base_class):
    functions = get_functions_by_type(module, base_class)
    return functions.get(func_name.upper())


@functools.cache
def find_function(func_name: str, f_type: FunctionTypes):
    if f_type == FunctionTypes.SCALAR:
        from dbdb.expressions.functions import scalar_functions

        return find_function_by_type(func_name, scalar_functions, ScalarFunction)

    elif f_type == FunctionTypes.TABLE:
        from dbdb.expressions.functions import table_functions

        return find_function_by_type(func_name, table_functions, TableFunction)

    elif f_type == FunctionTypes.AGGREGATE:
        from dbdb.expressions.functions import aggregate_functions

        return find_function_by_type(func_name, aggregate_functions, AggregateFunction)

    elif f_type == FunctionTypes.WINDOW:
        from dbdb.expressions.functions import window_functions

        return find_function_by_type(func_name, window_functions, WindowFunction)

    else:
        raise RuntimeError(f"Invalid function type: {f_type}")
