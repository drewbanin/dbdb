
from dbdb.operators.functions import system

modules = [
    system,
]


func_map = {}
for mod in modules:
    for local in dir(mod):
        if local.startswith('DBDB_'):
            func_map[local] = getattr(mod, local)


def find_func(func_name):
    search = f"DBDB_{func_name.upper()}"
    if search in func_map:
        return func_map[search]
    else:
        raise RuntimeError(f"function {func_name} not found!")
