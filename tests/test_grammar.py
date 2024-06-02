
import pytest
import asyncio
import pathlib
import re
import yaml

import dbdb.engine

TEST_DIR = pathlib.Path(__file__).parent.resolve()
SQL_DIR = TEST_DIR / "sql"


def parse_test_file(test_path):
    with open(test_path) as fh:
        contents = fh.read().strip()

    collected_tests = []
    delim = r"^=+$"
    lines = re.split(delim, contents, flags=re.MULTILINE)

    while len(lines) > 0:
        if lines[0].strip() == "":
            lines.pop(0)
            continue

        test_name = lines.pop(0)
        test_body = lines.pop(0)
        parts = test_body.split("---")
        test_sql, test_expected_yml = parts
        test_expected = yaml.load(test_expected_yml, Loader=yaml.Loader)

        yield (
            test_path,
            test_name.strip(),
            test_sql.strip(),
            test_expected
        )

def find_tests(dir_path):
    print(f"Looking for files in {dir_path}")

    for p in dir_path.iterdir():
        if p.is_dir():
            yield from find_tests(p)
            continue
        elif p.suffix == ".txt":
            yield from parse_test_file(p.resolve())


SQL_TESTS = list(find_tests(SQL_DIR))

def run_query(filename, test_name, sql):
    query_id, plan, nodes, edges = dbdb.engine.plan_query(sql)
    task = dbdb.engine.run_query(query_id, plan, nodes)
    try:
        return asyncio.run(task)
    except RuntimeError as e:
        relpath = pathlib.Path(filename).relative_to(TEST_DIR)
        raise RuntimeError(f"Error in {relpath} - {test_name}: {e}")


@pytest.mark.parametrize("filename, test_name, sql, expected", SQL_TESTS)
def test_sql_statement(filename, test_name, sql, expected):
    actual = run_query(filename, test_name, sql)
    assert actual == expected
