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

        skip = False
        test_name = lines.pop(0).strip()

        if "\n" in test_name:
            test_name, opts = test_name.split("\n")
            if "skip" in opts:
                skip = True

        test_body = lines.pop(0).strip()

        parts = test_body.split("---")
        test_sql, test_expected_yml = parts
        test_expected = yaml.load(test_expected_yml, Loader=yaml.Loader)

        yield (
            test_path,
            test_name,
            test_sql,
            test_expected,
            skip,
        )


def find_tests(dir_path):
    print(f"Looking for files in {dir_path}")

    for p in dir_path.iterdir():
        if p.is_dir():
            yield from find_tests(p)
            continue
        elif p.suffix == ".txt":
            yield from parse_test_file(p.resolve())


def make_test_name(test_index):
    filename, test_name, sql, expected, skip = SQL_TESTS[test_index]

    filename_s = filename.stem
    test_name_s = test_name.replace(" ", "-").lower()
    return f"{filename_s}.{test_name_s}"


SQL_TESTS = list(find_tests(SQL_DIR))
SQL_TEST_INDEX = {make_test_name(i): test for i, test in enumerate(SQL_TESTS)}


def run_query(filename, test_name, sql):
    query_id, plan, nodes, edges = dbdb.engine.plan_query(sql)
    task = dbdb.engine.run_query(query_id, plan, nodes)
    try:
        return asyncio.run(task)
    except RuntimeError as e:
        relpath = pathlib.Path(filename).relative_to(TEST_DIR)
        raise RuntimeError(f"Error in {relpath} - {test_name}: {e}")


@pytest.mark.parametrize("test_index", SQL_TEST_INDEX.keys())
def test_sql_statement(test_index):
    filename, test_name, sql, expected, skip = SQL_TEST_INDEX[test_index]
    if skip:
        pytest.skip("Test is skipped")
        return

    # Run all statements, but only save results of the last one
    # Splitting on the ; is kind of jank - we should do this using
    # the grammar - but i think it's fine for the test suite...
    statements = sql.split(";")
    actual = None
    for sql_statement in statements:
        sql_statement = sql_statement.strip()
        if len(sql_statement) == 0:
            continue

        actual = run_query(filename, test_name, sql_statement)

    assert actual == expected
