#!/usr/bin/env python

import datetime
import time
import hexdump
import random


from dbdb.files.file_format import (
    Table,
    Column,
    ColumnInfo,
    ColumnData
)
from dbdb.files.types import (
        DataType,
        DataEncoding,
        DataSorting
)

from dbdb.lang import lang


sample_data = list(range(100))
col1 = Column.new(
    column_name='my_number',
    column_type=DataType.INT8,
    encoding=DataEncoding.RUN_LENGTH,
    sorting=DataSorting.SORTED,
    data=sample_data
)

col2 = Column.new(
    column_name='is_odd',
    column_type=DataType.BOOL,
    encoding=DataEncoding.RAW,
    sorting=DataSorting.UNSORTED,
    data=list(i % 2 for i in sample_data)
)


def make_date(i):
    day = datetime.date(2022, 1, 1) + datetime.timedelta(i)
    return int(time.mktime(day.timetuple()))


col3 = Column.new(
    column_name='my_time',
    column_type=DataType.DATE,
    encoding=DataEncoding.DELTA,
    sorting=DataSorting.UNSORTED,
    data=[make_date(i) for i in sample_data],
)

words = ['abc', 'def', 'ghi']
col4 = Column.new(
    column_name='my_string',
    column_type=DataType.STR,
    column_width=5,
    encoding=DataEncoding.DICTIONARY,
    sorting=DataSorting.UNSORTED,
    data=[random.choice(words) for i in sample_data],
)


table = Table(
    columns=[col1, col2, col3, col4],
)


def to_hex(buf_s, line_width=16):
    return hexdump.hexdump(buf_s, result='return')


def test():
    print("Serialize")
    table.describe()
    byte_array = table.serialize()

    print()
    print(to_hex(byte_array))

    with open('my_table.dumb', 'wb') as fh:
        fh.write(byte_array)

    print()
    print("Deserialize")
    deserialized = table.deserialize(byte_array)
    deserialized.describe()

    print()
    print("Query:")
    query = """
    select
        my_number,
        is_odd
    from my_table
    limit 2
    """.rstrip()
    print(query)

    struct_query = lang.parse_query(query)
    print()
    print("Parsed:")
    struct_query.describe()


if __name__ == '__main__':
    test()
