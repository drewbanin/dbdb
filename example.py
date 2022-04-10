#!/usr/bin/env python

import datetime
import time
import hexdump

from dbdb.files.file_format import (
    Table,
    Column,
)
from dbdb.files.types import (
        DataType,
        DataEncoding,
        DataSorting
)


sample_data = [2, 7, 3, 9, 1]
col1 = Column(
    name='my_number',
    data_type=DataType.INT8,
    encoding=DataEncoding.RAW,
    is_sorted=DataSorting.SORTED,
    data=sample_data
)

col2 = Column(
    name='is_odd',
    data_type=DataType.BOOL,
    encoding=DataEncoding.RAW,
    is_sorted=DataSorting.UNSORTED,
    data=list(i % 2 for i in sample_data)
)

col3 = Column(
    name='my_time',
    data_type=DataType.DATE,
    encoding=DataEncoding.RAW,
    is_sorted=DataSorting.UNSORTED,
    data=[
        int(time.mktime(datetime.date(2022, 1, 1).timetuple())),
        int(time.mktime(datetime.date(2022, 1, 2).timetuple())),
        int(time.mktime(datetime.date(2022, 1, 3).timetuple())),
        int(time.mktime(datetime.date(2022, 1, 4).timetuple())),
        int(time.mktime(datetime.date(2022, 1, 5).timetuple())),

    ]
)

col4 = Column(
    name='my_string',
    data_type=DataType.STR,
    encoding=DataEncoding.RAW,
    is_sorted=DataSorting.UNSORTED,
    data=[
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
    ]
)

table = Table(
    columns=[col1, col2, col3, col4],
)


def to_hex(buf_s, line_width=16):
    return hexdump.hexdump(buf_s, result='return')


def test():
    table.describe()
    byte_array = table.serialize()

    print()
    print(to_hex(byte_array))

    with open('my_table.dumb', 'wb') as fh:
        fh.write(byte_array)

    deserialized = table.deserialize(byte_array)
    deserialized.describe()


if __name__ == '__main__':
    test()
