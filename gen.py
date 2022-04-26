#!/usr/bin/env python

import datetime
import time
import hexdump
import random


from dbdb.io.file_format import (
    Table,
    Column,
    ColumnInfo,
    ColumnData
)
from dbdb.io.types import (
    DataType,
    DataEncoding,
    DataCompression,
    DataSorting
)

from dbdb.lang import lang


sample_data = list(range(1000000))
col1 = Column.new(
    column_name='my_number',
    column_type=DataType.INT32,
    encoding=DataEncoding.RUN_LENGTH,
    compression=DataCompression.ZLIB,
    sorting=DataSorting.SORTED,
    data=[i for i in sample_data]
)

col2 = Column.new(
    column_name='is_odd',
    column_type=DataType.BOOL,
    encoding=DataEncoding.RAW,
    sorting=DataSorting.UNSORTED,
    data=list(i % 2 == 0 for i in sample_data)
)


def make_date(i):
    day = datetime.datetime(2022, 1, 1) + datetime.timedelta(seconds=i)
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


table.describe()
byte_array = table.serialize()

with open('my_table.dumb', 'wb') as fh:
    fh.write(byte_array)


# -------------------- Second table ----------------


col1 = Column.new(
    column_name='powers_of_two',
    column_type=DataType.INT32,
    encoding=DataEncoding.RUN_LENGTH,
    compression=DataCompression.ZLIB,
    sorting=DataSorting.SORTED,
    data=[2,4,8,16,32]
)

col2 = Column.new(
    column_name='my_string',
    column_type=DataType.STR,
    column_width=5,
    encoding=DataEncoding.DICTIONARY,
    sorting=DataSorting.UNSORTED,
    data=['abc', 'def', 'ghi', 'jkl', 'mno']
)


table = Table(
    columns=[col1, col2],
)


table.describe()
byte_array = table.serialize()

with open('other_table.dumb', 'wb') as fh:
    fh.write(byte_array)

