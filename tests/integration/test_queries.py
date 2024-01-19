from unittest import TestCase, mock

import datetime
import time
import hexdump


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



def make_table(size):
    sample_data = list(range(size))
    col1 = Column.new(
        column_name='my_number',
        column_type=DataType.INT32,
        encoding=DataEncoding.RUN_LENGTH,
        compression=DataCompression.ZLIB,
        sorting=DataSorting.SORTED,
        data=[1 for i in sample_data]
    )

    col2 = Column.new(
        column_name='is_odd',
        column_type=DataType.BOOL,
        encoding=DataEncoding.RAW,
        sorting=DataSorting.UNSORTED,
        data=list(i % 2 for i in sample_data)
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
        data=[words[i % len(words)] for i in sample_data],
    )


    table = Table(
        columns=[col1, col2, col3, col4],
    )
    return table


def to_hex(buf_s, line_width=16):
    return hexdump.hexdump(buf_s, result='return')


class QueryTestCase(TestCase):
    def test_query(self):
        table = make_table(1_000)
        print("Serialize")
        table.describe()
        byte_array = table.serialize()

        with open('test_table.dumb', 'wb') as fh:
            fh.write(byte_array)

        print()
        print("Deserialize")
        deserialized = table.deserialize(byte_array)
        deserialized.describe()

        sql = """
        select
            my_number,
            2 * is_odd
        from test_table
        """.rstrip()

        query = lang.parse_query(sql)
        res = query.execute()
        import pdb; pdb.set_trace()
        pass
