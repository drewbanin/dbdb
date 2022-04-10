
"""
Describe file format here...
"""


import struct
import hexdump
from dbdb.files.types import (
    DataType,
    DataEncoding,
    DataSorting
)

from dbdb.files.constants import MAGIC_NUMBER


def sort_together(sort_index, to_sort):
    return [i for _, i in sorted(zip(sort_index, to_sort))]


def chomp(pack_s, buffer, unpack_single=True):
    size = struct.calcsize(pack_s)
    res = struct.unpack(pack_s, buffer[:size])
    if len(res) == 1 and unpack_single:
        res = res[0]
    return res, buffer[size:]


class Column(object):
    def __init__(
        self,
        name,
        data_type,
        encoding,
        is_sorted,
        data=None,
        min_val=None,
        max_val=None
    ):
        self.name = name
        self.data_type = data_type
        self.encoding = encoding
        self.is_sorted = is_sorted

        self.data = data
        self.min_val = min_val
        self.max_val = max_val

    def sort_by(self, sort_index):
        # this mutates the order of self.data
        self.data = sort_together(sort_index, self.data)

    def size(self):
        return len(self.data)

    def serialize_header(self):
        packed_bytes = bytearray()

        p_column_name = struct.pack(">16s", bytes(self.name, 'ascii'))
        packed_bytes.extend(p_column_name)

        p_column_type = struct.pack(">c", self.data_type.as_byte())
        packed_bytes.extend(p_column_type)

        p_column_encoding = struct.pack(">c", self.encoding.as_byte())
        packed_bytes.extend(p_column_encoding)

        p_is_sorted = struct.pack(">?", self.is_sorted.value)
        packed_bytes.extend(p_is_sorted)

        if self.data_type in (DataType.INT8, DataType.INT32):
            min_val = min(self.data)
            p_min_val = struct.pack(">i", min_val)
            packed_bytes.extend(p_min_val)

            max_val = max(self.data)
            p_max_val = struct.pack(">i", max_val)
            packed_bytes.extend(p_max_val)
        else:
            p_min_max_vals = struct.pack('>8x')
            packed_bytes.extend(p_min_max_vals)

        return packed_bytes

    @classmethod
    def deserialize_header(cls, buffer):
        name, buffer = chomp('>16s', buffer)
        data_type, buffer = chomp('>B', buffer)
        encoding, buffer = chomp('>B', buffer)
        is_sorted, buffer = chomp('>?', buffer)

        # Only valid if sortable, still want to eat bytes though
        if data_type in (DataType.INT8, DataType.INT32):
            min_val, buffer = chomp('>i', buffer)
            max_val, buffer = chomp('>i', buffer)
        else:
            _, buffer = chomp('>8x', buffer)
            min_val = 0
            max_val = 0

        column = cls(
            name.decode('ascii').strip('\x00'),
            DataType(data_type),
            DataEncoding(encoding),
            DataSorting(is_sorted),
            data=None,
            min_val=min_val,
            max_val=max_val
        )

        return column, buffer

    def serialize_column(self):
        size = len(self.data)

        if self.data_type == DataType.BOOL:
            packed = struct.pack(f'>{size}?', *self.data)
        elif self.data_type == DataType.INT8:
            packed = struct.pack(f'>{size}B', *self.data)
        elif self.data_type == DataType.INT32:
            packed = struct.pack(f'>{size}i', *self.data)
        elif self.data_type == DataType.DATE:
            packed = struct.pack(f'>{size}i', *self.data)
        elif self.data_type == DataType.STR:
            # strings, being annoying
            # for now, pad all strings to 8 bytes (lol)
            packed = bytearray()
            for string in self.data:
                buf = struct.pack('>8s', string.encode())
                packed.extend(buf)

        else:
            raise RuntimeError(f"Could not encode type: {self.data_type}")

        return packed

    @classmethod
    def deserialize_column(cls, column, num_rows, buffer):
        size = num_rows
        if column.data_type == DataType.BOOL:
            data, buffer = chomp(f'>{size}?', buffer, False)
        elif column.data_type == DataType.INT8:
            data, buffer = chomp(f'>{size}B', buffer, False)
        elif column.data_type == DataType.INT32:
            data, buffer = chomp(f'>{size}i', buffer, False)
        elif column.data_type == DataType.DATE:
            data, buffer = chomp(f'>{size}i', buffer, False)
        elif column.data_type == DataType.STR:
            # strings, being annoying
            # for now, unpack all strings to 8 bytes (lol)
            #   and stop @ 0x00
            data = []
            for i in range(size):
                val, buffer = chomp('>8s', buffer)
                val_s = val.decode().strip("\x00")
                data.append(val_s)
        else:
            raise RuntimeError(f"Could not encode type: {column.data_type}")

        return data, buffer


class Table(object):
    def __init__(self, columns):
        self.columns = columns

        # find the sort column if one is specified
        sort_column = None
        for i, column in enumerate(columns):
            do_sort = column.is_sorted == DataSorting.SORTED
            if do_sort and sort_column:
                raise RuntimeError("Cannot sort by more than one column")
            elif do_sort:
                sort_column = column

        numbers = list(range(len(sort_column.data)))
        sort_index = sort_together(sort_column.data, numbers)

        for column in columns:
            column.sort_by(sort_index)

    def describe(self):
        for col in self.columns:
            print(f"Column {col.name}")
            print(f"  - Type={col.data_type} ({col.data_type.value})")
            print(f"  - Encoding={col.encoding} ({col.encoding.value})")
            print(f"  - Sorted?={col.is_sorted} ({col.is_sorted.value})")
            print(f"  - Size={col.size()}")
            print(f"  - Data={list(col.data[0:10])}")

    def magic_number(self):
        return struct.pack(">4s", MAGIC_NUMBER)

    def serialize_header(self):
        packed_bytes = bytearray()

        num_columns = len(self.columns)
        packed_bytes.extend(struct.pack(">i", num_columns))

        if num_columns == 0:
            num_rows = 0
        else:
            num_rows = self.columns[0].size()

        packed_bytes.extend(struct.pack(">i", num_rows))

        for column in self.columns:
            header_bytes = column.serialize_header()
            packed_bytes.extend(header_bytes)

        return packed_bytes

    def serialize(self):
        packed_bytes = bytearray()
        packed_bytes.extend(self.magic_number())
        packed_bytes.extend(self.serialize_header())

        for column in self.columns:
            column_bytes = column.serialize_column()
            packed_bytes.extend(column_bytes)

        return packed_bytes

    @classmethod
    def deserialize_header(cls, buffer):
        magic_number, buffer = chomp('>4s', buffer)

        if magic_number != MAGIC_NUMBER:
            raise RuntimeError(f"Bad magic number: {magic_number}")

        num_columns, buffer = chomp('>i', buffer)
        num_rows, buffer = chomp('>i', buffer)
        return num_columns, num_rows, buffer

    @classmethod
    def deserialize(cls, buffer):
        num_columns, num_rows, buffer = cls.deserialize_header(buffer)

        columns = []
        for i in range(num_columns):
            column, buffer = Column.deserialize_header(buffer)
            columns.append(column)

        for column in columns:
            column_data, buffer = Column.deserialize_column(
                column,
                num_rows,
                buffer
            )
            column.data = column_data

        return Table(columns)
