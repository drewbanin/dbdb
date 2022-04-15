
"""
Describe file format here...
"""


import struct
from dbdb.files.types import (
    DataType,
    DataEncoding,
    DataSorting
)

from dbdb.files import constants
from dbdb.files import encoder, compressor


def sort_together(sort_index, to_sort):
    return [i for _, i in sorted(zip(sort_index, to_sort))]


def chomp(pack_s, buffer, unpack_single=True):
    size = struct.calcsize(pack_s)
    res = struct.unpack(pack_s, buffer[:size])
    if len(res) == 1 and unpack_single:
        res = res[0]
    return res, buffer[size:]


def pack(self, buffers):
    to_ret = bytearray()

    for buf in buffers:
        to_ret.extend(buf)

    return to_ret


class ColumnInfo(object):
    def __init__(
        self,
        column_type,
        column_name,

        encoding=None,
        sorting=None,
        column_width=None,
        column_offset=None
    ):
        self.column_type = column_type

        if self.column_type == DataType.STR and column_width > 0:
            self.column_width = column_width
        elif self.column_type == DataType.STR:
            self.column_width = DataType.DEFAULT_STRING_SIZE
        else:
            self.column_width = 0

        self.encoding = encoding or DataEncoding.RAW
        self.sorting = sorting or DataSorting.UNSORTED

        # TODO: Column compression?

        self.column_offset = column_offset
        self.column_name = column_name

    def serialize(self):
        return pack([
            # data type
            struct.pack(">c", self.column_type.as_byte()),

            # data type width
            struct.pack(">i", self.column_width),

            # column encoding
            struct.pack(">c", self.encoding.as_byte()),

            # Column sorting
            struct.pack(">?", self.sorting.value),

            # column data offset in file
            struct.pack(">i", self.column_offset),

            # column name
            struct.pack(">16s", bytes(self.column_name, 'ascii')),
        ])

    @classmethod
    def deserialize(cls, buffer):
        # data type
        column_type, buffer = chomp(">c", buffer)

        # data type width
        column_width = chomp(">i", buffer)

        # column encoding
        encoding, buffer = chomp(">c", buffer)

        # Column sorting
        sorting, buffer = chomp(">?", buffer)

        # column data offset in file
        column_offset, buffer = chomp(">i", buffer)

        # column name
        column_name, buffer = chomp(">16s", buffer)
        column_name = column_name.decode('ascii').strip('\x00')

        return cls(
            column_type=DataType(column_type),
            column_width=column_width,
            encoding=DataEncoding(encoding),
            sorting=DataSorting(sorting),
            column_offset=column_offset,
            column_name=column_name
        )

    def serialize_stats(self, min_val, max_val):
        if self.column_type.supports_column_stats():
            p_min_max = struct.pack(">ii", min_val, max_val)
        else:
            p_min_max = struct.pack(">8x")

        return p_min_max

    def is_sorted(self):
        return self.sorting == DataSorting.SORTED

    def describe(self):
        print(f"Column {self.column_name}")
        print(f"  - Type={self.column_type} ({self.column_type.value})")
        print(f"  - Encoding={self.encoding} ({self.encoding.value})")
        print(f"  - Sorted?={self.sorting} ({self.sorting.value})")


class ColumnData(object):
    def __init__(self, data):
        self.data = data

        self.serialize_buffer = bytearray()

    def sort_by(self, sort_index):
        # this mutates the order of self.data
        self.data = sort_together(sort_index, self.data)

    def size(self):
        return len(self.data)

    def serialize(self, column_info):
        # Can we compress here? Does that have to happen inside the encoder?
        # Don't think i can just compress this blob b/c i won't know where
        # the page starts and ends! I think that I should compress/decompress
        # each page, but retain the uncompressed column header
        encoded = encoder.encode(column_info, self.data)

        # compressed = compressor.compress(column_info, encoded)
        return encoded

    @classmethod
    def deserialize(cls, column_info, num_records, buffer):
        # decompressed = compressor.decompress(column_info, decoded)
        decoded = encoder.decode(column_info, num_records, buffer)
        return decoded

    def describe(self):
        print(f"  - Size={self.size()}")
        print(f"  - Data={list(self.data[0:10])}")


class Column(object):
    def __init__(self, column_info, column_data):
        self.column_info = column_info
        self.column_data = column_data

    def is_sorted(self):
        return self.column_info.is_sorted()

    def sort_by(self, sort_index):
        self.column_data.sort_by(sort_index)

    def size(self):
        return self.column_data.size()

    def serialize_data(self):
        return self.column_data.serialize(self.column_info)

    def serialize_block_header(self):
        packed_bytes = bytearray()

        # Column start byte - not used, but helpful in hexdump
        p_column_ident = struct.pack(">1s", constants.COLUMN_IDENT)
        packed_bytes.extend(p_column_ident)

        # Min and max of full dataset
        min_val = min(self.data)
        max_val = max(self.data)
        p_min_max = self._serialize_stats(min_val, max_val)
        packed_bytes.extend(p_min_max)

        return packed_bytes

    def describe(self):
        self.column_info.describe()
        self.column_data.describe()

    @classmethod
    def new(cls, **kwargs):
        data = kwargs.pop('data', None)

        return cls(
            column_info=ColumnInfo(**kwargs),
            column_data=ColumnData(data)
        )


class Table(object):
    def __init__(self, columns):
        self.columns = columns

        sort_column = self.get_sort_column(columns)
        self.sort_columns_in_place(sort_column)

    def sort_columns_in_place(self, sort_column):
        if sort_column is None:
            return

        numbers = list(range(sort_column.size()))
        sort_index = sort_together(sort_column.column_data.data, numbers)

        for column in self.columns:
            column.sort_by(sort_index)

    def get_sort_column(self, columns):
        # find the sort column if one is specified
        sort_column = None
        for i, column in enumerate(columns):
            do_sort = column.is_sorted()
            if do_sort and sort_column:
                raise RuntimeError("Cannot sort by more than one column")
            elif do_sort:
                sort_column = column

        return sort_column

    def describe(self):
        for col in self.columns:
            col.describe()

    def magic_number(self):
        return struct.pack(">4s", constants.MAGIC_NUMBER)

    def serialize_header(self):
        # TODO: You stopped here. Columns are encodable, we just need to
        # wrap up the column headers and figure out what the table header
        # looks like...
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
        # TODO : Prep table (encode + compress) first so that
        #        we can actually know what the column offset is

        raw_bytes = {}
        for column in self.columns:
            buffer = column.serialize_data()

        packed_bytes = pack([
            # magic number
            self.magic_number(),

            # table header
            self.serialize_header()
        ])

        for column in self.columns:
            block_header = column.serialize_block_header()
            packed_bytes.extend(block_header)

            column_bytes = column.serialize_column()
            packed_bytes.extend(column_bytes)

        return packed_bytes

    @classmethod
    def deserialize_header(cls, buffer):
        magic_number, buffer = chomp('>4s', buffer)

        if magic_number != constants.MAGIC_NUMBER:
            raise RuntimeError(f"Bad magic number: {magic_number}")

        num_columns, buffer = chomp('>i', buffer)
        num_rows, buffer = chomp('>i', buffer)
        return num_columns, num_rows, buffer

    @classmethod
    def deserialize(cls, buffer):
        num_columns, num_rows, buffer = cls.deserialize_header(buffer)

        column_header_info = []
        # We only have partial info so far...
        # Alternatively, could jump ahead to column data?
        # Maybe with a pointer into the buffer?
        # not sure...
        for i in range(num_columns):
            column_info, buffer = Column.deserialize_header(buffer)
            column_header_info.append(column_info)

        for column_info in column_header_info:
            column_data, buffer = Column.deserialize_column(
                column,
                num_rows,
                buffer
            )
            column.data = column_data

        return Table(columns)
