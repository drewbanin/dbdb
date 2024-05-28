import struct
from dbdb.io.types import (
    DataType,
    DataEncoding,
    DataCompression,
    DataSorting,
    DEFAULT_STRING_SIZE,
)
from dbdb.logger import logger

from dbdb.io import constants
from dbdb.io import encoder, compressor
from typing import Optional


"""
Describe file format here...
"""


def sort_together(sort_index, to_sort):
    return [i for _, i in sorted(zip(sort_index, to_sort))]


def chomp(pack_s, buffer, unpack_single=True):
    size = struct.calcsize(pack_s)
    res = struct.unpack(pack_s, buffer[:size])
    if len(res) == 1 and unpack_single:
        res = res[0]
    return res, buffer[size:]


def pack(buffers):
    to_ret = bytearray()

    for buf in buffers:
        to_ret.extend(buf)

    return to_ret


class ColumnInfo(object):
    def __init__(
        self,
        column_type: DataType,
        column_name: str,

        encoding: DataEncoding = None,
        sorting: DataSorting = None,
        compression: DataCompression = None,
        column_width: Optional[int] = None,
        column_data_size: Optional[int] = None
    ):
        self.column_type = column_type

        column_width = column_width or 0
        if self.column_type == DataType.STR and column_width > 0:
            self.column_width = column_width
        elif self.column_type == DataType.STR:
            self.column_width = DEFAULT_STRING_SIZE
        else:
            self.column_width = 0

        self.encoding = encoding or DataEncoding.RAW
        self.compression = compression or DataCompression.RAW
        self.sorting = sorting or DataSorting.UNSORTED

        self.column_data_size = column_data_size
        self.column_name = column_name

    @classmethod
    def serialized_size(cls):
        # TODO Ideally this is a constant / not hardcoded...
        # Just calculated as size of stuff in the header...
        return 28

    def serialize(self):
        if self.column_data_size is None:
            raise RuntimeError(
                f"Cannot serialize table header for column {self.column_name}"
                " because column_data_size has not been set!"
            )

        return pack([
            # data type
            struct.pack(">B", self.column_type.value),

            # data type width
            struct.pack(">i", self.column_width),

            # column encoding
            struct.pack(">B", self.encoding.value),

            # column compression
            struct.pack(">B", self.compression.value),

            # Column sorting
            struct.pack(">?", self.sorting.value),

            # column data offset in file
            struct.pack(">i", self.column_data_size),

            # column name
            struct.pack(">16s", bytes(self.column_name, 'ascii')),
        ])

    @classmethod
    def deserialize(cls, buffer):
        # data type
        column_type, buffer = chomp(">B", buffer)

        # data type width
        column_width, buffer = chomp(">i", buffer)

        # column encoding
        encoding, buffer = chomp(">B", buffer)

        # column compression
        compression, buffer = chomp(">B", buffer)

        # Column sorting
        sorting, buffer = chomp(">?", buffer)

        # column data offset in file
        column_data_size, buffer = chomp(">i", buffer)

        # column name
        column_name, buffer = chomp(">16s", buffer)
        column_name = column_name.decode('ascii').strip('\x00')

        column_info = cls(
            column_type=DataType(column_type),
            column_width=column_width,
            encoding=DataEncoding(encoding),
            compression=DataCompression(compression),
            sorting=DataSorting(sorting),
            column_data_size=column_data_size,
            column_name=column_name
        )

        return column_info, buffer

    def _serialize_stats(self):
        if self.column_type.supports_column_stats():
            p_min_max = struct.pack(">ii", min_val, max_val)
        else:
            p_min_max = struct.pack(">8x")

        return p_min_max

    def is_sorted(self):
        return self.sorting == DataSorting.SORTED

    def encoding_name(self):
        pass

    def to_dict(self):

        encoding_lookup = {
            DataEncoding.RAW: "RAW",
            DataEncoding.RUN_LENGTH: "RUN_LENGTH",
            DataEncoding.DELTA: "DELTA",
            DataEncoding.DICTIONARY: "DICTIONARY",
        }

        type_lookup = {
            DataType.BOOL: "BOOL",
            DataType.INT8: "INT_8",
            DataType.INT32: "INT_32",
            DataType.STR: "STRING",
            DataType.DATE: "DATE"
        }

        return {
            "name": self.column_name,
            "type": type_lookup[self.column_type],
            "encoding": encoding_lookup[self.encoding],
            # "compression": self.compression,
            # "sorted": self.sorting
        }

    def describe(self):
        logger.info(f"Column {self.column_name}")
        logger.info(f"  - Type={self.column_type} ({self.column_type.value})")
        logger.info(f"  - Encoding={self.encoding} ({self.encoding.value})")
        logger.info(f"  - Sorted?={self.sorting} ({self.sorting.value})")


class ColumnData(object):
    def __init__(self, data):
        self.data = data

    def sort_by(self, sort_index):
        # this mutates the order of self.data
        self.data = sort_together(sort_index, self.data)

    def size(self):
        return len(self.data)

    # TODO : Handle struct errors here (eg. value too large)
    #        or value of wrong type
    def serialize(self, column_info):
        return encoder.encode(column_info, self.data)

    @classmethod
    def deserialize(cls, column_info, num_records, buffer):
        decoded = encoder.decode(column_info, num_records, buffer)
        return cls(decoded)

    @classmethod
    def read_page(cls, column_info, num_rows, buffer):
        decoder = encoder.iter_decode(column_info, num_rows, buffer)
        for res in decoder:
            yield res

    def describe(self):
        logger.info(f"  - Size={self.size()}")
        logger.info(f"  - Data={list(self.data[0:10])}")


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

    def finalize(self, column_data_size):
        self.column_info.column_data_size = column_data_size

    def serialize_table_header(self):
        return self.column_info.serialize()

    def serialize_block_header(self):
        return bytearray()
        # packed_bytes = bytearray()

        # Column start byte - not used, but helpful in hexdump
        # p_column_ident = struct.pack(">1s", constants.COLUMN_IDENT)
        # packed_bytes.extend(p_column_ident)

        # TODO : Include column stats! Put this somewhere else
        # so that we can call the same func in page data...
        # Min and max of full dataset
        # min_val = min(self.column_data.data)
        # max_val = max(self.column_data.data)
        # p_min_max = self._serialize_stats(min_val, max_val)
        # packed_bytes.extend(p_min_max)
        # return packed_bytes

    def serialize_data_pages(self):
        return self.column_data.serialize(self.column_info)

    def serialize_data(self):
        buffer = bytearray()
        buffer.extend(self.serialize_block_header())
        buffer.extend(self.serialize_data_pages())
        return buffer

    @classmethod
    def deserialize_table_header(cls, buffer):
        column_info, buffer = ColumnInfo.deserialize(buffer)
        return column_info, buffer

    def deserialize_block_header(self, buffer):
        pass

    @classmethod
    def deserialize_data(self, column_info, num_records, buffer):
        return ColumnData.deserialize(
            column_info,
            num_records,
            buffer
        )

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
        packed_bytes = bytearray()

        num_columns = len(self.columns)
        packed_bytes.extend(struct.pack(">i", num_columns))

        if num_columns == 0:
            num_rows = 0
        else:
            num_rows = self.columns[0].size()

        packed_bytes.extend(struct.pack(">i", num_rows))

        for column in self.columns:
            header_bytes = column.serialize_table_header()
            packed_bytes.extend(header_bytes)

        return packed_bytes

    def serialize(self):
        # Serialize data buffers before we serialize the headers so that
        # we can determine column offsets correctly
        data_buffer = bytearray()
        for i, column in enumerate(self.columns):
            # includes block header + page data
            column_buffer = column.serialize_data()
            buflen = len(column_buffer)
            column.finalize(buflen)
            data_buffer.extend(column_buffer)

        packed_bytes = pack([
            # magic number
            self.magic_number(),

            # table header
            self.serialize_header()
        ])

        packed_bytes.extend(data_buffer)

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
    def deserialize_column_headers(cls, num_columns, buffer):
        column_info_list = []
        for i in range(num_columns):
            column_info, buffer = Column.deserialize_table_header(buffer)
            column_info_list.append(column_info)

        return column_info_list, buffer

    @classmethod
    def deserialize(cls, buffer):
        num_columns, num_rows, buffer = cls.deserialize_header(buffer)
        column_info_list, buffer = cls.deserialize_column_headers(num_columns, buffer)

        # buffer is now at the start of the data pages. Find the relevant span
        # in the buffer by picking the bytes between the start of the buffer
        # and the beginning of the next data page
        columns = []
        start = 0
        for column_info in column_info_list:
            end = start + column_info.column_data_size

            data_buffer = buffer[start:end]

            column_data = Column.deserialize_data(
                column_info,
                num_rows,
                data_buffer
            )

            columns.append(Column(
                column_info=column_info,
                column_data=column_data
            ))

            start = end

        return Table(columns)

    @classmethod
    def read_header(cls, fh):
        # TODO: Ideally this is a constant / stored somewhere..
        table_header_size = 12
        buffer = fh.read(table_header_size)
        num_columns, num_rows, buffer = cls.deserialize_header(buffer)

        column_header_size = ColumnInfo.serialized_size()
        bytes_to_read = column_header_size * num_columns

        buffer = fh.read(bytes_to_read)
        column_info_list, buffer = cls.deserialize_column_headers(
            num_columns,
            buffer
        )

        return column_info_list, num_rows

    @classmethod
    def iter_pages(cls, fh, column, num_records, start, end):
        yield from encoder.iter_decode(fh, column, num_records, start, end)


def read_header(reader):
    with reader.open() as fh:
        column_info_list, num_rows = Table.read_header(fh)

    return column_info_list


def read_pages(reader, columns):
    with reader.open() as fh:
        column_info_list, num_rows = Table.read_header(fh)

        start = 0
        selected = {}
        for column_info in column_info_list:
            end = start + column_info.column_data_size
            if column_info.column_name in columns:
                selected[column_info.column_name] = (start, end, column_info)

            start = end

        data_start = fh.tell()

        # file pointer is now at the end of the column header...
        # start simple, yield one page from each column at a time....
        page_iterators = []
        for col_name in columns:
            (start, end, column) = selected[col_name]
            iterator = Table.iter_pages(
                fh,
                column,
                num_rows,
                data_start + start,
                data_start + end,
            )

            page_iterators.append(iterator)

        yield from zip(*page_iterators)

def infer_type(value):
    if type(value) == int:
        return DataType.INT32
    elif type(value) == float:
        return DataType.FLOAT
    elif type(value) == str:
        return DataType.STR
    elif type(value) == bool:
        return DataType.BOOL
    else:
        raise RuntimeError(f"Cannot infer column type for value: {value} ({type(value)})")
