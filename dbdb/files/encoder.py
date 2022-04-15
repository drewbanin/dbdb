
from dbdb.files.types import DataEncoding, DataType
from itertools import chain
import struct


# In bytes
# PAGE_SIZE = 8000
PAGE_SIZE = 50


class DataEncoder:
    def __init__(self, column_info):
        self.column_info = column_info

    @property
    def col_type(self):
        return self.column_info.column_type

    def as_pages(self, data):
        column_size = DataType.size(
            self.column_info.column_type,
            self.column_info.column_width
        )

        values_per_page = int(PAGE_SIZE / column_size)

        pages = []
        for i in range(0, len(data), values_per_page):
            page = data[i:i+values_per_page]
            pages.append(page)

        return pages

    def from_pages(self, buffer):
        page_buffers = []

        i = 0
        while i < len(buffer):
            # Read number of bytes in page
            (page_size, ) = struct.unpack_from('>i', buffer, offset=i)

            # Advance past page_size
            page_start = i + 4
            page_end = page_start + page_size
            # Read page_size_p bytes from buffer. This is a page
            page_data = buffer[page_start:page_end]
            page_buffers.append(page_data)

            i = page_end

        return page_buffers

    def _encode(self, page):
        raise NotImplementedError()

    def _decode(self, page):
        raise NotImplementedError()

    def format_page(self, pages):
        # Pages is a list of bytearrays
        # Write page header here....
        # Should we be collecting stats as we encode? probably..
        buffer = bytearray()

        for page in pages:
            page_size = len(page)
            page_size_p = struct.pack('>i', page_size)
            buffer.extend(page_size_p)

            buffer.extend(page)

        return buffer

    def encode(self, data):
        self.validate()

        pages = self.as_pages(data)
        return self.format_page([self._encode(page) for page in pages])

    def decode(self, num_records, buffer):
        self.validate()

        pages = self.from_pages(buffer)
        flat = chain.from_iterable([self._decode(page) for page in pages])
        return list(flat)

    def valid_type(self):
        return True

    def validate(self):
        if self.valid_type():
            return

        encoder = type(self).__name__
        raise RuntimeError(
            f"Cannot encode data of type {self.col_type} with encoder "
            f"{encoder}"
        )


class RawEncoder(DataEncoder):
    def _encode(self, data):
        buffer = bytearray()

        column_type = self.column_info.column_type
        pack_string = DataType.pack_string(
            column_type,
            self.column_info.column_width
        )
        pack_f = f'>{pack_string}'

        for value in data:
            encodable = DataType.as_bytes(column_type, value)
            packed = struct.pack(pack_f, encodable)
            buffer.extend(packed)

        return buffer

    def _decode(self, page):
        column_type = self.column_info.column_type
        pack_string = DataType.pack_string(
            column_type,
            self.column_info.column_width
        )
        pack_f = f'>{pack_string}'

        unpacked = struct.iter_unpack(pack_f, page)
        return [el[0] for el in unpacked]


class RunLengthEncoder(DataEncoder):
    def valid_type(self):
        return self.col_type in (
            DataType.INT8,
            DataType.INT32,
            DataType.DATE,
        )

    def _encode(self, data):
        buffer = bytearray()

        # we can use a single byte because we know that the page
        # is 8kb. If that changes (or is configurable) we might
        # need to update this
        pack_string = DataType.pack_string(
            self.column_info.column_type,
            self.column_info.column_width
        )
        pack_f = f'>B{pack_string}'

        # Loop over page and encode values
        i = 0

        num_records = len(data)
        while i < num_records:
            repeated = 1
            this_element = data[i]

            for j in range(1, num_records - i):
                next_element = data[i + j]
                if next_element == this_element:
                    repeated += 1
                else:
                    break

            i = i + repeated

            packed = struct.pack(pack_f, repeated, this_element)
            buffer.extend(packed)

        return buffer

    def _decode(self, page):
        column_type = self.column_info.column_type
        pack_string = DataType.pack_string(
            column_type,
            self.column_info.column_width
        )
        pack_f = f'>B{pack_string}'

        unpacked = struct.iter_unpack(pack_f, page)

        res = []
        for repeat, value in unpacked:
            res += [value] * repeat

        return res


class DeltaEncoder(DataEncoder):
    def valid_type(self):
        return self.col_type in (
            DataType.INT8,
            DataType.INT32,
            DataType.DATE,
        )

    def _encode(self, data):
        buffer = bytearray()

        if len(data) == 0:
            return buffer

        # we can use a single byte because we know that the page
        # is 8kb. If that changes (or is configurable) we might
        # need to update this
        pack_string = DataType.pack_string(
            self.column_info.column_type,
            self.column_info.column_width
        )
        pack_f = f'>{pack_string}'

        # Loop over page and encode values
        i = 0

        last = 0
        while i < len(data):
            val = data[i] - last
            last = data[i]

            packed = struct.pack(pack_f, val)
            buffer.extend(packed)

            i += 1

        # Observation: our deltas are still ints, so... lol
        # did this help?
        # Think i need to figure out Simple-8b
        return buffer

    def _decode(self, page):
        column_type = self.column_info.column_type
        pack_string = DataType.pack_string(
            column_type,
            self.column_info.column_width
        )
        pack_f = f'>{pack_string}'

        unpacked = struct.iter_unpack(pack_f, page)

        res = []
        last = 0
        for (value,) in unpacked:
            unpacked = value + last
            res.append(unpacked)
            last = unpacked

        return res


class DictionaryEncoder(DataEncoder):
    def valid_type(self):
        return self.col_type in (
            DataType.STR,
        )

    def _pack_header(self, dictionary):
        dict_buffer = bytearray()

        # Construct dictionary first so we know the size
        for val in dictionary:
            # Pack in a null bit for string terminal
            pack_f = f'>{len(val)}sc'
            encodable = DataType.as_bytes(self.column_info.column_type, val)
            packed = struct.pack(pack_f, encodable, b'\x00')
            dict_buffer.extend(packed)

        buffer = bytearray()
        size_p = struct.pack('>B', len(dict_buffer))

        buffer.extend(size_p)
        buffer.extend(dict_buffer)

        return buffer

    def _unpack_header(self, page):
        (dict_length,) = struct.unpack_from('>B', page, 0)

        # read dict_length bytes and split on null bytes
        buffer = page[1:1+dict_length]

        pack_f = f'>{dict_length}s'
        (dict_entries_s,) = struct.unpack(pack_f, buffer)

        null = b"\x00"
        dict_entries = dict_entries_s.strip(null).split(null)

        dictionary = {}
        for i, entry in enumerate(dict_entries):
            entry_s = entry.decode("ascii")
            dictionary[i] = entry_s

        rest = page[1+dict_length:]
        return dictionary, rest

    def get_dictionary(self, data):
        uniques = {val for val in data}
        return list(sorted(uniques))

    def _encode(self, data):
        buffer = bytearray()

        if len(data) == 0:
            return buffer

        dictionary = self.get_dictionary(data)
        packed_header = self._pack_header(dictionary)
        buffer.extend(packed_header)

        # Front of buffer contains our dictionary
        # Dictionary is serialized as:
        #   int32
        #   null-terminated string
        # one byte at the front declares # of dict entries

        # We could avoid padding strings if we're ok with dealing
        # with null bytes.... honestly.... would it be so bad if we
        # do that kind of thing in the encoder?

        # TODO: Using an unsigned byte limits how many entries can be in
        # dict. Is this ok? I think it depends on the page size...
        lookup = {val: i for i, val in enumerate(dictionary)}
        for value in data:
            index = lookup[value]
            packed = struct.pack('>B', index)
            buffer.extend(packed)

        return buffer

    def _decode(self, page):
        dictionary, page = self._unpack_header(page)

        unpacked = struct.iter_unpack('>B', page)
        return [dictionary[val] for (val,) in unpacked]


lookup = {
    DataEncoding.RAW: RawEncoder,
    DataEncoding.RUN_LENGTH: RunLengthEncoder,
    DataEncoding.DELTA: DeltaEncoder,
    DataEncoding.DICTIONARY: DictionaryEncoder,
}


def get_encoder(column_info):
    encoding = column_info.encoding

    Encoder = lookup.get(encoding)

    if Encoder is None:
        raise RuntimeError(f"No encoder found for {encoding}")

    return Encoder(column_info)


def encode(column_info, data):
    encoder = get_encoder(column_info)
    return encoder.encode(data)


def decode(column_info, num_records, buffer):
    encoder = get_encoder(column_info)
    return encoder.decode(num_records, buffer)
