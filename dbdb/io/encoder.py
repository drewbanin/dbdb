
from dbdb.io.types import DataEncoding, DataType
from dbdb.io import compressor
from itertools import chain
import struct


# In bytes
# PAGE_SIZE = 8000
PAGE_SIZE = 8000


def rle_encode(iterable, max_repeats=255):
    # Returns a bytearray containing a representation of the RLE
    # encoded data.

    index = 0
    encoded = []
    while index < len(iterable):
        repeated = 1
        this_element = iterable[index]

        for j, next_element in enumerate(iterable[index + 1:]):
            if next_element == this_element and repeated < max_repeats:
                repeated += 1
            else:
                break

        index = index + repeated
        encoded.append((repeated, this_element))
    return encoded


def encode_null_bitmap(data):
    # Big idea: create a bitmap where each bit indicates if the value is
    # null or present. Null elements are popped out of the dataset and are
    # not represented in the data page in this file format

    bitmap = bytearray()
    without_nulls = []

    # TODO: Actually RLE this data... that feels hard and confusing haha
    # null_bitfield = [0 if el is None else 1 for el in data]

    byte_size = 8
    for byte_index in range(0, len(data), byte_size):
        block = data[byte_index:byte_index+byte_size]

        byte = 0
        for bit_index, element in enumerate(block):
            present = (element is not None)
            if present:
                place = 2 ** bit_index
                byte = byte | place
                without_nulls.append(element)

        (byte_p,) = struct.pack(">B", byte)
        bitmap.append(byte_p)

    return bitmap, without_nulls


def decode_null_bitmap(bitfield_buffer, data, num_records):
    with_nulls = []

    bitfield_bytes = struct.iter_unpack(">B", bitfield_buffer)

    byte_size = 8
    element_index = 0
    elements_created = 0
    for (byte,) in bitfield_bytes:
        for bit_index in range(byte_size):
            place = 2 ** bit_index
            present = (byte & place)

            elements_created += 1
            if present:
                with_nulls.append(data[element_index])
                element_index += 1
            else:
                with_nulls.append(None)

            # Make sure that we don't encode Nulls at the end of the buffer
            # for unset bits
            if elements_created >= num_records:
                break

    return with_nulls


class DataEncoder:
    def __init__(self, column_info):
        self.column_info = column_info

    @property
    def col_type(self):
        return self.column_info.column_type

    def chunk_to_pages(self, data):
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
        i = 0
        while i < len(buffer):
            # Read number of bytes in page
            (page_size, ) = struct.unpack_from('>i', buffer, offset=i)

            # Advance past page_size
            page_start = i + 4
            page_end = page_start + page_size
            # Read page_size_p bytes from buffer. This is a page

            # TODO: Will this work with a file pointer? prolly now?
            # can we read from a buffer?
            compressed_page_data = buffer[page_start:page_end]

            decompressed = self.decompress_page(compressed_page_data)
            yield decompressed

            i = page_end

    def _encode(self, page):
        raise NotImplementedError()

    def _decode(self, page):
        raise NotImplementedError()

    def compress_page(self, page):
        compression_type = self.column_info.compression
        return compressor.compress(compression_type, page)

    def decompress_page(self, page):
        compression_type = self.column_info.compression
        return compressor.decompress(compression_type, page)

    def format_page(self, pages):
        # Pages is a list of bytearrays
        # Write page header here....
        # Should we be collecting stats as we encode? probably..
        buffer = bytearray()

        for page in pages:
            # Compress first so that we know the page size
            compressed = self.compress_page(page)

            page_size = len(compressed)
            page_size_p = struct.pack('>i', page_size)
            buffer.extend(page_size_p)
            buffer.extend(compressed)

        return buffer

    def encode_bitfield(self, bitfield_buffer, data_buffer):
        # bitfield_buffer is a packed bytearray of present bits
        # data_buffer is a packed bytearray of data
        # Idea: store an int32 (dumb) representing the bitfield...
        #   how many slots does that give us? int32 = 32 bits... so that's not good...
        #   but: we do want to RLE encode this data... but in the worst case there is
        #   going to be one bit per entry.... so i guess the number of bytes needed
        #   is actually going to be $data_length / 8? I guess we can swing that... sucks tho
        # Alternatively: can just store the lenght of the bitfield in bytes as a header. prolly
        # the right move for the time being.... costs 4 bytes but is way simpler...
        buffer = bytearray()

        bitfield_size = len(bitfield_buffer)
        bitfield_size_p = struct.pack(">i", bitfield_size)

        buffer.extend(bitfield_size_p)
        buffer.extend(bitfield_buffer)
        buffer.extend(data_buffer)
        return buffer

    def decode_bitfield(self, buffer):
        # first int32 is size of bitfield
        # then bitfield
        # then data

        (bitfield_size,) = struct.unpack_from(">i", buffer, 0)
        bitfield_buffer = buffer[4:4+bitfield_size]
        data_buffer = buffer[4+bitfield_size:]

        return bitfield_buffer, data_buffer

    def encode(self, data):
        self.validate()

        chunked = self.chunk_to_pages(data)

        pages = []
        for page_data in chunked:
            bitfield, present_data = encode_null_bitmap(page_data)
            encoded = self._encode(present_data)
            encoded_with_bitfield = self.encode_bitfield(bitfield, encoded)
            pages.append(encoded_with_bitfield)

        return self.format_page(pages)

    def decode(self, num_records, buffer):
        self.validate()

        flat = []
        for page in self.from_pages(buffer):
            bitfield_buffer, data_buffer = self.decode_bitfield(page)
            decoded = self._decode(data_buffer)
            # TODO : I don't think i did this right... it should be #
            # of records in a page, not number of records in one column.
            with_nulls = decode_null_bitmap(
                bitfield_buffer,
                decoded,
                num_records
            )
            flat.extend(with_nulls)

        return flat

    def iter_pages(self, fh, start, end):
        pos = start
        while pos < end:
            fh.seek(pos)
            # Read number of bytes in page
            buffer = fh.read(4)
            (page_size, ) = struct.unpack('>i', buffer)

            # Advance past page_size
            buffer = fh.read(page_size)

            # Kind of a hack to make sure that other iterators which
            # use this file handle don't throw us off of our pointer
            # TODO: Would it be smarter to use one file handler per
            # column? That might be better if the data is coming from
            # a remote system over the network....
            pos = fh.tell()

            decompressed = self.decompress_page(buffer)
            yield decompressed

    def iter_decode(self, fh, num_records, start, end):
        self.validate()

        for page in self.iter_pages(fh, start, end):
            bitfield_buffer, data_buffer = self.decode_bitfield(page)
            decoded = self._decode(data_buffer)
            with_nulls = decode_null_bitmap(
                bitfield_buffer,
                decoded,
                num_records
            )
            yield from with_nulls

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
                # Store up to 255 repeated values... if we uncapped this
                # then the number of repeated elements in the RLE encoding
                # would exceed the amount of data we can store in the counter
                # byte. That would be ok if we used an int32, but it would take
                # up more space than we want, especially for random data
                if next_element == this_element and repeated < 255:
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


def iter_decode(fh, column_info, num_records, start, end):
    encoder = get_encoder(column_info)
    yield from encoder.iter_decode(fh, num_records, start, end)
