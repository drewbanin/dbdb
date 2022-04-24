from dbdb.io.types import DataCompression, DataType
import zlib


class Compression:
    def compress(self, buffer):
        raise NotImplementedError()

    def decompress(self, buffer):
        raise NotImplementedError()


class RawCompression:
    def compress(self, buffer):
        return buffer

    def decompress(self, buffer):
        return buffer


class ZLibCompression(Compression):
    def compress(self, buffer):
        return zlib.compress(buffer)

    def decompress(self, buffer):
        return zlib.decompress(buffer)


lookup = {
    DataCompression.RAW: RawCompression,
    DataCompression.ZLIB: ZLibCompression,
}


def get_compressor(compression):
    compressor = lookup.get(compression)
    if compressor is None:
        raise RuntimeError(f"Cannot use compressor {compression}")

    return compressor()


def compress(compression, data):
    compressor = get_compressor(compression)
    return compressor.compress(data)


def decompress(compression, data):
    compressor = get_compressor(compression)
    return compressor.decompress(data)
