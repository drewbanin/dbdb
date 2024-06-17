import enum

DEFAULT_STRING_SIZE = 255


class BinEnum(enum.Enum):
    def as_byte(self):
        return self.value.to_bytes(1, "big")


class DataType(BinEnum):
    BOOL = 1
    INT8 = 2
    INT32 = 3
    STR = 4
    DATE = 5
    FLOAT64 = 6

    @classmethod
    def size(cls, data_type, data_width=None):
        if data_type == DataType.BOOL:
            return 1
        elif data_type == DataType.INT8:
            return 1
        elif data_type == DataType.INT32:
            return 4
        elif data_type == DataType.STR:
            return data_width
        elif data_type == DataType.DATE:
            return 4
        elif data_type == DataType.FLOAT64:
            return 8

    @classmethod
    def pack_string(cls, data_type, data_width=None):
        packed_size = cls.size(data_type, data_width)

        if data_type == DataType.BOOL:
            pack_f = "?"
        elif data_type == DataType.INT8:
            pack_f = "b"
        elif data_type == DataType.INT32:
            pack_f = "i"
        elif data_type == DataType.STR:
            pack_f = f"{packed_size}s"
        elif data_type == DataType.DATE:
            pack_f = "I"
        elif data_type == DataType.FLOAT64:
            pack_f = "d"

        return pack_f

    @classmethod
    def as_bytes(cls, data_type, value):
        if data_type == DataType.STR:
            return bytes(value, "ascii")
        else:
            return value


class DataEncoding(BinEnum):
    RAW = 1
    RUN_LENGTH = 2
    DELTA = 3
    DICTIONARY = 4


class DataCompression(BinEnum):
    RAW = 1
    ZLIB = 2


class DataSorting(enum.Flag):
    UNSORTED = False
    SORTED = True
