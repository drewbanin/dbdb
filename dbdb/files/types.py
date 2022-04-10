
import enum


class BinEnum(enum.Enum):
    def as_byte(self):
        return self.value.to_bytes(1, 'big')


class DataType(BinEnum):
    BOOL = 1
    INT8 = 2
    INT32 = 3
    STR = 4
    DATE = 5


class DataEncoding(BinEnum):
    RAW = 1
    RUN_LENGTH = 2
    DELTA = 3
    DICTIONARY = 4


class DataSorting(enum.Flag):
    UNSORTED = False
    SORTED = True
