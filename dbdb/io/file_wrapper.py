
# Simple wrapper around opening and reading files
# Records stats

from contextlib import contextmanager
import time
import asyncio
import os

if os.path.exists("/dbdb-data"):
    DATA_DIR = "/dbdb-data"
else:
    DATA_DIR = "./data"


class FileHandleProxy:
    def __init__(self, fh):
        self.fh = fh

        self.bytes_read = 0
        self.reads = 0

        self.bytes_written = 0
        self.writes = 0

        # Set after initialization for reads
        self.size = 0

    def read_size(self):
        # Initialize file size (for stats)
        self.fh.seek(0, 2)
        self.size = self.fh.tell()
        self.fh.seek(0)

    def read(self, count):
        if count is None:
            raise NotImplementedError()

        # Keep track of scans
        self.bytes_read += count
        self.reads += 1

        return self.fh.read(count)

    def write(self, data):
        if data is None:
            raise RuntimeError("called write() with no data")

        # Keep track of scans
        self.bytes_written += len(data)
        self.writes += 1

        return self.fh.write(data)

    def tell(self):
        return self.fh.tell()

    def seek(self, place):
        return self.fh.seek(place)

    def stats(self):
        return {
            # Reads
            'bytes_read': self.bytes_read,
            'reads': self.reads,

            # Writes
            'bytes_written': self.bytes_written,
            'writes': self.writes,

            # Progress (reads)
            'bytes_total': self.size,
            'bytes_read_pct': self.bytes_read / self.size,
        }


class FileReader:
    def __init__(self, table_ref):
        self.table_ref = table_ref
        self.table_path = self.make_path(table_ref)
        self.handle = None

    @classmethod
    def make_path(cls, table_name):
        return f"{DATA_DIR}/{table_name}.dumb"

    @contextmanager
    def open(self, mode='rb'):
        with open(self.table_path, mode) as fh:
            self.handle = FileHandleProxy(fh)
            if mode == 'rb':
                self.handle.read_size()
            yield self.handle

    def read(self, count=None):
        self.handle.read(count)

    def write(self, data):
        self.handle.write(data)

    def stats(self):
        return self.handle.stats()
