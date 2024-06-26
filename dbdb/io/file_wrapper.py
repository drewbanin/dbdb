# Simple wrapper around opening and reading files
# Records stats

from contextlib import contextmanager
import os
from pathlib import Path

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
            "bytes_read": self.bytes_read,
            "reads": self.reads,
            # Writes
            "bytes_written": self.bytes_written,
            "writes": self.writes,
            # Progress (reads)
            "bytes_total": self.size,
            "bytes_read_pct": self.bytes_read / self.size,
        }


class FileReader:
    def __init__(self, table):
        self.table_ref = str(table)
        self.table_path = self.make_path(table)
        self.handle = None

    @classmethod
    def make_path(cls, table):
        parts = ["database", "schema", "name"]
        path = Path(DATA_DIR)

        for part in parts:
            value = getattr(table, part)
            if part == "name":
                value = f"{value}.dumb"
            elif value is None:
                value = "dbdb"

            path = path / value

        return path

    @contextmanager
    def open(self, mode="rb"):
        if mode == "wb" and not self.table_path.exists():
            dir_path = self.table_path.parent
            dir_path.mkdir(parents=True, exist_ok=True)

        try:
            with open(self.table_path, mode) as fh:
                self.handle = FileHandleProxy(fh)
                if mode == "rb":
                    self.handle.read_size()
                yield self.handle
        except FileNotFoundError:
            table_name = str(self.table_ref)
            raise RuntimeError(f"Table `{table_name}` does not exist")

    def read(self, count=None):
        self.handle.read(count)

    def write(self, data):
        self.handle.write(data)

    def stats(self):
        return self.handle.stats()
