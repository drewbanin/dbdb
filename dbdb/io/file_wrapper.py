
# Simple wrapper around opening and reading files
# Records stats

from contextlib import contextmanager
import time


class FileHandleProxy:
    def __init__(self, fh):
        self.fh = fh

        self.bytes_read = 0
        self.reads = 0

        # Initialize file size (for stats)
        fh.seek(0, 2)
        self.size = fh.tell()
        fh.seek(0)

    def read(self, count):
        print("Blocking read")
        time.sleep(0.5)

        if count is None:
            raise NotImplementedError()

        # Keep track of scans
        self.bytes_read += count
        self.reads += 1

        return self.fh.read(count)

    def tell(self):
        return self.fh.tell()

    def seek(self, place):
        return self.fh.seek(place)

    def stats(self):
        return {
            'bytes_read': f"{self.bytes_read:,}",
            'bytes_read_pct': f"{100 * self.bytes_read / self.size:0.2f}%",
            'bytes_total': f"{self.size:,}",
            'reads': f"{self.reads:,}",
        }


class FileReader:
    def __init__(self, table_ref):
        self.table_ref = table_ref
        self.handle = None

    @contextmanager
    def open(self):
        with open(self.table_ref, 'rb') as fh:
            self.handle = FileHandleProxy(fh)
            yield self.handle

    def read(self, count=None):
        self.handle.read(count)

    def stats(self):
        return self.handle.stats()
