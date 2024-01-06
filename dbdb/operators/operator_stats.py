
import time
from pympler import asizeof


STATE_WAITING = "waiting"
STATE_RUNNING = "running"
STATE_DONE = "done"

SIZE_CACHE = {}
STAT_CALLBACK = None


def set_stats_callback(function):
    global STAT_CALLBACK
    STAT_CALLBACK = function


class OperatorStats:
    def __init__(self, operator_id, operator_type):
        self.operator_id = operator_id
        self.operator_type = operator_type

        self.rows_processed = 0
        self.rows_emitted = 0
        self.bytes_processed = 0
        self.bytes_emmitted = 0

        self.state = STATE_WAITING
        self.start_time = None
        self.end_time = None
        self.custom_stats = {}

        self.size_cache_hits = 0

    def update_start_running(self):
        self.state = STATE_WAITING

        if (STAT_CALLBACK):
            STAT_CALLBACK("waiting", self.get_stats())

    def update_done_running(self):
        self.state = STATE_DONE
        self.end_time = time.time()

        if (STAT_CALLBACK):
            STAT_CALLBACK("done", self.get_stats())

    def _get_elapsed_time(self):
        if self.start_time is None:
            return 0
        elif self.end_time is None:
            return time.time() - self.start_time
        else:
            return self.end_time - self.start_time

    def _get_bytes_per_sec(self):
        elapsed = self._get_elapsed_time()
        if elapsed == 0:
            return 0

        return self.rows_processed / elapsed

    def _get_rows_per_sec(self):
        elapsed = self._get_elapsed_time()
        if elapsed == 0:
            return 0

        return self.rows_processed / elapsed

    def _get_size_of_row(self, row):
        # this is a fairly heavy-handed way to do this, but w/e
        # cache it b/c in practice is is slow asf
        row_id = id(row)
        if row_id in SIZE_CACHE:
            return SIZE_CACHE[row_id]

        size = asizeof.asizeof(row)
        SIZE_CACHE[row_id] = size

        return size

    def update_row_processed(self, row):
        if self.start_time is None:
            self.start_time = time.time()

        self.state = STATE_RUNNING

        self.rows_processed += 1
        # self.bytes_processed += self._get_size_of_row(row)

        if (STAT_CALLBACK):
            STAT_CALLBACK("processing", self.get_stats())

    def update_row_emitted(self, row):
        self.rows_emitted += 1
        # self.bytes_emmitted += self._get_size_of_row(row)

        if (STAT_CALLBACK):
            STAT_CALLBACK("processing", self.get_stats())

    def update_custom_stats(self, stats_dict):
        self.custom_stats.update(stats_dict)

        if (STAT_CALLBACK):
            STAT_CALLBACK("processing", self.get_stats())

    def push_event(self, event_name, name, data):
        if (STAT_CALLBACK):
            # TODO : this third arg sucks
            STAT_CALLBACK(name, data, event_name=event_name)

    def get_stats(self):
        return {
            "operator_id": self.operator_id,
            "operator_type": self.operator_type,

            "rows_processed": self.rows_processed,
            "rows_emitted": self.rows_emitted,
            "bytes_processed": self.bytes_processed,
            "bytes_emmitted": self.bytes_emmitted,
            "elapsed_time": self._get_elapsed_time(),
            "bytes_per_sec": self._get_bytes_per_sec(),
            "rows_per_sec": self._get_rows_per_sec(),
            "state": self.state,
            "custom": self.custom_stats,
        }
