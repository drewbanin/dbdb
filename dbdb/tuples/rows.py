from dbdb.tuples.identifiers import TableIdentifier
from dbdb.logger import logger

import asyncio
import collections
import tabulate


class RowTuple:
    def __init__(self, fields, data):
        self.fields = fields

        if isinstance(data, RowTuple):
            self.data = data.data
        elif isinstance(data, (tuple, list)):
            self.data = data
        else:
            raise RuntimeError(f"bad input to RowTuple: {data}")

    def field(self, name):
        found = None
        for i, field in enumerate(self.fields):
            matched = field == "*" or field.is_match(name)
            if matched and found is not None:
                raise RuntimeError(f"Ambiguous column: {name}")
            elif matched:
                found = i

        if found is None:
            raise RuntimeError(f"field {name} not found in table")

        return self.data[found]

    def iter_values_for_field(self, name):
        for i, field in enumerate(self.fields):
            if field.is_match(name):
                yield self.data[i]

    def has_field(self, name):
        for f in self.fields:
            if field.is_match(f):
                return True
        return False

    def nulls(self):
        return (None,) * len(self.fields)

    def index(self, index):
        return self.data[index]

    def merge(self, other):
        return RowTuple(
            fields=tuple(list(self.fields) + list(other.fields)),
            data=tuple(list(self.data) + list(other.data)),
        )

    def as_tuple(self):
        return tuple(self.data)


class Rows:
    def __init__(self, table, fields, iterator):
        self.table = table
        self.fields = fields
        self.iterator = iterator
        self.data = None

        self.consumers = []
        self.seen = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        record = await self.iterator.__anext__()
        row = self._make_row(record)
        self.seen.append(row)
        return row

    def _make_row(self, record):
        return RowTuple(self.fields, record)

    def new(self, iterator):
        return Rows(self.table, self.fields, iterator)

    def nulls(self):
        return (None,) * len(self.fields)

    def consume(self):
        new_deque = collections.deque()
        for val in self.seen:
            new_deque.append(val)

        self.consumers.append(new_deque)

        async def gen(mydeque):
            while True:
                if not mydeque:
                    try:
                        newval = await self.__anext__()
                    except StopAsyncIteration:
                        break

                    for consumer in self.consumers:
                        consumer.append(newval)

                yield mydeque.popleft()

        return Rows(self.table, self.fields, gen(new_deque))

    async def iter_rows_batches(self, take=10):
        """
        This is non-blocking and relies on another thread/task to
        be consuming the output using consume(). Pretty jank, should fix.
        """

        # Run output consumer in the bg
        rows = []
        status = {"complete": False, "error": None}

        async def bg_consume():
            try:
                consumer = self.consume()
                async for row in consumer:
                    rows.append(row)
                    await asyncio.sleep(0)
            except Exception as e:
                status["error"] = e

            status["complete"] = True

        task = asyncio.create_task(bg_consume())
        index = 0
        while not status["complete"]:
            batch = rows[index : index + take]
            yield batch
            index += len(batch)
            await asyncio.sleep(0)

        if status["error"]:
            raise status["error"]

        try:
            # make asyncio happy that we checked...
            task.exception()

        except asyncio.exceptions.InvalidStateError:
            pass

        # Clean up remaining rows if complete occurs
        # before output is fully consumed
        if index < len(rows):
            yield rows[index:]

    @classmethod
    def merge(self, row_objs, iterator):
        # TODO : Handle overlapping field names?
        # suffix with _1 or whatever

        if len(row_objs) < 1:
            raise RuntimeError("Cannot merge rowsets from an empty list")

        fields = []
        for row in row_objs:
            for field in row.fields:
                scoped = row.table.field(field.name)
                fields.append(scoped)

        table = TableIdentifier.temporary()
        return Rows(table, fields, iterator)

    async def materialize(self):
        if not self.data:
            consumer = self.consume()
            self.data = tuple([self._make_row(row) async for row in consumer])

        return self.data

    async def as_table(self):
        data = await self.materialize()

        raw = tuple([r.data for r in data])
        fields = [f.name for f in self.fields]

        accum = []
        for row in raw:
            as_dict = dict(zip(fields, row))
            accum.append(as_dict)

        return accum

    async def display(self, num_rows=10):
        # We need to materialize the iterator otherwise
        # printing the table will consume all rows :/
        data = await self.materialize()
        raw = tuple([r.data for r in data])

        if num_rows is not None:
            to_print = raw[:num_rows]
        else:
            to_print = raw

        tbl = tabulate.tabulate(to_print, headers=self.fields, tablefmt="presto")
        logger.info(f"TABLE\n{tbl}")

        return self.new(iter(data))

    @classmethod
    def from_literals(cls, table, fields, data):
        return cls(table=table, fields=fields, iterator=iter(data))
