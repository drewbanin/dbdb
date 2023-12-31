from dbdb.tuples.identifiers import TableIdentifier

import tabulate
from typing import NamedTuple


class RowTuple:
    def __init__(self, fields, data):
        self.fields = fields

        if isinstance(data, RowTuple):
            self.data = data.data
        elif isinstance(data, (tuple, list)):
            self.data = data
        else:
            import ipdb; ipdb.set_trace()
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

    def index(self, index):
        return self.data[index]

    def merge(self, other):
        return RowTuple(
            fields=tuple(list(self.fields) + list(other.fields)),
            data=tuple(list(self.data) + list(other.data))
        )

    def as_tuple(self):
        return tuple(self.data)


class Rows:
    def __init__(self, table, fields, iterator):
        self.table = table
        self.fields = fields
        self.iterator = iterator
        self.data = None

    # def __iter__(self):
    #     return self

    # def __next__(self):
    #     record = next(self.iterator)
    #     return self._make_row(record)

    def __aiter__(self):
        return self

    async def __anext__(self):
        import asyncio
        await asyncio.sleep(0.0)
        record = await self.iterator.__anext__()
        return self._make_row(record)

    def _make_row(self, record):
        return RowTuple(self.fields, record)

    def new(self, iterator):
        return Rows(self.table, self.fields, iterator)

    def nulls(self):
        return (None,) * len(self.fields)

    @classmethod
    def merge(self, row_objs, iterator):
        # TODO : Handle overlapping field names?
        # suffix with _1 or whatever

        if len(row_objs) < 1:
            raise RuntimeError(
                "Cannot merge rowsets from an empty list"
            )

        fields = []
        for row in row_objs:
            for field in row.fields:
                scoped = row.table.field(field.name)
                fields.append(scoped)

        table = TableIdentifier.temporary()
        return Rows(table, fields, iterator)

    async def materialize(self):
        if type(self.iterator).__name__ == 'generator':
            import ipdb; ipdb.set_trace()
        if not self.data:
            self.data = tuple([self._make_row(row) async for row in self.iterator])

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

        print("Table:", self.table)
        tbl = tabulate.tabulate(
            to_print,
            headers=self.fields,
            tablefmt='presto'
        )
        print(tbl)

        return self.new(iter(data))

    @classmethod
    def from_literals(cls, table, fields, data):
        return cls(
            table=table,
            fields=fields,
            iterator=iter(data)
        )
