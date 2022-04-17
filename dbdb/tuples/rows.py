
from collections import namedtuple
import tabulate


class Rows:
    def __init__(self, fields, iterator):
        self.fields = fields
        self.Row = namedtuple('Row', self.fields)

        self.data = None

        self.iterator = iterator

    def __iter__(self):
        return self

    def __next__(self):
        record = next(self.iterator)
        return self.Row._make(record)

    def new(self, iterator):
        return Rows(self.fields, iterator)

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

        combined = row_objs[0].fields
        for row in row_objs[1:]:
            combined += row.fields

        return Rows(combined, iterator)

    @classmethod
    def merge_rows(cls, ltuple, rtuple):
        fields = ltuple._fields + rtuple._fields
        Row = namedtuple('Row', fields)

        return Row(*ltuple, *rtuple)

    def materialize(self):
        if not self.data:
            self.data = [self.Row._make(row) for row in self.iterator]

        return self.data

    def display(self):
        # We need to materialize the iterator otherwise
        # printing the table will consume all rows :/
        data = self.materialize()
        tbl = tabulate.tabulate(
            data,
            headers=self.fields,
            tablefmt='presto'
        )
        print(tbl)

        return self.new(iter(data))
