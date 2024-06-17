from dbdb.operators.base import Operator, OperatorConfig
from dbdb.tuples.identifiers import TableIdentifier, FieldIdentifier
from dbdb.tuples.rows import Rows

import itertools

"""
It is extremely dumb to create a new operator to rename fields
in derived scopes... and yet... here we are!

Ex:

with abc as (
    select * from my_table
)

select id  <----  this should be abc.id. not my_table.id
from abc
"""


class RenameScopeOperatorConfig(OperatorConfig):
    def __init__(
        self,
        scope_name,
    ):
        self.scope_name = scope_name


class RenameScopeOperator(Operator):
    Config = RenameScopeOperatorConfig

    def name(self):
        return "Scope"

    async def make_iterator(self, tuples):
        async for row in tuples:
            yield row
        self.stats.update_done_running()

    async def run(self, rows):
        self.stats.update_start_running()
        iterator = self.make_iterator(rows)

        derived_table = TableIdentifier(name=self.config.scope_name)
        mapped_fields = [
            FieldIdentifier(name=field.name, parent=derived_table)
            for field in rows.fields
        ]

        iterator = self.add_exit_check(iterator)
        return Rows(table=derived_table, fields=mapped_fields, iterator=iterator)
