
class TableIdentifier:
    def __init__(self, database=None, schema=None, relation=None):
        self.database = database
        self.schema = schema
        self.relation = relation

    def scope(self, name):
        return FieldIdentifier(self, name)

    @classmethod
    def new(cls, ident_str):
        parts = ident_str.split(".")
        rest = 3 - len(parts)
        args = [None] * rest + parts
        return cls(*args)

    @classmethod
    def temporary(cls):
        return cls(database=None, schema=None, relation="<temporary>")

    def __str__(self):
        parts = [self.database, self.schema, self.relation]
        parts = [p for p in parts if p is not None]
        return ".".join(parts)

    def __repr__(self):
        return self.__str__()


class FieldIdentifier(TableIdentifier):
    def __init__(self, table_identifier, name):
        self.table_identifier = table_identifier
        self.name = name

    @classmethod
    def field(cls, name):
        if '.' in name:
            table, ident = name.split(".", 1)
            table_ident = TableIdentifier(relation=table)
            return cls(table_ident, name)
        else:
            return cls(None, name)

    @classmethod
    def columns_from(cls, table_identifier, column_names):
        return [
            cls(table_identifier, name)
            for name in column_names
        ]

    def is_match(self, candidate):
        if self.name == candidate:
            return True
        elif self.table_identifier is None:
            return False

        candidate_parts = candidate.split(".")

        if len(candidate_parts) == 2:
            table, field = candidate_parts
            return self.name == field and \
                self.table_identifier.relation == table

        elif len(candidate_parts) == 3:
            schema, table, field = candidate_parts
            return self.name == field \
                and self.table_identifier.relation == table \
                and self.table_identifier.schema == schema

        elif len(candidate_parts) == 4:
            database, schema, table, field = candidate_parts
            return self.name == field \
                and self.table_identifier.relation == table \
                and self.table_identifier.schema == schema \
                and self.table_identifier.database == database

        return False

    def __str__(self):
        return f"{self.table_identifier}.{self.name}"

    def __repr__(self):
        return self.__str__()
