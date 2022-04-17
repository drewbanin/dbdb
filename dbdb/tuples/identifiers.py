
class TableIdentifier:
    def __init__(self, database=None, schema=None, relation=None):
        self.database = database
        self.schema = schema
        self.relation = relation

    def scope(self, name):
        return f'{str(self)}'

    @classmethod
    def new(cls, ident_str):
        parts = ident_str.split(".")
        return cls(*parts)

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
        return cls(None, name)

    @classmethod
    def columns_from(cls, table_identifier, column_names):
        return [
            cls(table_identifier, name)
            for name in column_names
        ]

    def __str__(self):
        return f"{self.table_identifier}.{self.name}"

    def __repr__(self):
        return self.__str__()
