
import itertools


class Identifier:
    pass


class TableIdentifier(Identifier):
    def __init__(self, database=None, schema=None, name=None, alias=None):
        self.database = database
        self.schema = schema
        self.name = name
        self.alias = alias

    def field(self, field_name):
        return FieldIdentifier(field_name, self)

    def is_match(self, candidate_parts):
        # Zip our parts with candidate parts; determine if they match
        # Reverse both lists so we match from most significant to least
        # If candidate is None but our part is present, that's a match!

        # If a field is aliased, it can no longer be addressed by its name
        can_alias = self.alias is not None and len(candidate_parts) > 0
        if can_alias and candidate_parts[0] == self.alias:
            return True
        elif can_alias:
            return False

        our_parts = self.provided_parts()
        candidate_reversed = candidate_parts[::-1]
        ours_reversed = our_parts[::-1]

        zipped = itertools.zip_longest(candidate_reversed, ours_reversed)
        for (candidate, ours) in zipped:
            if candidate == ours:
                # Match means keep going
                continue
            elif candidate is None:
                # Unset means we hit the end of the scoped name; it's a match
                return True
            else:
                # Non-match means the candidate is not a match
                return False

        return True

    def provided_parts(self):
        parts = [self.database, self.schema, self.name]
        parts = [p for p in parts if p is not None]
        return parts

    @classmethod
    def new(cls, ident_str, alias=None):
        parts = ident_str.split(".")
        rest = 3 - len(parts)
        args = [None] * rest + parts
        return cls(*args, alias=alias)

    @classmethod
    def temporary(cls):
        return cls(database=None, schema=None, name="<temporary>")

    def __str__(self):
        parts = self.provided_parts()
        return ".".join(parts)

    def __repr__(self):
        return self.__str__()


class FieldIdentifier(Identifier):
    def __init__(self, name, parent):
        self.name = name
        self.parent = parent

    def is_match(self, candidate):
        candidate_parts = candidate.split(".")
        assert len(candidate_parts) > 0, f"Got empty candidate: {candidate}"

        candidate_field = candidate_parts.pop()
        if candidate_field != self.name:
            return False

        return self.parent.is_match(candidate_parts)

    def __str__(self):
        parent_qualifier = self.parent.provided_parts()
        qualified = parent_qualifier + [self.name]
        return ".".join(qualified)

    def __repr__(self):
        return self.__str__()


class GlobIdentifier(FieldIdentifier):
    def __init__(self, name, parent=None):
        self.name = name
        self.parent = parent

    def is_match(self, candidate):
        candidate_parts = candidate.split(".")
        assert len(candidate_parts) > 0, f"Got empty candidate: {candidate}"

        # Pop * off of candidate
        candidate_parts.pop()

        if self.parent:
            return self.parent.is_match(candidate_parts)
        else:
            return True

    def __str__(self):
        if self.parent:
            parent_qualifier = self.parent.provided_parts()
            qualified = parent_qualifier + [self.name]
        else:
            qualified = '*'

        return ".".join(qualified)

    def __repr__(self):
        return self.__str__()
