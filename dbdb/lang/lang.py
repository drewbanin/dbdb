
import pyparsing as pp


class SelectQuery(object):
    def __init__(self):
        self.projections = []
        self.source = None
        self.filters = []
        self.limit = None

    def add_projections(self, projections):
        for proj in projections:
            self.projections.append(proj)

    def add_source(self, source):
        self.source = source

    def add_filter(self, filter_):
        self.filters.append(filter_)

    def add_limit(self, limit):
        self.limit = limit

    def describe(self):
        print(f"Source table: {self.source}")
        print(f" Projections: {self.projections}")
        print(f"     Filters: {self.filters}")
        print(f"       Limit: {self.limit}")


IDENT = pp.Word(pp.srange("[a-zA-Z_]"), pp.srange("[a-zA-Z0-9_"))
GRAMMAR = (
    pp.CaselessKeyword("SELECT") +

    # columns
    pp.Group(
        IDENT +
        pp.ZeroOrMore(
            pp.Suppress(pp.Literal(",")) +
            IDENT
        )
    ).set_results_name('columns') +

    # from clause
    pp.CaselessKeyword("FROM") +
    IDENT.set_results_name("from") +

    # limit
    pp.Optional(
        pp.CaselessKeyword("LIMIT") +
        pp.Word(pp.nums)
        .set_results_name("limit")
        .setParseAction(lambda x: int(x[0]))
    )
)


def parse_query(query):
    """
    Expected format
        SELECT
            <column>, [<column>, ...]
        FROM <table path>
        [LIMIT <count>]
    """
    res = GRAMMAR.parseString(query)

    struct_query = SelectQuery()
    for key, value in res.asDict().items():
        if key == 'columns':
            struct_query.add_projections(value)
        elif key == 'from':
            struct_query.add_source(value)
        elif key == 'limit':
            struct_query.add_limit(value)
        else:
            raise RuntimeError(f"unsupported field: {key}")

    return struct_query
