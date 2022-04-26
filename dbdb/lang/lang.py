
import pyparsing as pp
from pyparsing import common as ppc


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


IDENT = (
    pp.Word(pp.srange("[a-zA-Z_]"), pp.srange("[a-zA-Z0-9_"))
)("ident*")

QUALIFIED_IDENT = pp.Group(
    pp.delimitedList(IDENT, delim=".", max=2)
)("qualified_ident")

AS = pp.CaselessKeyword("AS")
ON = pp.CaselessKeyword("ON")

LPAR = pp.Literal('(')
RPAR = pp.Literal(')')

STR_LITERAL = pp.QuotedString(quote_char="'")
NUMBER_LITERAL = ppc.number
LITERAL = STR_LITERAL | NUMBER_LITERAL

AND = pp.CaselessKeyword("AND")
OR = pp.CaselessKeyword("OR")

IS = pp.CaselessKeyword("IS")
IS_NOT = IS + pp.CaselessKeyword("NOT")

WHERE = pp.CaselessKeyword("WHERE")
ORDER_BY = (pp.CaselessKeyword("ORDER") + pp.CaselessKeyword("BY"))


EXPRESSION = pp.Forward()

FUNC_CALL = (
    IDENT("func_name") +
    LPAR +
    pp.Opt(EXPRESSION("func_expression")) +
    RPAR
)("func_call")


EXPRESSION << pp.infix_notation(
    FUNC_CALL | QUALIFIED_IDENT | LITERAL,
    [
        ('-', 1, pp.OpAssoc.RIGHT),
        (pp.oneOf('* /'), 2, pp.OpAssoc.LEFT),
        (pp.oneOf('+ -'), 2, pp.OpAssoc.LEFT),
        (pp.oneOf('<= >='), 2, pp.OpAssoc.LEFT),
        (pp.oneOf('< >'), 2, pp.OpAssoc.LEFT),
        (pp.oneOf('= !='), 2, pp.OpAssoc.LEFT),

        (IS_NOT, 2, pp.OpAssoc.LEFT),
        (IS, 2, pp.OpAssoc.LEFT),

        (AND, 2, pp.OpAssoc.LEFT),
        (OR, 2, pp.OpAssoc.LEFT),
    ]
)("expression")

COLUMN_EXPR = pp.Group(
    (FUNC_CALL | EXPRESSION)
    + pp.Opt(AS)
    + pp.Opt(IDENT("alias"))
)

JOIN_TYPE = (
    (pp.CaselessKeyword("LEFT") + pp.Opt(pp.CaselessKeyword("OUTER"))) |
    (pp.CaselessKeyword("INNER")) |
    (pp.CaselessKeyword("FULL") + pp.CaselessKeyword("OUTER")) |
    (pp.CaselessKeyword("CROSS")) |
    (pp.Empty())
) + pp.CaselessKeyword("JOIN")

JOIN_CLAUSE = (
    JOIN_TYPE + QUALIFIED_IDENT + ON + EXPRESSION
)

GRAMMAR = (
    pp.CaselessKeyword("SELECT") +

    pp.delimitedList(
        COLUMN_EXPR
    )('columns') +

    # from clause
    pp.CaselessKeyword("FROM") + QUALIFIED_IDENT("_from") +

    pp.ZeroOrMore(
        JOIN_CLAUSE
    )("joins") +

    pp.Opt(
        WHERE + EXPRESSION
    )("where") +

    pp.Opt(
        ORDER_BY + pp.delimitedList(EXPRESSION)
    )("order_by") +

    # limit
    pp.Opt(
        pp.CaselessKeyword("LIMIT") +
        pp.Word(pp.nums)("limit")
        .setParseAction(lambda x: int(x[0]))
    )("limit") +

    pp.StringEnd()
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

    # I think i want to make a visitor kind of pattern for this bad-boy

    return res
