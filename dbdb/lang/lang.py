
import pyparsing as pp
from pyparsing import common as ppc


from dbdb.tuples.identifiers import (
    TableIdentifier,
    FieldIdentifier,
    GlobIdentifier
)

from dbdb.operators.joins import JoinType

from dbdb.io import file_format
from dbdb.io.file_wrapper import FileReader

pp.ParserElement.enable_packrat()
# pp.enable_all_warnings()


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


def handle_is(string, loc, toks):
    return "IS"

def handle_is_not(string, loc, toks):
    return "IS_NOT"


LPAR = pp.Literal('(')
RPAR = pp.Literal(')')

WITH = pp.CaselessKeyword("WITH")
SELECT = pp.CaselessKeyword("SELECT")
FROM = pp.CaselessKeyword("FROM")
AS = pp.CaselessKeyword("AS")
ON = pp.CaselessKeyword("ON")
AND = pp.CaselessKeyword("AND")
OR = pp.CaselessKeyword("OR")

IS = pp.CaselessKeyword("IS").setParseAction(handle_is)
IS_NOT = (IS + pp.CaselessKeyword("NOT")).setParseAction(handle_is_not)

WHERE = pp.CaselessKeyword("WHERE")
GROUP_BY = (pp.CaselessKeyword("GROUP") + pp.CaselessKeyword("BY"))
ORDER_BY = (pp.CaselessKeyword("ORDER") + pp.CaselessKeyword("BY"))
LIMIT = pp.CaselessKeyword("LIMIT")

ASC = pp.CaselessKeyword("ASC")
DESC = pp.CaselessKeyword("DESC")

LEFT_OUTER = pp.CaselessKeyword("LEFT") + pp.Opt(pp.CaselessKeyword("OUTER"))
RIGHT_OUTER = pp.CaselessKeyword("RIGHT") + pp.Opt(pp.CaselessKeyword("OUTER"))
FULL_OUTER = pp.CaselessKeyword("FULL") + pp.CaselessKeyword("OUTER")
INNER = pp.CaselessKeyword("INNER")
CROSS = pp.CaselessKeyword("CROSS")
NATURAL = pp.CaselessKeyword("NATURAL")
JOIN = pp.CaselessKeyword("JOIN")
USING = pp.CaselessKeyword("USING")

# Window funcs
OVER = pp.CaselessKeyword("OVER")
ROWS = pp.CaselessKeyword("ROWS")
BETWEEN = pp.CaselessKeyword("BETWEEN")
UNBOUNDED = pp.CaselessKeyword("UNBOUNDED")
PRECEDING = pp.CaselessKeyword("PRECEDING")
FOLLOWING = pp.CaselessKeyword("FOLLOWING")
CURRENT = pp.CaselessKeyword("CURRENT")
ROW = pp.CaselessKeyword("ROW")

RESERVED = pp.Group(
    FROM     |
    WITH     |
    SELECT   |
    AS       |
    ON       |
    AND      |
    OR       |
    IS       |
    IS_NOT   |
    WHERE    |
    GROUP_BY |
    ORDER_BY |
    USING    |
    LIMIT    |
    LEFT_OUTER  |
    RIGHT_OUTER |
    INNER       |
    CROSS       |
    NATURAL     |
    JOIN |
    ASC  |
    DESC
).set_name("reserved_word")

IDENT = ~RESERVED + (
    pp.Word(pp.srange("[a-zA-Z_]"), pp.srange("[a-zA-Z0-9_"))
)("ident*")

class ASTToken:
    def has_aggregate(self):
        return False


class ColumnIdentifier(ASTToken):
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def qualify(self):
        if self.table:
            name = f"{self.table}.{self.column}"
        else:
            name = self.column

        return name

    def eval(self, row):
        name = self.qualify()
        return row.field(name)

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        name = self.qualify()
        return {name}


def make_col_identifier(string, loc, toks):
    ident_parts = toks[0]
    if len(ident_parts) == 1:
        table = None
        column = ident_parts[0]
    else:
        table, column = ident_parts

    return ColumnIdentifier(table, column)


class TableIdent(ASTToken):
    def __init__(self, table_name, alias=None):
        self.table_name = table_name
        self.alias = alias

    def eval(self, row):
        # If we call this, something bad happened...
        raise NotImplementedError()



def make_table_identifier(string, loc, toks):
    table = toks[0]
    return TableIdent(table)


def make_aliased_table_identifier(string, loc, toks):
    table = toks[0]
    if len(toks) > 1:
        table.alias = toks[-1].table_name
    return table


def make_aliased_table_function(string, loc, toks):
    func = toks[0]
    if len(toks) > 1:
        func.alias = toks[-1].table_name
    else:
        func.alias = toks[0].func_name
    return func


QUALIFIED_IDENT = pp.Group(
    pp.delimitedList(IDENT, delim=".", max=2)
)("qualified_ident").setParseAction(make_col_identifier)


TABLE_IDENT = IDENT.copy() \
    .set_name("table_ident") \
    .setParseAction(make_table_identifier)


class Literal(ASTToken):
    def __init__(self, val):
        self.val = val

    def eval(self, row):
        return self.val

    def value(self):
        return self.val

    def get_aggregated_fields(self):
        return set()

    def get_non_aggregated_fields(self):
        return set()

    def is_int(self):
        return isinstance(self.val, int)


def as_literal(string, loc, toks):
    # Big hack - no idea why this happens!
    if isinstance(toks[0], Literal):
        return toks[0]
    else:
        return Literal(toks[0])

def as_bool(string, loc, toks):
    if toks[0].upper() == 'TRUE':
        return True
    elif toks[0].upper() == 'FALSE':
        return False
    else:
        raise RuntimeError("how?")


LIT_STR = pp.QuotedString(quote_char="'")
LIT_NUM = ppc.number

LIT_BOOL = (
    pp.CaselessKeyword("TRUE") | pp.CaselessKeyword("FALSE")
).setParseAction(as_bool)

LITERAL = (LIT_STR | LIT_NUM | LIT_BOOL).setParseAction(as_literal)
STAR = "*"

EXPRESSION = pp.Forward()


from dbdb.operators.aggregate import Aggregates
class FunctionCall(ASTToken):
    def __init__(self, func_name, func_expr, agg_type):
        self.func_name = func_name
        self.func_expr = func_expr
        self.agg_type = agg_type

    def eval(self, row):
        return self.func_expr.eval(row)

    def get_aggregated_fields(self):
        # If this is a scalar function, return the set of fields
        # that are aggregated within the function expression
        if self.agg_type == Aggregates.SCALAR:
            return self.func_expr.get_aggregated_fields()
        else:
            # If it's an aggregate function, then confirm that the func_expr
            # is _not_ also an aggregate. Otherwise, return the non-agg fields
            # contained within the function expression
            scalar_fields = set()

            for expr in self.func_expr:
                aggs = expr.get_aggregated_fields()
                if len(aggs) > 0:
                    raise RuntimeError("Tried to agg an agg")

                scalars = expr.get_non_aggregated_fields()
                scalar_fields.update(scalars)

            # So these are the un-agg fields that become aggregated via being
            # contained within this function
            return scalar_fields

    def get_non_aggregated_fields(self):
        if self.agg_type == Aggregates.SCALAR:
            return self.func_expr.get_non_aggregated_fields()
        else:
            return set()


def call_function(string, loc, toks):
    # <func_name> ( <expr> )
    func_name = toks[0].func_name[0].upper()
    func_expr = toks[0].func_expression
    agg_type = getattr(Aggregates, func_name, Aggregates.SCALAR)
    return FunctionCall(func_name, func_expr, agg_type)


def call_window_function(string, loc, toks):
    func_name = toks[0].func_name[0].upper()
    func_expr = toks[0].func_expression
    agg_type = Aggregates.SCALAR

    import ipdb; ipdb.set_trace()
    return FunctionCall(func_name, func_expr, agg_type)


FUNC_CALL = pp.Group(
    IDENT("func_name") +
    LPAR +
    pp.Opt(
        pp.delimitedList(EXPRESSION)("func_expression")
    ) +
    RPAR
).setParseAction(call_function)

FRAME_CLAUSE = pp.Group(
    ROWS +
    BETWEEN +
    (
        (UNBOUNDED + PRECEDING) | (CURRENT + ROW)
    )("frame_start") +
    AND +
    (
        (CURRENT + ROW) | (UNBOUNDED + FOLLOWING)
    )("frame_end")
)

WINDOW_CALL = pp.Group(
    IDENT("func_name") +
    LPAR +
    pp.Opt(
        pp.delimitedList(EXPRESSION)("func_expression")
    ) +
    RPAR +
    OVER +
    LPAR +
    FRAME_CLAUSE("frame") +
    RPAR
).setParseAction(call_window_function)


class BinaryOperator:
    def __init__(self, lhs, operator, rhs):
        self.lhs = lhs
        self.operator = operator
        self.rhs = rhs

    def eval(self, row):
        if self.operator == '+':
            op = lambda l, r: l + r
        elif self.operator == '-':
            op = lambda l, r: l - r
        elif self.operator == '*':
            op = lambda l, r: l * r
        elif self.operator == '/':
            op = lambda l, r: l / r

        # TODO: Don't think this is right....
        elif self.operator == 'AND':
            op = lambda l, r: l and r
        elif self.operator == 'OR':
            op = lambda l, r: l or r

        elif self.operator == '=':
            op = lambda l, r: l == r
        elif self.operator == '!=':
            op = lambda l, r: l != r

        elif self.operator == 'IS':
            op = lambda l, r: l is r

        elif self.operator == 'IS_NOT':
            op = lambda l, r: l is not r

        else:
            import ipdb; ipdb.set_trace()

        return op(
            self.lhs.eval(row),
            self.rhs.eval(row)
        )

    def get_aggregated_fields(self):
        return self.lhs.get_aggregated_fields().union(
            self.rhs.get_aggregated_fields()
        )

    def get_non_aggregated_fields(self):
        return self.lhs.get_non_aggregated_fields().union(
            self.rhs.get_non_aggregated_fields()
        )


def op_negate(string, loc, toks):
    # TODO:
    pass


def binary_operator(string, loc, toks):
    lhs, op, rhs = toks[0]
    return BinaryOperator(lhs, op, rhs)


EXPRESSION << pp.infix_notation(
    FUNC_CALL | LITERAL | QUALIFIED_IDENT | STAR,
    [
        ('-', 1, pp.OpAssoc.RIGHT, op_negate),
        (pp.oneOf('* /'), 2, pp.OpAssoc.LEFT, binary_operator),
        (pp.oneOf('+ -'), 2, pp.OpAssoc.LEFT, binary_operator),
        (pp.oneOf('<= >='), 2, pp.OpAssoc.LEFT, binary_operator),
        (pp.oneOf('< >'), 2, pp.OpAssoc.LEFT, binary_operator),
        (pp.oneOf('= !='), 2, pp.OpAssoc.LEFT, binary_operator),

        (IS_NOT, 2, pp.OpAssoc.LEFT, binary_operator),
        (IS, 2, pp.OpAssoc.LEFT, binary_operator),

        (AND, 2, pp.OpAssoc.LEFT, binary_operator),
        (OR, 2, pp.OpAssoc.LEFT, binary_operator),
    ]
)("expression")

COLUMN_EXPR = pp.Group(
    EXPRESSION("column_expression") +
    pp.Opt(
        pp.Opt(AS) +
        IDENT("alias")
    )
)

class JoinCondition:
    def __init__(self, join_type, join_expr):
        self.join_type = join_type
        self.join_expr = join_expr

    def eval(self, row):
        return self.join_expr.eval(row)

    @classmethod
    def make_from_on_expr(cls, tokens):
        # ON <expr>
        return JoinCondition('ON', tokens[1])

    @classmethod
    def make_from_using_expr(cls, tokens):
        # USING ( <field list> )
        # Build our own expression...
        # TODO: What is the LHS and RHS???
        join_expr = None
        return JoinCondition('USING', join_expr)


def make_explicit_join(string, loc, toks):
    return JoinCondition.make_from_on_expr(toks)

def make_implicit_join(string, loc, toks):
    return JoinCondition.make_from_using_expr(toks)


def make_join(string, loc, toks):
    if toks[0] == 'JOIN':
        return JoinType.INNER
    elif toks[0] == 'INNER':
        return JoinType.INNER
    elif toks[0] == 'LEFT' and toks[1] == 'OUTER':
        return JoinType.LEFT_OUTER
    elif toks[0] == 'RIGHT' and toks[1] == 'OUTER':
        return JoinType.RIGHT_OUTER
    elif toks[0] == 'FULL' and toks[1] == 'OUTER':
        return JoinType.FULL_OUTER
    elif toks[0] == 'NATURAL':
        return JoinType.NATURAL
    elif toks[0] == 'CROSS':
        return JoinType.CROSS
    elif toks[0] == ',':
        return JoinType.CROSS
    else:
        raise RuntimeError(f"Invalid join type: {toks}")


QUALIFIED_JOIN_TYPES = (
    (LEFT_OUTER + JOIN) |
    (FULL_OUTER + JOIN) |
    (RIGHT_OUTER + JOIN) |
    (INNER + JOIN) |
    JOIN
).setParseAction(make_join)

UNQUALIFIED_JOIN_TYPES = (
    (NATURAL + JOIN) |
    (CROSS + JOIN) |
    pp.Literal(",")
).setParseAction(make_join)

# TODO : For this to actually work, we need to know about the LHS and RHS
# otherwise we cannot build the binary operator....
JOIN_CONDITION = (
    (ON + EXPRESSION).setParseAction(make_explicit_join) |
    (USING + LPAR + pp.delimitedList(IDENT) + RPAR).setParseAction(make_implicit_join)
)


class JoinClause(ASTToken):
    def __init__(self, to, join_type, on):
        self.to = to
        self.join_type = join_type
        self.on = on

    @classmethod
    def new_qualified(cls, to, join_type, on):
        return cls(to, join_type, on)

    @classmethod
    def new_unqualified(cls, to, join_type):
        return cls(to, join_type, on=True)


def make_join_clause(string, loc, tokens):
    # <join type> <target> [condition]
    if len(tokens) == 2:
        join_type, to = tokens
        return JoinClause.new_unqualified(to, join_type)
    elif len(tokens) == 3:
        join_type, to, on = tokens
        return JoinClause.new_qualified(to, join_type, on)
    else:
        raise RuntimeError(f"Unexpected join: {tokens}")


ALIASED_TABLE_IDENT = (
    TABLE_IDENT + pp.Opt(AS) + pp.Opt(TABLE_IDENT)
).setParseAction(make_aliased_table_identifier)

ALIASED_TABLE_FUNCTION = (
    FUNC_CALL + pp.Opt(AS) + pp.Opt(TABLE_IDENT)
).setParseAction(make_aliased_table_function)

JOIN_CLAUSE = (
    (QUALIFIED_JOIN_TYPES + ALIASED_TABLE_IDENT + JOIN_CONDITION) |
    (UNQUALIFIED_JOIN_TYPES + ALIASED_TABLE_IDENT)
).setParseAction(make_join_clause)


from dbdb.lang.select import SelectOrder
ORDER_BY_LIST = pp.delimitedList(
    pp.Group(EXPRESSION + pp.Opt(ASC | DESC))
).setParseAction(SelectOrder.parse_tokens)


SELECT_STATEMENT = pp.Forward()

SELECT_GRAMMAR = pp.Group(
    SELECT +

    pp.delimitedList(
        COLUMN_EXPR
    )('columns') +

    # from clause
    # TODO: This is optional...
    FROM + (ALIASED_TABLE_FUNCTION("_from") | ALIASED_TABLE_IDENT("_from")) +

    pp.ZeroOrMore(
        JOIN_CLAUSE
    )("joins") +

    pp.Opt(
        WHERE + EXPRESSION
    )("where") +

    pp.Opt(
        GROUP_BY + pp.delimitedList(EXPRESSION)
    )("group_by") +

    pp.Opt(
        ORDER_BY + ORDER_BY_LIST
    )("order_by") +

    # limit
    pp.Opt(
        LIMIT + LIT_NUM.setParseAction(as_literal)
    )("limit")
)("select")

# Subqueries and CTEs
SUBQUERY = LPAR + SELECT_STATEMENT + RPAR + pp.Opt(pp.Opt(AS) + TABLE_IDENT)
CTE_SELECT = pp.Group(TABLE_IDENT("cte_alias") + AS + LPAR + SELECT_STATEMENT + RPAR)("cte")
CTE_LIST = WITH + pp.delimitedList(CTE_SELECT)("ctes")

# Set final select statement (CTEs + select)
SELECT_STATEMENT << (
    pp.Optional(CTE_LIST) +
    SELECT_GRAMMAR("select")
)

from dbdb.lang.select import (
    Select,
    SelectList,
    SelectProjection,
    SelectFilter,
    SelectFileSource,
    SelectFunctionSource,
    SelectReferenceSource,
    SelectMemorySource,
    SelectJoin,
    SelectGroupBy,
    SelectOrder,
    SelectOrderBy,
    SelectLimit,
)


def stringify(alias):
    if alias:
        return alias[0]
    else:
        return None


def extract_projections(ast):
    projections = []
    for i, column in enumerate(ast.columns):
        expr = column.column_expression
        alias = stringify(column.alias) or f'col_{i}'

        projection = SelectProjection(
            expr=expr,
            alias=alias
        )
        projections.append(projection)

    return SelectList(projections=projections)


def extract_wheres(ast):
    if 'where' not in ast:
        return None

    _, where_clause = ast.where
    # where clause is just a binary operator or expression here..

    return SelectFilter(expr=where_clause)


def make_table_source(table_source):
    table_name = table_source.table_name
    table_alias = table_source.alias
    table_id = TableIdentifier.new(table_name, table_alias)

    fname = f"{table_source.table_name}.dumb"
    reader = FileReader(fname)
    column_data = file_format.read_header(reader)
    column_names = [c.column_name for c in column_data]
    columns = [table_id.field(name) for name in column_names]

    return SelectFileSource(
        file_path=f"{table_id}.dumb",
        table_identifier=table_id,
        columns=columns,
    )


def make_source_function(func_source):
    func_name = func_source.func_name
    func_expr = func_source.func_expr
    func_alias = func_source.alias

    table_id = TableIdentifier.new(func_alias, func_alias)

    return SelectFunctionSource(
        function_name=func_name,
        function_args=[f.value() for f in func_expr],
        table_identifier=table_id,
    )


def make_reference_source(source):
    table_id = TableIdentifier.new(source.table_name, source.alias)
    return SelectReferenceSource(
        table_identifier=table_id,
    )


def extract_source(source, scopes):
    if isinstance(source, FunctionCall):
        return make_source_function(source)

    scope_name = source.table_name
    if scope_name in scopes:
        return make_reference_source(source)
    else:
        return make_table_source(source)


def extract_joins(ast, scopes):
    joins = []
    for join_clause in ast.joins:
        join_source = extract_source(join_clause.to, scopes)
        join = SelectJoin.new(
            to=join_source,
            expression=join_clause.on,
            join_type=join_clause.join_type,
        )
        joins.append(join)

    return joins


def extract_group_by(ast, projections):
    if 'group_by' not in ast:
        return None

    grouping_exprs = ast.group_by[2:]

    # GROUP BY <expr>
    return SelectGroupBy(
        grouping_exprs,
        projections,
    )


def extract_order_by(ast):
    if 'order_by' not in ast:
        return None

    # ORDER BY <expr>
    return ast.order_by[2]


def extract_limit(ast):
    if 'limit' not in ast:
        return None

    # LIMIT <count>
    limit = ast.limit[1]
    return SelectLimit(limit)

def make_select_from_scope(ast_select, scopes):
    projections = extract_projections(ast_select)
    wheres = extract_wheres(ast_select)
    joins = extract_joins(ast_select, scopes)
    order_by = extract_order_by(ast_select)
    group_by = extract_group_by(ast_select, projections)
    limit = extract_limit(ast_select)

    source = None
    if '_from' in ast_select:
        source = extract_source(ast_select._from, scopes)

    return Select(
        projections=projections,
        where=wheres,
        source=source,
        joins=joins,
        group_by=group_by,
        order_by=order_by,
        limit=limit
    )

def ast_to_select_obj(ast):
    scopes = {}
    for cte  in ast.ctes:
        alias = cte.cte_alias
        select = make_select_from_scope(cte.select, scopes)
        scopes[alias.table_name] = select

    select = make_select_from_scope(ast.select, scopes)
    select.ctes = scopes

    return select


def parse_query(query):
    """
    Expected format
        SELECT
            <column>, [<column>, ...]
        FROM <table path>
        [LIMIT <count>]
    """
    ast = SELECT_STATEMENT.parseString(query)
    select_obj = ast_to_select_obj(ast)
    return select_obj
