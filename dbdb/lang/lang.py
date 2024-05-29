
import pyparsing as pp
from pyparsing import common as ppc


from dbdb.lang.expr_types import (
    ColumnIdentifier,
    TableIdent,
    Literal,
    FunctionCall,
    BinaryOperator,
    CaseWhen,
    CastExpr,
    JoinClause,
    JoinCondition,
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
    CreateTableAs
)

from dbdb.tuples.identifiers import (
    TableIdentifier,
    FieldIdentifier,
    GlobIdentifier
)

from dbdb.operators.joins import JoinType
from dbdb.operators.aggregate import Aggregates
from dbdb.operators.union import UnionOperator

from dbdb.io import file_format
from dbdb.io.file_wrapper import FileReader
from dbdb.logger import logger

import math

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
        logger.info(f"Source table: {self.source}")
        logger.info(f" Projections: {self.projections}")
        logger.info(f"     Filters: {self.filters}")
        logger.info(f"       Limit: {self.limit}")


def handle_is(string, loc, toks):
    return "IS"

def handle_is_not(string, loc, toks):
    return "IS_NOT"


LPAR = pp.Literal('(')
RPAR = pp.Literal(')')

WITH = pp.CaselessKeyword("WITH")
SELECT = pp.CaselessKeyword("SELECT")
FROM = pp.CaselessKeyword("FROM")
UNION = pp.CaselessKeyword("UNION")
AS = pp.CaselessKeyword("AS")
ON = pp.CaselessKeyword("ON")
AND = pp.CaselessKeyword("AND")
OR = pp.CaselessKeyword("OR")


CREATE = pp.CaselessKeyword("CREATE")
TABLE = pp.CaselessKeyword("TABLE")

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

# Case when
CASE = pp.CaselessKeyword("CASE")
WHEN = pp.CaselessKeyword("WHEN")
THEN = pp.CaselessKeyword("THEN")
ELSE = pp.CaselessKeyword("ELSE")
END = pp.CaselessKeyword("END")

# dbdb - fun!
PLAY = pp.CaselessKeyword("PLAY")
AT = pp.CaselessKeyword("AT")
BPM = pp.CaselessKeyword("BPM")

# TYPES
STRING = pp.CaselessKeyword("STRING")
TEXT = pp.CaselessKeyword("TEXT")
INT = pp.CaselessKeyword("INT")
FLOAT = pp.CaselessKeyword("FLOAT")

# Literals
PI = pp.CaselessKeyword("PI").setParseAction(lambda x: Literal(math.pi))

MATH = (
    PI
)

RESERVED = pp.Group(
    UNION    |
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
    DESC |
    PLAY |
    AT   |
    PI   |
    CASE |
    WHEN |
    THEN |
    ELSE |
    END |
    CREATE
).set_name("reserved_word")

IDENT = ~RESERVED + (
    pp.Word(pp.srange("[a-zA-Z_]"), pp.srange("[a-zA-Z0-9_"))
)("ident*")



def make_col_identifier(string, loc, toks):
    ident_parts = toks[0]
    if len(ident_parts) == 1:
        table = None
        column = ident_parts[0]
    else:
        table, column = ident_parts

    return ColumnIdentifier(table, column)


def make_table_identifier(string, loc, toks):
    table = toks[0]
    return TableIdent(table)


def make_qualified_table_identifier(string, loc, toks):
    table_parts = [t.table_name for t in toks[0]]

    database = None
    schema = None

    if len(table_parts) == 1:
        table_name = table_parts[0]

    elif len(table_parts) == 2:
        schema = table_parts[0]
        table_name = table_parts[1]

    elif len(table_parts) == 3:
        database = table_parts[0]
        schema = table_parts[1]
        table_name = table_parts[2]

    return TableIdent(
        table_name=table_name,
        schema=schema,
        database=database
    )


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


NAMESPACED_TABLE_IDENT = pp.Group(
    pp.delimitedList(TABLE_IDENT, delim=".", max=3)
)("qualified_table_ident").setParseAction(make_qualified_table_identifier)



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

    raise RuntimeError("Window functions are not currently supported")

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


def case_when(string, loc, toks):
    when_conds = toks[0].when_conds
    else_expr = toks[0].else_cond

    when_exprs = [(w.when_expr, w.then_expr) for w in when_conds]
    return CaseWhen(when_exprs, else_expr)

WHEN_COND = pp.Group(
    WHEN + EXPRESSION.copy()("when_expr") +
    THEN + EXPRESSION.copy()("then_expr")
)

CASE_WHEN_EXPR = pp.Group(
    CASE +
    pp.OneOrMore(WHEN_COND)("when_conds") +
    pp.Opt(ELSE + EXPRESSION.copy()("else_cond"))
    + END
)("case_when").setParseAction(case_when)



TYPE = (
    STRING |
    TEXT   |
    INT    |
    FLOAT
).setParseAction(CastExpr.make)



def op_negate(string, loc, toks):
    # TODO:
    pass


def binary_operator(string, loc, toks):
    # Combine exprs, eg:
    # -> x * y * z
    # -> x * op(y, *, z)
    # -> op(x, * op(y, *, z)

    exprs = toks[0].deepcopy()
    binop = None
    while len(exprs) > 1:
        rhs = exprs.pop()
        op = exprs.pop()
        lhs = exprs.pop()

        binop = BinaryOperator(lhs, op, rhs)
        exprs.append(binop)

    return binop

EXPRESSION << pp.infix_notation(
    FUNC_CALL | LITERAL | MATH | CASE_WHEN_EXPR | TYPE | QUALIFIED_IDENT | STAR,
    [
        ('::', 2, pp.OpAssoc.LEFT, binary_operator),
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
    NAMESPACED_TABLE_IDENT + pp.Opt(AS) + pp.Opt(TABLE_IDENT)
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

PLAIN_SELECT = pp.Group(
    SELECT +

    pp.delimitedList(
        COLUMN_EXPR
    )('columns').setName('column_list') +

    # from clause
    # TODO: This is optional...
    pp.Opt(
        FROM + (ALIASED_TABLE_FUNCTION("_from") | ALIASED_TABLE_IDENT("_from"))
    ).setName('from').setName('from') +

    pp.ZeroOrMore(
        JOIN_CLAUSE
    )("joins").setName('joins') +

    pp.Opt(
        WHERE + EXPRESSION
    )("where").setName('where') +

    pp.Opt(
        GROUP_BY + pp.delimitedList(EXPRESSION)
    )("group_by").setName('groups')
)("select")

SET_OPERATION = pp.Group(
    pp.delimitedList(PLAIN_SELECT, UNION, min=1)
)("union").setName('union')

# Subqueries and CTEs
SUBQUERY = LPAR + SELECT_STATEMENT + RPAR + pp.Opt(pp.Opt(AS) + TABLE_IDENT)
CTE_SELECT = pp.Group(TABLE_IDENT("cte_alias") + AS + LPAR + SELECT_STATEMENT + RPAR)("cte")

# [ WITH cte, ... ]
# { SELECT_STATEMENT | PLAIN_SELECT }
# [ ORDER... ]
# [ LIMIT... ]
SELECT_STATEMENT << (
    pp.Opt(
        WITH +
        pp.delimitedList(CTE_SELECT)("ctes")
    ).setName('CTEs') +

    (
        SET_OPERATION.setName('select.set') |
        PLAIN_SELECT("select").setName('select.plain') |
        SELECT_STATEMENT("select")
    ) +

    pp.Opt(
        ORDER_BY + ORDER_BY_LIST
    )("order_by").setName('order_by') +

    # limit
    pp.Opt(
        LIMIT + LIT_NUM.setParseAction(as_literal)
    )("limit").setName('limit')
)

CREATE_TABLE_AS_STATEMENT = pp.Group(
    CREATE +
    TABLE +
    NAMESPACED_TABLE_IDENT("table_name") +
    AS +
    LPAR +
    pp.Group(SELECT_STATEMENT)("table_select") +
    RPAR
)("create")

GRAMMAR = (
    pp.stringStart() +
    (
        CREATE_TABLE_AS_STATEMENT
        | SELECT_STATEMENT
    ) +
    pp.stringEnd()
)

def extract_projections(ast):
    projections = []
    for i, column in enumerate(ast.columns):
        expr = column.column_expression
        alias = column.alias[0] if column.alias else None

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
    table_id = TableIdentifier(
        database=table_source.database,
        schema=table_source.schema,
        name=table_source.table_name,
        alias=table_source.alias
    )

    reader = FileReader(table_id)
    column_data = file_format.read_header(reader)
    column_names = [c.column_name for c in column_data]
    columns = [table_id.field(name) for name in column_names]

    return SelectFileSource(
        table=table_id,
        columns=columns,
    )


def make_source_function(func_source):
    func_name = func_source.func_name
    func_expr = func_source.func_expr
    func_alias = func_source.alias

    table_id = TableIdentifier.new(func_alias, func_alias)

    return SelectFunctionSource(
        function_name=func_name,
        function_args=[f.eval(None) for f in func_expr],
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


def extract_unions(ast):
    if 'union' not in ast:
        return None

    unions = ast.union[:-1]
    select = ast.union[-1]
    return unions, select


def make_select_from_ast(ast_select, scopes):
    projections = extract_projections(ast_select)
    wheres = extract_wheres(ast_select)
    joins = extract_joins(ast_select, scopes)
    order_by = extract_order_by(ast_select)
    group_by = extract_group_by(ast_select, projections)
    limit = extract_limit(ast_select)

    source = None
    if '_from' in ast_select:
        # TODO : Handle when FROM is missing!
        source = extract_source(ast_select._from, scopes)

    return Select(
        projections=projections,
        where=wheres,
        source=source,
        joins=joins,
        group_by=group_by,
        order_by=order_by,
        limit=limit,

        scopes=scopes,
    )


def make_select_from_scope(ast, scopes):
    if ast.select:
        unions = []
        select = ast.select
    elif ast.union:
        unions, select = extract_unions(ast)
    else:
        raise RuntimeError("Saw a SELECT that is niether a single select or union?")

    select = make_select_from_ast(select, scopes)

    unions = [make_select_from_ast(u, scopes) for u in unions]
    select.unions = unions
    return select


def ast_to_select_obj(ast):
    scopes = {}
    plan = None
    for cte in ast.ctes:
        alias = cte.cte_alias
        scope_copy = scopes.copy()
        select = make_select_from_scope(cte, scope_copy)
        plan, output_node = select.make_plan(plan)
        scopes[alias.table_name] = output_node

    select = make_select_from_scope(ast, scopes)
    select.save_plan(plan)

    return select


def ast_to_create_obj(ast):
    ast = ast.create

    table_source = ast.table_name
    table_select_ast = ast.table_select
    table_select_obj = ast_to_select_obj(table_select_ast)

    table_id = TableIdentifier(
        database=table_source.database,
        schema=table_source.schema,
        name=table_source.table_name,
        alias=table_source.alias
    )

    ctas = CreateTableAs(
        table=table_id,
        select=table_select_obj
    )

    ctas.save_plan()
    return ctas


def parse_query(query):
    ast = GRAMMAR.parseString(query)
    if ast.create:
        query = ast_to_create_obj(ast)
    else:
        query = ast_to_select_obj(ast)
    return query
