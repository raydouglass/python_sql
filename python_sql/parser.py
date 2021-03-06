import itertools
import re

from python_sql.logic import *

ALPHANUMERIC = re.compile('[A-Za-z]')
DIGIT = re.compile('[0-9]')
WORD = re.compile('\w')
WHITESPACE = re.compile(r'[\s\n\r\t]')
NOT_WHITESPACE = re.compile('[^\s]')
COMMA = re.compile(',')
PERIOD = re.compile('\.')
NOT_QUOTE = re.compile('[^\']')


class ParsedString():
    def __init__(self, string):
        self.string = string
        self.index = 0
        self.previous_token = None
        self.tried_start_index = None

    def try_start(self):
        self.tried_start_index = self.index

    def try_revert(self):
        self.index = self.tried_start_index

    def is_trying(self):
        return self.tried_start_index is not None

    def try_end(self):
        self.tried_start_index = None

    def peek(self, num_chars=1, skip_whitespace=True):
        if skip_whitespace:
            self.skip_whitespace()
        return ''.join(self.string[self.index:self.index + num_chars])

    def consume(self, num_chars=1, skip_whitespace=True):
        r = self.peek(num_chars, skip_whitespace)
        self.index += num_chars
        return r

    def peek_token(self, pattern=ALPHANUMERIC, skip_whitespace=True):
        if skip_whitespace:
            self.skip_whitespace()
        r = itertools.takewhile(lambda x: pattern.match(x),
                                self.string[self.index:])
        return ''.join(r)

    def consume_token(self, pattern=ALPHANUMERIC, skip_whitespace=True):
        if skip_whitespace:
            self.skip_whitespace()
        r = itertools.takewhile(lambda x: pattern.match(x),
                                self.string[self.index:])
        r = ''.join(r)
        self.index += len(r)
        self.previous_token = r
        if len(r) == 0:
            return self.raise_exception(expected=pattern)
        return r

    def consume_expected(self, expected, match_case=False,
                         skip_whitespace=True):
        if skip_whitespace:
            self.skip_whitespace()
        toRet = self.string[self.index:self.index + len(expected)]
        if not match_case:
            expected = expected.lower()
            actual = toRet.lower()
        else:
            actual = toRet
        if expected != actual:
            self.raise_exception(expected, actual)
        else:
            self.index += len(expected)
        return toRet

    def skip_whitespace(self):
        r = itertools.takewhile(lambda x: WHITESPACE.match(x),
                                self.string[self.index:])
        r = ''.join(r)
        self.index += len(r)

    def raise_exception(self, expected, actual=None):
        if actual is None:
            actual = self.previous_token if self.previous_token is not None and self.previous_token != '' else self.peek_token(
                NOT_WHITESPACE)
            index = self.index - len(self.previous_token)
        else:
            index = self.index - len(actual)
        raise ParseException(expected=expected, actual=actual, index=index)


class ParseException(Exception):
    def __init__(self, expected, actual, index=-1):
        if type(expected) == str:
            message = 'Expected "{}" but got "{}" at index {}'.format(expected,
                                                                      actual,
                                                                      index)
        elif type(expected) == tuple or type(expected) == list:
            message = 'Expected one of "{}" but got "{}" at index {}'.format(
                expected, actual, index)
        else:
            message = 'Expected matching regex "{}" but got "{}" at index {}'.format(
                expected.pattern, actual, index)
        super(ParseException, self).__init__(message)


QUERY_TYPES = ('select', 'insert', 'create', 'update', 'delete')


def query_type(parsed_string: ParsedString):
    next_token = parsed_string.consume_token().lower()
    if next_token in QUERY_TYPES:
        return next_token
    else:
        parsed_string.raise_exception(QUERY_TYPES)


def consume_list(parsed_string: ParsedString, consumer):
    results = []
    results.append(consumer(parsed_string))
    while parsed_string.peek_token(COMMA) == ',':
        parsed_string.consume_token(COMMA)
        results.append(consumer(parsed_string))
    return results


def column_consumer(col: ParsedString):
    # Required to be table.column
    table = col.consume_token(WORD)
    col.consume_expected('.')
    name = col.consume_token(WORD)
    if col.peek_token() == 'AS':
        col.consume_expected('AS')
        as_name = col.consume_token(WORD)
    else:
        as_name = None
    return ColumnReference(table, name, as_name)


def table_consumer(parsed_string: ParsedString):
    table = parsed_string.consume_token(WORD)
    return TableReference(table)


def string_literal(parsed_string: ParsedString):
    parsed_string.consume_expected("'")
    r = parsed_string.consume_token(NOT_QUOTE)
    parsed_string.consume_expected("'")
    while parsed_string.peek(1, skip_whitespace=False) == "'":
        r += "'"
        parsed_string.consume(1, skip_whitespace=False)
        r += parsed_string.consume_token(NOT_QUOTE)
        parsed_string.consume_expected("'")
    return StringLiteral(r)


def integer_literal(parsed_string: ParsedString):
    r = parsed_string.consume_token(DIGIT)
    if r is not None:
        return IntegerLiteral(int(r))
    return None


def try_consume(parsed_string: ParsedString, potentials):
    parsed_string.try_start()
    try:
        for p in potentials:
            try:
                r = p(parsed_string)
                if r is not None:
                    return r
            except ParseException:
                parsed_string.try_revert()
        raise ParseException(expected=potentials, actual=None,
                             index=parsed_string.index)
    finally:
        parsed_string.try_end()


LITERALS = [integer_literal,
            string_literal,
            column_consumer]


def _where_operand(parsed_string: ParsedString):
    return try_consume(parsed_string, LITERALS)


def consume_literal_list(parsed_string: ParsedString):
    def consumer(parsed_string):
        return try_consume(parsed_string, LITERALS)

    return consume_list(parsed_string, consumer)


OPERATIONS = {'=': Equals,
              '<': LessThan,
              '>': GreaterThan}
OPERATIONS2 = {'!=': NotEquals,
               '>=': GreaterThanEquals,
               '<=': LessThanEquals,
               'in': None}
ALL_OPERATIONS = {**OPERATIONS, **OPERATIONS2}


def _where_clause(parsed_string: ParsedString) -> Operation:
    if parsed_string.peek_token() == 'not':
        parsed_string.consume_expected('not')
        parsed_string.consume_expected('(')
        logic = _where(parsed_string)
        parsed_string.consume_expected(')')
        return Not(logic)
    else:
        left = _where_operand(parsed_string)
        operation = parsed_string.peek(2)
        if operation not in OPERATIONS2:
            operation = parsed_string.peek(1)
            if operation not in OPERATIONS:
                parsed_string.raise_exception(
                    expected=list(ALL_OPERATIONS.keys()), actual=operation)
        parsed_string.consume_expected(operation)
        if operation == 'in':
            parsed_string.consume_expected('(')
            values = consume_literal_list(parsed_string)
            parsed_string.consume_expected(')')
            return InFunc(left, values)
        else:
            right = _where_operand(parsed_string)

        op = ALL_OPERATIONS[operation]
        if op:
            return op(left, right)
        else:
            pass


def _where(parsed_string: ParsedString) -> Operation:
    parens = parsed_string.peek(1)
    if parens == '(':
        parsed_string.consume_expected('(')
        logic = _where(parsed_string)
        parsed_string.consume_expected(')')
    else:
        logic = _where_clause(parsed_string)
    options = ('and', 'or')
    while parsed_string.peek_token().lower() in options:
        op = parsed_string.consume_token().lower()
        n = _where(parsed_string)
        if op == 'and':
            logic = And(logic, n)
        elif op == 'or':
            logic = Or(logic, n)
    return logic


COLUMN_TYPES = ('int', 'double', 'varchar')


def column_definition_consumer(parsed_string: ParsedString) -> ColumnDefinition:
    name = parsed_string.consume_token()
    col_type = parsed_string.consume_token().lower()
    if col_type not in COLUMN_TYPES:
        parsed_string.raise_exception(COLUMN_TYPES, col_type)
    if col_type == 'varchar':
        parsed_string.consume_expected('(')
        size = int(parsed_string.consume_token(pattern=DIGIT))
        parsed_string.consume_expected(')')
    else:
        size = 8
    flags = ColumnConstraint.NONE
    while True:
        next_token = parsed_string.peek_token().lower()
        if next_token == 'primary':
            parsed_string.consume_expected('primary')
            parsed_string.consume_expected('key')
            flags |= ColumnConstraint.PRIMARY_KEY
            # flags |= ColumnConstraint.UNIQUE
            # flags |= ColumnConstraint.NOT_NULL
            if col_type!='int':
                raise Exception('Primary key must be an int')
        elif next_token == 'unique':
            parsed_string.consume_expected('unique')
            flags |= ColumnConstraint.UNIQUE
        elif next_token == 'not':
            parsed_string.consume_expected('not')
            parsed_string.consume_expected('null')
            flags |= ColumnConstraint.NOT_NULL
        else:
            break

    return ColumnDefinition(name, col_type, size, flags)


def _from(parsed_string: ParsedString):
    parsed_string.consume_expected('from')
    table = table_consumer(parsed_string)
    joins = []
    while parsed_string.peek_token().lower() in ('left', 'join'):
        # Join!
        parsed_string.consume_token()
        joined_table_ref = table_consumer(parsed_string)
        if parsed_string.peek_token().lower() == 'on':
            parsed_string.consume_expected('on')
            left = column_consumer(parsed_string)
            parsed_string.consume_expected('=')
            right = column_consumer(parsed_string)
            if right.table != joined_table_ref.name:
                # Always order so the joining table is second
                left, right = right, left
        else:
            left = None
            right = None
        joins.append(JoinTable(joined_table_ref, left, right))
    return From(table, joins)


def _order_by(parsed_string: ParsedString):
    parsed_string.consume_expected('order')
    parsed_string.consume_expected('by')
    order_by = consume_list(parsed_string, column_consumer)
    if parsed_string.peek_token().lower() == 'desc':
        parsed_string.consume_expected('desc')
        reverse = True
    else:
        reverse = False
    return OrderBy(order_by, reverse)


def _update_expr_consumer(expr: ParsedString):
    column = column_consumer(expr)
    expr.consume_expected('=')
    value = try_consume(expr, LITERALS)
    return column, value


def parse(query):
    parsed_string = ParsedString(query)
    type = query_type(parsed_string)
    if type == 'select':
        columns = consume_list(parsed_string, column_consumer)
        parsed_string.skip_whitespace()
        tables = _from(parsed_string)
        where_clause = TrueOp()
        got_where = False
        token = parsed_string.peek_token(NOT_WHITESPACE).lower()
        if token == 'where':
            parsed_string.consume_expected('where')
            where_clause = _where(parsed_string).simplify()
            token = parsed_string.peek_token(NOT_WHITESPACE).lower()
            got_where = True
        order_by = None
        if token == 'order':
            order_by = _order_by(parsed_string)
        elif token is not None and token != '':
            parsed_string.consume_token(NOT_WHITESPACE)
            if got_where:
                expected = 'order by'
            else:
                expected = ('where', 'order by')
            parsed_string.raise_exception(expected)
        return Select(columns, tables, where_clause, order_by)
    elif type == 'insert':
        parsed_string.consume_expected('into')
        table = table_consumer(parsed_string)
        parsed_string.consume_expected('values')
        parsed_string.consume_expected('(')
        values = consume_literal_list(parsed_string)
        parsed_string.consume_expected(')')
        return Insert(table, values)
    elif type == 'create':
        parsed_string.consume_expected('table')
        table = table_consumer(parsed_string)
        parsed_string.consume_expected('(')
        column_defs = consume_list(parsed_string, column_definition_consumer)
        parsed_string.consume_expected(')')
        return CreateTable(table, column_defs)
    elif type == 'update':
        table = table_consumer(parsed_string)
        parsed_string.consume_expected('set')
        columns = consume_list(parsed_string, _update_expr_consumer)
        map = {k: v for k, v in columns}
        token = parsed_string.peek_token(NOT_WHITESPACE).lower()
        if token == 'where':
            parsed_string.consume_expected('where')
            where_clause = _where(parsed_string).simplify()
        elif token:
            raise ParseException(expected='where', actual=token,
                                 index=parsed_string.index)
        else:
            where_clause = None
        return Update(table, map, where_clause)
    elif type == 'delete':
        parsed_string.consume_expected('from')
        table = table_consumer(parsed_string)
        token = parsed_string.peek_token(NOT_WHITESPACE).lower()
        if token == 'where':
            parsed_string.consume_expected('where')
            where_clause = _where(parsed_string).simplify()
        elif token:
            raise ParseException(expected='where', actual=token,
                                 index=parsed_string.index)
        else:
            where_clause = None
        return Delete(table, where_clause)


if __name__ == '__main__':
    query = 'select table.cola, table.col_b2 from table where a.c = 3 and (a.b = 1 or a.b = 2)'
    result = parse(query)
    print(result)

    query = "insert into table values('test', 123)"
    result = parse(query)
    print(result)

    query = "update table set table.column='test', table.cola=table.cola where table.cola = 1"
    result = parse(query)
    print(result)

    query = 'delete from table where table.cola != 1'
    result = parse(query)
    print(result)
