from enum import Flag, auto
from typing import List, Dict, Any
from collections import namedtuple


class ColumnConstraint(Flag):
    NONE = 0
    PRIMARY_KEY = auto()
    UNIQUE = auto()
    NOT_NULL = auto()


class Operation:
    pass


class VarOp(Operation):
    pass


class Terminal():
    def is_terminal(self):
        return True

    def columns_used(self):
        raise Exception()


class NotTerminal():
    def is_terminal(self):
        return False


class Literal:
    def simplify(self):
        return self


class TrueOp(Operation, Literal):
    value = True

    def evaluate(self, context):
        return True


class FalseOp(Operation, Literal):
    value = False

    def evaluate(self, context):
        return False


def collect_values(op):
    if type(op) == Equals:
        return [op.right]
    elif type(op) == InFunc:
        return op.values
    else:
        raise Exception()


class And(Operation, NotTerminal):
    def __init__(self, left: Operation, right: Operation):
        self.left = left
        self.right = right

    def __repr__(self):
        return '({} and {})'.format(self.left, self.right)

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()
        if type(self.left) == FalseOp or type(self.right) == FalseOp:
            return FalseOp()
        elif type(self.left) == TrueOp and type(self.right) == TrueOp:
            return TrueOp()
        else:
            return self

    def visit(self, consumer, order=0):
        consumer(self)
        self.left.visit(consumer, order)
        self.right.visit(consumer, order)

    def evaluate(self, context):
        return self.left.evaluate(context) and self.right.evaluate(context)


class Or(Operation, NotTerminal):
    def __init__(self, left: Operation, right: Operation):
        self.left = left
        self.right = right

    def __repr__(self):
        return '({} or {})'.format(self.left, self.right)

    def visit(self, consumer, order=0):
        consumer(self)
        self.left.visit(consumer, order)
        self.right.visit(consumer, order)

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()
        if type(self.left) == FalseOp and type(self.right) == FalseOp:
            return FalseOp()
        elif type(self.left) == TrueOp or type(self.right) == TrueOp:
            return TrueOp()
        else:
            collapse_types = (Equals, InFunc)
            if type(self.left) in collapse_types and type(
                    self.right) in collapse_types:
                if self.left.left == self.right.left:
                    # same column
                    values = collect_values(self.left) + collect_values(
                        self.right)
                    return InFunc(self.left.left, values)

        return self

    def evaluate(self, context):
        return self.left.evaluate(context) or self.right.evaluate(context)


class Not(Operation, NotTerminal):
    def __init__(self, operation: Operation):
        self.operation = operation

    def __repr__(self):
        return 'not ({})'.format(self.operation)

    def visit(self, consumer, order=0):
        consumer(self)
        self.operation.visit(consumer, order)

    def simplify(self):
        self.operation = self.operation = self.operation.simplify()
        if type(self.operation) == TrueOp:
            return FalseOp()
        elif type(self.operation) == FalseOp:
            return TrueOp()
        elif type(self.operation) == Equals:
            return NotEquals(self.operation.left, self.operation.right)
        elif type(self.operation) == NotEquals:
            return Equals(self.operation.left, self.operation.right)
        else:
            return self

    def evaluate(self, context):
        return not self.operation.evaluate(context)


class InFunc(Operation, Terminal):
    def __init__(self, left, values):
        self.left = left
        self.values = values

    def __repr__(self):
        return '{} in [{}]'.format(self.left, ', '.join(map(str, self.values)))

    def visit(self, consumer, order=0):
        consumer(self)

    def columns_used(self):
        return [self.left, self.values]

    def simplify(self):
        return self

    def evaluate(self, context):
        return context.evaluate(self.left) in (context.evaluate(x) for x in
                                               self.values)


class Equals(Operation, Terminal):
    def __init__(self, left: VarOp, right: VarOp):
        self.left = left
        self.right = right

    def __repr__(self):
        return '{} = {}'.format(self.left, self.right)

    def visit(self, consumer, order=0):
        consumer(self)

    def columns_used(self):
        return [self.left, self.right]

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()
        if issubclass(type(self.left), Literal) and issubclass(type(self.right),
                                                               Literal):
            if self.left.value == self.right.value:
                return TrueOp
            else:
                return FalseOp
        # if type(self.left) == ColumnReference and type(self.right) == ColumnReference and self.left.table!=self.right.table:
        #     raise Exception('Join in where clauses are not supported: {}'.format(self))
        if issubclass(type(self.left), Literal):
            # Left column should be non-literal
            self.left, self.right = self.right, self.left
        return self

    def evaluate(self, context):
        left = context.evaluate(self.left)
        right = context.evaluate(self.right)
        return left == right


class NotEquals(Operation, Terminal):
    def __init__(self, left: Operation, right: Operation):
        self.left = left
        self.right = right

    def __repr__(self):
        return '{} != {}'.format(self.left, self.right)

    def visit(self, consumer, order=0):
        consumer(self)

    def columns_used(self):
        return [self.left, self.right]

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()
        if issubclass(type(self.left), Literal) and issubclass(type(self.right),
                                                               Literal):
            if self.left.value != self.right.value:
                return TrueOp
            else:
                return FalseOp
        return self

    def evaluate(self, context):
        left = context.evaluate(self.left)
        right = context.evaluate(self.right)
        return left != right


class GreaterThan(Operation, Terminal):
    def __init__(self, left: VarOp, right: VarOp):
        self.left = left
        self.right = right

    def __repr__(self):
        return '{} > {}'.format(self.left, self.right)

    def visit(self, consumer, order=0):
        consumer(self)

    def columns_used(self):
        return [self.left, self.right]

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()
        if issubclass(type(self.left), Literal) and issubclass(type(self.right),
                                                               Literal):
            if self.left.value > self.right.value:
                return TrueOp
            else:
                return FalseOp
        if issubclass(type(self.left), Literal):
            # Left column should be non-literal
            self.left, self.right = self.right, self.left
        return self

    def evaluate(self, context):
        left = context.evaluate(self.left)
        right = context.evaluate(self.right)
        return left > right


class GreaterThanEquals(Operation, Terminal):
    def __init__(self, left: VarOp, right: VarOp):
        self.left = left
        self.right = right

    def __repr__(self):
        return '{} >= {}'.format(self.left, self.right)

    def visit(self, consumer, order=0):
        consumer(self)

    def columns_used(self):
        return [self.left, self.right]

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()
        if issubclass(type(self.left), Literal) and issubclass(type(self.right),
                                                               Literal):
            if self.left.value >= self.right.value:
                return TrueOp
            else:
                return FalseOp
        if issubclass(type(self.left), Literal):
            # Left column should be non-literal
            self.left, self.right = self.right, self.left
        return self

    def evaluate(self, context):
        left = context.evaluate(self.left)
        right = context.evaluate(self.right)
        return left >= right


class LessThan(Operation, Terminal):
    def __init__(self, left: VarOp, right: VarOp):
        self.left = left
        self.right = right

    def __repr__(self):
        return '{} < {}'.format(self.left, self.right)

    def visit(self, consumer, order=0):
        consumer(self)

    def columns_used(self):
        return [self.left, self.right]

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()
        if issubclass(type(self.left), Literal) and issubclass(type(self.right),
                                                               Literal):
            if self.left.value < self.right.value:
                return TrueOp
            else:
                return FalseOp
        if issubclass(type(self.left), Literal):
            # Left column should be non-literal
            self.left, self.right = self.right, self.left
        return self

    def evaluate(self, context):
        left = context.evaluate(self.left)
        right = context.evaluate(self.right)
        return left < right


class LessThanEquals(Operation, Terminal):
    def __init__(self, left: VarOp, right: VarOp):
        self.left = left
        self.right = right

    def __repr__(self):
        return '{} <= {}'.format(self.left, self.right)

    def visit(self, consumer, order=0):
        consumer(self)

    def columns_used(self):
        return [self.left, self.right]

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()
        if issubclass(type(self.left), Literal) and issubclass(type(self.right),
                                                               Literal):
            if self.left.value <= self.right.value:
                return TrueOp
            else:
                return FalseOp
        if issubclass(type(self.left), Literal):
            # Left column should be non-literal
            self.left, self.right = self.right, self.left
        return self

    def evaluate(self, context):
        left = context.evaluate(self.left)
        right = context.evaluate(self.right)
        return left <= right


class IntegerLiteral(Literal):
    def __init__(self, value: int):
        self.value = value

    def __repr__(self):
        return "{}".format(self.value)


class StringLiteral(Literal):
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return "'{}'".format(self.value)


class ColumnReference(
    namedtuple('ColumnReference', ['table', 'column', 'as_name'])):
    # table: str
    # column: str
    # as_name: str

    def __repr__(self):
        return '{}.{}'.format(self.table, self.column)

    def __eq__(self, other):
        if isinstance(other, ColumnReference):
            return self.table == other.table and self.column == other.column
        return False

    def __hash__(self):
        return hash((self.table, self.column))

    def simplify(self):
        return self

    @property
    def reference_name(self):
        return self.as_name if self.as_name else self.__repr__()


class TableReference(namedtuple('TableReference', ['name'])):
    # name: str

    def __repr__(self):
        return self.name


class JoinTable(namedtuple('JoinTable', ['table', 'left', 'right'])):
    # table: TableReference
    # left: ColumnReference
    # right: ColumnReference

    def __repr__(self):
        return 'JOIN {} ON {} = {}'.format(self.table, self.left, self.right)


class From(namedtuple('From', ['table', 'joins'])):
    # table: TableReference
    # joins: List[JoinTable]

    def __repr__(self):
        if len(self.joins) > 0:
            return 'FROM {} {}'.format(self.table,
                                       ' '.join(map(str, self.joins)))
        else:
            return 'FROM {}'.format(self.table)


class ColumnDefinition(
    namedtuple('ColumnDefinition', ['name', 'type', 'constraints'])):
    # name: str
    # type: str
    # constraints: ColumnConstraint
    pass


class OrderBy(namedtuple('OrderBy', ['columns', 'reverse'])):
    # columns: List[ColumnReference] = []
    # reverse: bool = False

    def __repr__(self):
        if len(self.columns) > 0:
            return ' ORDER BY {}{}'.format(', '.join(map(str, self.columns)),
                                           ' DESC' if self.reverse else '')
        else:
            return ''


class Update(namedtuple('Update', ['table', 'columns', 'where'])):
    # table: TableReference
    # columns: Dict[ColumnReference, Any]
    # where: Operation = TrueOp()

    def __repr__(self):
        sets = ', '.join(
            ['{}={}'.format(k, v) for k, v in self.columns.items()])
        s = 'UPDATE {} SET {}'.format(self.table, sets)
        if self.where:
            s += ' WHERE {}'.format(self.where)
        return s


class Delete(namedtuple('Delete', ['table', 'where'])):
    # table: TableReference
    # where: Operation = TrueOp()

    def __repr__(self):
        s = 'DELETE FROM {}'.format(self.table)
        if self.where:
            s += ' WHERE {}'.format(self.where)
        return s


class Select(
    namedtuple('Select', ['columns', 'from_clause', 'where', 'order_by'])):
    # columns: List[ColumnReference]
    # from_clause: From
    # where: Operation = TrueOp()
    # order_by: OrderBy = OrderBy()

    def __repr__(self):
        s = 'SELECT {} {}'.format(','.join(map(str, self.columns)),
                                  self.from_clause)
        if self.where:
            s += ' WHERE {}'.format(self.where)
        s += str(self.order_by)
        return s


class Insert(namedtuple('Insert', ['table', 'values'])):
    # table: TableReference
    # values: List

    def __repr__(self):
        return 'INSERT INTO {} VALUES({})'.format(self.table, ', '.join(
            map(str, self.values)))


class CreateTable(namedtuple('CreateTable', ['table', 'columns'])):
    # table: TableReference
    # columns: List[ColumnDefinition]
    pass


class Context:
    def __init__(self, row, columns: List[ColumnReference]):
        self.values = dict(zip(columns, row))

    def evaluate(self, reference):
        if reference in self.values:
            return self.values[reference]
        elif issubclass(type(reference), Literal):
            return reference.value
        raise Exception('Value not available')
