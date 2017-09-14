from data_structures.b_tree import BTree
from typing import NamedTuple, List
from data_structures.logic import *
from data_structures.parser import parse
import itertools
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)


class Table():
    def __init__(self, create_table: CreateTable):
        self.name = create_table.table.name
        self.column_defs = create_table.columns
        self.pk_def = self.column_defs[0]
        self._pk_index = BTree()
        self._data = []

    def insert(self, row):
        row_data = []
        for i, column_def in enumerate(self.column_defs):
            row_data.append(row.get(column_def.name, None))
        pk = row_data[0]
        if pk in self._pk_index:
            data_index = self._pk_index[pk]
            self._data[data_index] = row_data
        else:
            self._pk_index[pk] = len(self._pk_index)
            self._data.append(tuple(row_data))

    def direct_insert(self, row):
        if len(row) != len(self.column_defs):
            raise Exception('Cannot directly insert row with missing or extra columns.')
        row = tuple(x.value for x in row)
        pk = row[0]
        if pk in self._pk_index:
            raise Exception('Cannot insert duplicate row with Primary Key: {}'.format(pk))
        self._pk_index[pk] = len(self._pk_index)
        self._data.append(row)

    def row_list_to_insert(self, rows):
        new_rows = []
        for row in rows:
            new_row = {}
            for i, column_def in enumerate(self.column_defs):
                new_row[column_def.name] = row[i]
            new_rows.append(new_row)
        return new_rows

    def get_row_by_pk(self, pk):
        data_index = self._pk_index.search(pk)
        if data_index is None:
            return None
        return self.get_row_data(data_index)

    def get_row_data(self, index):
        logger.debug('Get Row Data - {}: row {}'.format(self.name, index))
        return self._data[index]

    def scan(self, start=None, stop=None):
        sp = slice(start, stop)
        for data_index in self._pk_index[sp]:
            yield self.get_row_data(data_index)

    @property
    def primary_key_def(self):
        return self.column_defs[0]

    @property
    def primary_key_ref(self):
        return ColumnReference(self.name, self.primary_key_def.name)


class Database:
    def __init__(self):
        self.tables = {}

    def execute(self, command):
        cmd_type = type(command)
        if cmd_type == str:
            command = parse(command)
            cmd_type = type(command)

        if cmd_type == Select:
            return self._select(command)
        elif cmd_type == Insert:
            return self._insert(command)
        elif cmd_type == CreateTable:
            table_name = command.table.name
            if table_name in self.tables:
                raise Exception('Cannot create existing table: {}'.format(command.name))
            self.tables[table_name] = Table(command)
        else:
            raise Exception('Unsupported type: {}'.format(cmd_type))

    def _get_table(self, table_name, raise_exception=True) -> Table:
        if type(table_name) == TableReference:
            table_name = table_name.name
        table = self.tables.get(table_name, None)
        if table is None and raise_exception:
            raise Exception('No table named {} found'.format(table_name))
        else:
            return table

    def _insert(self, insert: Insert):
        table = self._get_table(insert.table)
        table.direct_insert(insert.values)

    def _sort(self, rows, columns, order_by):
        if len(order_by.columns) > 0:
            indexes = [columns.index(c) for c in order_by.columns]

            def create_key(row):
                return tuple(row[i] for i in indexes)

            rows.sort(key=create_key, reverse=order_by.reverse)
        return rows

    def _trim_to_select(self, rows, columns, select):
        to_retain = [columns.index(c) for c in select.columns]
        results = []
        for row in rows:
            result = []
            for i, e in enumerate(row):
                if i in to_retain:
                    result.append(e)
            results.append(tuple(result))
        return results

    def _select(self, select: Select):
        from_clause = select.from_clause
        main_table = self._get_table(from_clause.table)
        columns = [ColumnReference(main_table.name, col.name) for col in main_table.column_defs]
        rows = []
        for row in self._get_rows(main_table, select.where):
            skip_row = False
            for joined_table in from_clause.joins:
                right_table = self._get_table(joined_table.table)
                right_table_columns = [ColumnReference(right_table.name, col.name) for col in right_table.column_defs]
                left_table_column_index = columns.index(joined_table.left)
                left_table_value = row[left_table_column_index]
                right_table_column_index = right_table_columns.index(joined_table.right)
                if right_table_column_index == 0:
                    logger.debug('Using primary key index for join on {}'.format(right_table.name))
                    # Primary key, so we can use the index
                    right_table_row = right_table.get_row_by_pk(left_table_value)
                    if right_table_row is None:
                        skip_row = True
                        break
                else:
                    # Need to loop all rows...
                    pass
                row += right_table_row
                columns += right_table_columns

            context = Context(row, columns)
            if not skip_row and select.where.evaluate(context):
                rows.append(row)

        rows = self._sort(rows, columns, select.order_by)
        return self._trim_to_select(rows, columns, select)

    def _get_rows(self, main_table, where_clause):
        if issubclass(type(where_clause), Terminal) and main_table.primary_key_ref in where_clause.columns_used():
            if type(where_clause) == Equals:
                logging.debug('Can use primary key index for where Equals')
                value = where_clause.right.value
                return [main_table.get_row_by_pk(value)]
            elif type(where_clause) == InFunc:
                logging.debug('Can use primary key index for where InFunc')
                return (main_table.get_row_by_pk(value.value) for value in where_clause.values)
            elif type(where_clause) in (GreaterThan, GreaterThanEquals) and isinstance(where_clause.right, Literal):
                logging.debug('Can use primary key index for where {}'.format(type(where_clause).__name__))
                value = where_clause.right.value
                # Note, in some cases, for GreaterThan, this will have an extra entry, but that will be filtered during where phase
                return main_table.scan(start=value)
            elif type(where_clause) in (LessThan, LessThanEquals) and isinstance(where_clause.right, Literal):
                logging.debug('Can use primary key index for where {}'.format(type(where_clause).__name__))
                value = where_clause.right.value
                toRet = iter(main_table.scan(stop=value))
                if type(where_clause) == LessThanEquals:
                    row = main_table.get_row_by_pk(value)
                    if row is not None:
                        second_it = itertools.repeat(row, 1)
                        return itertools.chain(second_it, toRet)
                return toRet
        return main_table.scan()


if __name__ == '__main__':
    db = Database()
    db.execute('create table keys(key int, value varchar)')
    db.execute('create table another(key int, value varchar)')
    db.execute('create table third(key int, value varchar)')
    for i in range(10):
        query = "insert into keys values({}, 'v{}')".format(i, str(i))
        db.execute(query)
        # query = "insert into another values({}, 'a{}')".format(i, str(i))
        # db.execute(query)

    db.execute("insert into another values(1, 'b1')")
    db.execute("insert into another values(2, 'a2')")
    db.execute("insert into third values(1, 't1')")
    db.execute("insert into third values(2, 't2')")

    query = """select another.key, another.value
from another

"""
    parsed = parse(query)
    print(parsed)
    results = db.execute(query)
    print(results)


    def consumer(op):
        if issubclass(type(op), Terminal):
            columns = op.columns_used()
            # print(columns)


    parsed.where.visit(consumer)
