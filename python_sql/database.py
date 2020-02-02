import itertools
import logging

from python_sql.b_tree import BTree
from python_sql.logic import *
from python_sql.parser import parse

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)


class Row():

    def __init__(self, data, columns):
        self.data = data if isinstance(data, tuple) else tuple(data)
        self.columns = [x.reference_name for x in columns]

    def __getitem__(self, key):
        if isinstance(key, int):
            index = key
        else:
            index = self.columns.index(key)
        return self.data[index]

    def __eq__(self, other):
        if isinstance(other, Row):
            return self.data == other.data and self.columns == other.columns
        elif isinstance(other, tuple):
            return self.data == other
        else:
            raise Exception('Can only compare Row or tuple')


class StorageDriver:

    def __init__(self):
        self.tables = {}

    def add_table(self, table):
        self.tables[table.name] = table.column_defs

    def write_row(self, table_name, pk, row_data):
        pass

    def append_row(self, table_name, row_data):
        pass

    def read_row(self, table_name, pk):
        pass

    def scan(self, table_name, start_pk=None, stop_pk=None):
        pass


class MemoryStorageDriver(StorageDriver):

    def __init__(self):
        super().__init__()
        self._data = {}

    def add_table(self, table):
        super().add_table(table)
        self._data[table.name] = []

    def write_row(self, table_name, pk, row_data):
        self._data[table_name][pk] = row_data

    def append_row(self, table_name, row_data):
        self._data[table_name].append(row_data)

    def read_row(self, table_name, pk):
        return self._data[table_name][pk]

    def scan(self, table_name, start_pk=None, stop_pk=None):
        sp = slice(start_pk, stop_pk)
        for row in self._data[table_name][sp]:
            yield row


class Table():
    def __init__(self, storage: StorageDriver, create_table: CreateTable):
        self.storage=storage
        self.name = create_table.table.name
        self.column_defs = create_table.columns
        self.pk_def = None
        self.auto_pk = False
        self._unique_indexes = {}
        for cd in self.column_defs:
            if cd.name == 'rowid' and ColumnConstraint.PRIMARY_KEY not in cd.constraints:
                raise Exception(
                    'Cannot have non-primary key column named rowid')
            if ColumnConstraint.PRIMARY_KEY in cd.constraints:
                if self.pk_def is not None:
                    raise Exception('Multiple Primary keys not supported')
                self.pk_def = cd
            if ColumnConstraint.UNIQUE in cd.constraints:
                self._unique_indexes[cd] = BTree()
        if self.pk_def is None:
            # Create fake PK
            self.pk_def = ColumnDefinition('rowid', 'int', 8,
                                           ColumnConstraint.PRIMARY_KEY)
            self.column_defs.insert(0, self.pk_def)
            self.auto_pk = True
        self._pk_index = BTree()
        self.storage.add_table(self)

    def insert(self, row):
        row_data = []
        for i, column_def in enumerate(self.column_defs):
            row_data.append(row.get(column_def.name, None))
        pk = row_data[0]
        if pk in self._pk_index:
            data_index = self._pk_index[pk]
            self.storage.write_row(self.name, data_index, row_data)
        else:
            self._pk_index[pk] = len(self._pk_index)
            self.storage.append_row(self.name, row_data)

    def direct_insert(self, row):
        if len(row) != len(self.column_defs) and not self.auto_pk:
            raise Exception(
                'Cannot directly insert row with missing or extra columns.')
        if self.auto_pk:
            new_pk = len(self._pk_index)
            row.insert(0, IntegerLiteral(new_pk))
        row = tuple(x.value for x in row)
        pk = row[0]
        if pk in self._pk_index:
            raise Exception(
                'Cannot insert duplicate row with Primary Key: {}'.format(pk))
        self._pk_index[pk] = len(self._pk_index)
        self.storage.append_row(self.name, row)

    def direct_update(self, row):
        if len(row) != len(self.column_defs):
            raise Exception(
                'Cannot directly update with missing or extra columns')

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
        return self.storage.read_row(self.name, index)

    def scan(self, start=None, stop=None):
        sp = slice(start, stop)
        for data_index in self._pk_index[sp]:
            yield self.get_row_data(data_index)

    @property
    def primary_key_def(self):
        return self.column_defs[0]

    @property
    def primary_key_ref(self):
        return ColumnReference(self.name, self.primary_key_def.name, None)

    @property
    def column_references(self):
        return [ColumnReference(self.name, col.name, None) for col in
                self.column_defs]


class Database:
    def __init__(self, storage: StorageDriver=MemoryStorageDriver()):
        self.tables = {}
        self.storage=storage

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
                raise Exception(
                    'Cannot create existing table: {}'.format(table_name))
            self.tables[table_name] = Table(self.storage, command)
        elif cmd_type == Update:
            return self._update(command)
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
            results.append(Row(result, select.columns))
        return results

    def _select_join_rows(self, joined_table: JoinTable, left_table_value):
        right_table = self._get_table(joined_table.table)
        right_table_columns = right_table.column_references
        right_table_column_index = right_table_columns.index(joined_table.right)
        right_table_column_def = right_table_columns[right_table_column_index]
        if right_table_column_def.column == right_table.pk_def.name:
            logger.debug('Using primary key index for join on {}'.format(
                right_table.name))
            # Primary key, so we can use the index
            right_table_row = right_table.get_row_by_pk(left_table_value)
            if right_table_row is None:
                yield None
            else:
                yield right_table_row
        else:
            for right_table_row in right_table.scan():
                if right_table_row[
                    right_table_column_index] == left_table_value:
                    yield right_table_row

    def _select(self, select: Select):
        from_clause = select.from_clause
        main_table = self._get_table(from_clause.table)
        columns = [ColumnReference(main_table.name, col.name, None) for col in
                   main_table.column_defs]
        rows = []
        for row in self._get_rows(main_table, select.where):
            skip_row = False
            temp_rows = [row]
            for joined_table in from_clause.joins:
                joined_rows = []
                right_table = self._get_table(joined_table.table)
                right_table_columns = right_table.column_references
                columns += right_table_columns
                left_table_column_index = columns.index(
                    joined_table.left) if joined_table.left is not None else None
                for curr_row in temp_rows:
                    if left_table_column_index is not None:
                        left_table_value = curr_row[left_table_column_index]
                        gen = self._select_join_rows(joined_table,
                                                     left_table_value)
                    else:
                        # cross join
                        gen = right_table.scan()
                    for right_table_row in gen:
                        if right_table_row is not None:
                            new_row = curr_row + right_table_row
                            joined_rows.append(new_row)
                temp_rows = joined_rows
            for r in temp_rows:
                rows.append(r)

        if select.where:
            rows = self._filter(rows, select.where, columns)
        if select.order_by:
            rows = self._sort(list(rows), columns, select.order_by)
        return self._trim_to_select(rows, columns, select)

    def _filter(self, rows: List, where, columns: List[ColumnReference]):
        for row in rows:
            if where is None or where.evaluate(Context(row, columns)):
                yield row

    def _get_rows(self, main_table: Table, where_clause):
        if issubclass(type(where_clause),
                      Terminal) and main_table.primary_key_ref in where_clause.columns_used():
            if type(where_clause) == Equals:
                logging.debug('Can use primary key index for where Equals')
                value = where_clause.right.value
                return [main_table.get_row_by_pk(value)]
            elif type(where_clause) == InFunc:
                logging.debug('Can use primary key index for where InFunc')
                return (main_table.get_row_by_pk(value.value) for value in
                        where_clause.values)
            elif type(where_clause) in (
                    GreaterThan, GreaterThanEquals) and isinstance(
                where_clause.right,
                Literal):
                logging.debug('Can use primary key index for where {}'.format(
                    type(where_clause).__name__))
                value = where_clause.right.value
                # Note, in some cases, for GreaterThan, this will have an extra entry, but that will be filtered during where phase
                return main_table.scan(start=value)
            elif type(where_clause) in (
                    LessThan, LessThanEquals) and isinstance(where_clause.right,
                                                             Literal):
                logging.debug('Can use primary key index for where {}'.format(
                    type(where_clause).__name__))
                value = where_clause.right.value
                toRet = iter(main_table.scan(stop=value))
                if type(where_clause) == LessThanEquals:
                    row = main_table.get_row_by_pk(value)
                    if row is not None:
                        second_it = itertools.repeat(row, 1)
                        return itertools.chain(second_it, toRet)
                return toRet
        return main_table.scan()

    def _update(self, update: Update):
        table = self._get_table(update.table)
        rows = self._get_rows(table, update.where)
        rows = self._filter(rows, update.where, table.column_references)
        columns = [ColumnReference(table.name, col.name, None) for col in
                   table.column_defs]
        count = 0
        for row in rows:
            row_data = {}
            for i, col_def in enumerate(table.column_defs):
                col_ref = ColumnReference(table.name, col_def.name, None)
                if col_ref in update.columns:
                    context = Context(row, columns)
                    new_value = context.evaluate(update.columns[col_ref])
                    row_data[col_def.name] = new_value
                else:
                    row_data[col_def.name] = row[i]
            table.insert(row_data)
            count += 1
        return count

        # indexes = {}
        # for target_column, value in update.columns.items():
        #     if target_column.table and target_column.table != table.name:
        #         raise Exception('Cannot update different tables. Update table={}, column={}'
        #                         .format(table.name, target_column.table))
        #     found = False
        #     for i, col_def in enumerate(table.column_defs):
        #         if col_def.name == target_column.column:
        #             indexes[i] = value
        #             found = True
        #             break
        #     if not found:
        #         raise Exception('No column named {} found in {}.'.format(target_column.column, table.name))
        # count = 0
        #
        # for row in rows:
        #     row = list(row)
        #     for i, value in indexes.items():
        #         context = Context(row, columns)
        #         new_value = context.evaluate(value)
        #         row[i] = new_value
        #     table.direct_insert(tuple(row))
        #     count += 1
        # return count


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
