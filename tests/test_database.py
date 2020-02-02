import unittest

from python_sql.database import Database
from python_sql.logic import *

MAIN_DATA = [
    (1, 10, 'a1'),
    (2, 9, 'a2'),
    (3, 8, 'a3'),
]

OTHER_DATA = [
    (1, 'other1'),
    (2, 'other2')
]

THIRD_DATA = [
    (1, 'other1'),
    (2, 'other2')
]


class TestCreateTable(unittest.TestCase):
    def test_create(self):
        db = Database()
        self.assertEqual(0, len(db.tables))
        db.execute('CREATE TABLE main(id int, cola int, colb str)')
        self.assertEqual(1, len(db.tables))
        self.assertEqual(ColumnConstraint.PRIMARY_KEY, db.tables['main'].column_defs[0].constraints)
        self.assertEqual('rowid', db.tables['main'].column_defs[0].name)
        self.assertEqual(ColumnConstraint.NONE, db.tables['main'].column_defs[1].constraints)

    def test_primary_key(self):
        db = Database()
        db.execute('CREATE TABLE main(id int primary key, cola int, colb str)')
        self.assertEqual(1, len(db.tables))
        self.assertNotEqual(ColumnConstraint.NONE, db.tables['main'].column_defs[0].constraints)
        self.assertTrue(ColumnConstraint.PRIMARY_KEY in db.tables['main'].column_defs[0].constraints)

    def test_unique(self):
        db = Database()
        db.execute('CREATE TABLE main(id int unique, cola int, colb str)')
        self.assertEqual(1, len(db.tables))
        self.assertNotEqual(ColumnConstraint.NONE, db.tables['main'].column_defs[1].constraints)
        self.assertTrue(ColumnConstraint.UNIQUE in db.tables['main'].column_defs[1].constraints)

    def test_not_null(self):
        db = Database()
        db.execute('CREATE TABLE main(id int NOT NULL, cola int, colb str)')
        self.assertEqual(1, len(db.tables))
        self.assertNotEqual(ColumnConstraint.NONE, db.tables['main'].column_defs[1].constraints)
        self.assertTrue(ColumnConstraint.NOT_NULL in db.tables['main'].column_defs[1].constraints)

    def test_unique_not_null(self):
        db = Database()
        db.execute('CREATE TABLE main(id int unique NOT NULL, cola int, colb str)')
        self.assertEqual(1, len(db.tables))
        self.assertNotEqual(ColumnConstraint.NONE, db.tables['main'].column_defs[1].constraints)
        self.assertTrue(ColumnConstraint.NOT_NULL in db.tables['main'].column_defs[1].constraints)
        self.assertTrue(ColumnConstraint.UNIQUE in db.tables['main'].column_defs[1].constraints)


class TestSelect(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.db.execute('CREATE TABLE main(id int, cola int, colb str)')
        for d in MAIN_DATA:
            self.db.execute("INSERT INTO main VALUES({}, {}, '{}')".format(*d))

        self.db.execute('CREATE TABLE other(id int, data str)')
        for d in OTHER_DATA:
            self.db.execute("INSERT INTO other VALUES({}, '{}')".format(*d))

        self.db.execute('CREATE TABLE third(id int, data str)')
        for d in THIRD_DATA:
            self.db.execute("INSERT INTO third VALUES({}, '{}')".format(*d))

    def assert_select(self, query, expected):
        rows = self.db.execute(query)
        self.assertEqual(expected, rows)

    def test_basic(self):
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main', MAIN_DATA)

    def test_row_by_name(self):
        row = self.db.execute('SELECT main.id, main.cola, main.colb FROM main where main.id = 1')[0]
        self.assertEqual(MAIN_DATA[0][0], row['main.id'])

    def test_row_by_index(self):
        row = self.db.execute('SELECT main.id, main.cola, main.colb FROM main where main.id = 1')[0]
        self.assertEqual(MAIN_DATA[0][0], row[0])
        self.assertEqual(MAIN_DATA[0][2], row[2])

    def test_row_by_name_as_clause(self):
        row = self.db.execute('SELECT main.id AS id FROM main where main.id = 1')[0]
        self.assertEqual(MAIN_DATA[0][0], row['id'])

    def test_basic_limited_columns(self):
        self.assert_select('SELECT main.id, main.cola FROM main', [(t[0], t[1]) for t in MAIN_DATA])

    def test_basic_order(self):
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main order by main.id ', MAIN_DATA)
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main order by main.id desc',
                           list(reversed(MAIN_DATA)))

    def test_basic_where(self):
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main where main.id = 1', [MAIN_DATA[0]])
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main where main.id = 1 or main.id=2',
                           list(MAIN_DATA[0:2]))

    def test_where_multi_line(self):
        self.assert_select("""
        SELECT main.id, main.cola, main.colb
        FROM main
        WHERE main.id = 1
          OR main.id = 2
        """, list(MAIN_DATA[0:2]))

    def test_where_order(self):
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main where main.id=1 order by main.id',
                           [MAIN_DATA[0]])
        self.assert_select(
            'SELECT main.id, main.cola, main.colb FROM main where main.id=1 or main.id=2 order by main.id desc',
            list(reversed(MAIN_DATA[0:2])))

    def test_where_column_compare(self):
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main where main.id != main.cola', MAIN_DATA)
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main where main.id = main.cola', [])

    def test_join(self):
        expected = [
            MAIN_DATA[0] + OTHER_DATA[0],
            MAIN_DATA[1] + OTHER_DATA[1],
        ]
        query = """
        select main.id, main.cola, main.colb, other.id, other.data
        FROM main
          JOIN other ON main.id=other.id
        """
        self.assert_select(query, expected)

        query = """
        select main.id, main.cola, main.colb, other.id, other.data
        FROM main
          JOIN other ON other.id=main.id
        """
        self.assert_select(query, expected)

    def test_join_where(self):
        expected = [
            MAIN_DATA[0] + OTHER_DATA[0]
        ]
        query = """
                select main.id, main.cola, main.colb, other.id, other.data
                FROM main
                  JOIN other ON main.id=other.id
                WHERE main.id=1
                """
        self.assert_select(query, expected)

    def test_two_joins(self):
        expected = [
            MAIN_DATA[0] + OTHER_DATA[0] + THIRD_DATA[0],
            MAIN_DATA[1] + OTHER_DATA[1] + THIRD_DATA[1],
        ]
        query = """
                select main.id, main.cola, main.colb, other.id, other.data, third.id, third.data
                FROM main
                  JOIN other ON main.id=other.id
                  JOIN third ON main.id=third.id
                """
        self.assert_select(query, expected)

        query = """
                select main.id, main.cola, main.colb, other.id, other.data, third.id, third.data
                FROM main
                  JOIN other ON main.id=other.id
                  JOIN third ON other.id=third.id
                """
        self.assert_select(query, expected)

    def test_cross_join(self):
        expected = []
        for md in MAIN_DATA:
            for other in OTHER_DATA:
                expected.append(md + other)
        query = """
                  select main.id, main.cola, main.colb, other.id, other.data
                  FROM main
                  JOIN other"""
        self.assert_select(query, expected)

    def test_join_in_where(self):
        expected = [
            MAIN_DATA[0] + OTHER_DATA[0],
            MAIN_DATA[1] + OTHER_DATA[1],
        ]
        query = """
        select main.id, main.cola, main.colb, other.id, other.data
        FROM main
          JOIN other
        WHERE main.id=other.id
        """
        self.assert_select(query, expected)

    def test_join_in_where_filter(self):
        expected = [
            MAIN_DATA[0] + OTHER_DATA[0]
        ]
        query = """
        select main.id, main.cola, main.colb, other.id, other.data
        FROM main
          JOIN other
        WHERE main.id=other.id
          AND main.id=1
        """
        self.assert_select(query, expected)


class TestUpdate(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.db.execute('CREATE TABLE main(id int, cola int, colb str)')
        for d in MAIN_DATA:
            self.db.execute("INSERT INTO main VALUES({}, {}, '{}')".format(*d))

    def assert_select(self, query, expected):
        rows = self.db.execute(query)
        self.assertEqual(expected, rows)

    def test_basic_update(self):
        count = self.db.execute('UPDATE main SET main.cola=1')
        self.assertEqual(3, count)
        self.assert_select('SELECT main.cola FROM main', [(1,), (1,), (1,)])

    def test_update_with_where_pk(self):
        count = self.db.execute('UPDATE main SET main.cola=1 WHERE main.rowid=0')
        self.assertEqual(1, count)
        self.assert_select('SELECT main.cola FROM main', [(1,), (9,), (8,)])

    def test_update_with_where(self):
        count = self.db.execute('UPDATE main SET main.cola=1 WHERE main.id=1')
        self.assertEqual(1, count)
        self.assert_select('SELECT main.cola FROM main', [(1,), (9,), (8,)])
