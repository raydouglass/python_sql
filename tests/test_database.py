import unittest
from data_structures.database import Database

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


class TestDatabase(unittest.TestCase):
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

    def test_where_order(self):
        self.assert_select('SELECT main.id, main.cola, main.colb FROM main where main.id=1 order by main.id',
                           [MAIN_DATA[0]])
        self.assert_select(
            'SELECT main.id, main.cola, main.colb FROM main where main.id=1 or main.id=2 order by main.id desc',
            list(reversed(MAIN_DATA[0:2])))

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
