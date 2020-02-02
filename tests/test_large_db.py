import unittest
from python_sql.database import Database


class TestLargeDatabase(unittest.TestCase):
    def test_large(self):
        db = Database()
        db.execute('CREATE TABLE main(id int, cola int, colb str)')
        for i in range(10000):
            d = (i, i, str(i))
            db.execute("INSERT INTO main VALUES({}, {}, '{}')".format(*d))

        results = db.execute('SELECT main.id, main.cola, main.colb FROM main WHERE main.id = 5043')
        self.assertEqual(results, [(5043, 5043, '5043')])
